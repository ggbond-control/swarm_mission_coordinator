from pathlib import Path

import yaml
from PIL import Image
from swarm_consensus.simulation import Cluster
from swarm_mission_coordinator.coordinator import Coordinator
from swarm_mission_planner.planner import plan
from swarm_protocol.contracts import normalize_mission


def make_map(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    Image.new("L", (80, 80), 255).save(root / "map_000.pgm")
    (root / "map_000.yaml").write_text(
        yaml.safe_dump(
            {
                "image": "map_000.pgm",
                "resolution": 0.5,
                "origin": [0, 0, 0],
                "negate": 0,
                "free_thresh": 0.196,
                "occupied_thresh": 0.65,
            }
        )
    )
    return str(root)


def mission(root, robots=None, tasks=3, epoch=1):
    return normalize_mission(
        {
            "round_epoch": epoch,
            "robot_ids": robots or ["R0", "R1", "R2"],
            "map_package": make_map(root),
            "tasks": [
                {
                    "id": f"p{i}",
                    "location": {
                        "frame_id": "map_000",
                        "pos": [3 + i * 6.0, 4 + i * 6.0, 0],
                    },
                    "tasks": [],
                }
                for i in range(tasks)
            ],
        },
        "demo",
    )


class MissionCluster:
    """Real Raft + real state machine + real planner, deterministic local IO."""

    def __init__(self, root, count=3, tasks=3):
        self.net = Cluster(count)
        self.mission = mission(root, self.net.ids, tasks)
        self.local = {r: [] for r in self.net.ids}
        self.acks = []
        self.cores = {}
        self.event_sequence = 0
        self.failed = set()
        for robot in self.net.ids:
            self.cores[robot] = Coordinator(
                robot,
                lambda value, r=robot: self.net.nodes[r].propose(value, self.net.now),
                lambda value, r=robot: self.calculate(r, value),
                lambda value, r=robot: self.local[r].append(value),
                lambda *value: self.acks.append(value),
                phase_timeout=4,
                planning_timeout=6,
                offline_timeout=2,
                stale_timeout=1,
            )
            self.cores[robot].ingress = (self.mission, {"bid": "demo", "tid": "t"}, "")
        self.fingerprint = plan(
            {**self.mission, "tasks": []}, self.robots(), radius=0.1
        )["map_digest"]
        self.executions = {}

    def robots(self):
        return {
            r: {
                "pose": {"frame_id": "map_000", "pos": [2 + i * 8.0, 2 + i * 8.0, 0]},
                "capabilities": ["device"],
                "speed": 1.0,
            }
            for i, r in enumerate(self.net.ids)
        }

    def calculate(self, robot, value):
        try:
            result = plan(value["mission"], value["robots"], radius=0.1)
            self.cores[robot].receive_plan(
                {"request_id": value["request_id"], "success": True, "plan": result}
            )
        except ValueError as error:
            self.cores[robot].receive_plan(
                {
                    "request_id": value["request_id"],
                    "success": False,
                    "error": str(error),
                }
            )

    def event(self, robot, kind, **fields):
        self.event_sequence += 1
        model = self.leader().model
        event = {
            "robot_id": robot,
            "session_id": robot + "-agent",
            "round_epoch": model.epoch,
            "revision": model.revision,
            "route_name": model.route_name(robot),
            "kind": kind,
            "event_id": f"ev-{self.event_sequence}",
            **fields,
        }
        self.leader().receive_event(event)
        return event

    def leader(self):
        return self.cores[self.net.leader().robot]

    def step(self, seconds=0.05, auto_prepare=True):
        self.net.step(seconds)
        robots = self.robots()
        for r, core in self.cores.items():
            if r in self.net.crashed:
                continue
            engine = self.net.nodes[r]
            state = {
                **engine.state(self.net.now),
                "entries": engine.committed(),
                "boot_id": engine.boot,
            }
            core.receive_consensus(state)
            for e in engine.committed():
                if e["command"]["type"] == "EXCLUDE":
                    engine.exclude(e["command"]["robot_id"])
            for p, info in robots.items():
                if p in self.net.crashed:
                    continue
                core.heartbeat(
                    {
                        "robot_id": p,
                        "session_id": p + "-agent",
                        "consensus_boot": p + "-boot",
                        "coordinator_boot": p + "-coordinator",
                        "sequence": int(self.net.now * 1000),
                        "ready": True,
                        "services_ready": True,
                        "map_digest": self.fingerprint,
                        **info,
                    },
                    self.net.now,
                )
            core.tick(self.net.now, 1000 + self.net.now)
        self.net.drain()
        try:
            model = self.leader().model
        except ValueError:
            return
        if auto_prepare:
            for robot in model.assignments:
                if robot not in model.prepared and robot not in self.net.crashed:
                    self.event(robot, "prepared", plan_digest=model.plan_digest)
            for robot in model.started:
                if robot not in model.start_acks and robot not in self.net.crashed:
                    self.event(robot, "started")
            if model.state in {"PAUSING", "STOPPING"}:
                for robot in (
                    set(model.started) - set(model.excluded) - set(model.stopped)
                ):
                    self.event(robot, "stopped", success=True)

    def run(self, seconds=4.0, **kwargs):
        for _ in range(int(seconds / 0.05)):
            self.step(**kwargs)

    def finish_routes(self, skipped=()):
        model = self.leader().model
        for r in model.started:
            if r in model.results and model.results[r]["finalized"]:
                continue
            tasks = model.assignments[r]["task_ids"]
            skips = {t: "NAVIGATION_FAILED" for t in tasks if t in skipped}
            self.event(
                r,
                "result",
                result={
                    "execution_id": f"exec-{model.revision}-{r}",
                    "sequence": 1,
                    "state": "COMPLETED",
                    "finalized": True,
                    "reached": [t for t in tasks if t not in skips],
                    "skipped": skips,
                    "pending": [],
                },
            )
