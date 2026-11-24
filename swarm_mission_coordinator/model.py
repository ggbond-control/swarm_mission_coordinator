"""Deterministic application state. Only committed commands may call apply()."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field

from swarm_protocol.contracts import TERMINAL, digest, normalize_task, public_mission


@dataclass
class MissionState:
    mission: dict = field(default_factory=dict)
    epoch: int = 0
    state: str = "EMPTY"
    revision: int = 0
    sessions: dict = field(default_factory=dict)
    excluded: list = field(default_factory=list)
    exclusion_pending: list = field(default_factory=list)
    tasks: dict = field(default_factory=dict)
    owners: dict = field(default_factory=dict)
    assignments: dict = field(default_factory=dict)
    results: dict = field(default_factory=dict)
    prepared: list = field(default_factory=list)
    started: list = field(default_factory=list)
    start_acks: list = field(default_factory=list)
    start_deadlines: dict = field(default_factory=dict)
    stopped: dict = field(default_factory=dict)
    unknown: dict = field(default_factory=dict)
    unassignable: dict = field(default_factory=dict)
    skip_history: list = field(default_factory=list)
    skip_retry_used: bool = False
    pending_additions: list = field(default_factory=list)
    deadline: float = 0.0
    reason: str = ""
    pause_reason: str = ""
    context: dict = field(default_factory=dict)
    map_digest: str = ""
    plan_digest: str = ""
    report_key: str = ""
    applied: dict = field(default_factory=dict)
    events: list = field(default_factory=list)
    last_index: int = 0
    terminal_index: int = 0

    @property
    def members(self):
        return sorted(set(self.sessions) - set(self.excluded))

    @property
    def unfinished(self):
        return [t for t, status in self.tasks.items() if status != "COMPLETED"]

    @property
    def executable(self):
        return {
            r
            for r, a in self.assignments.items()
            if any(p["type"] in {"task", "holding"} for p in a["checkpoints"])
        }

    @property
    def quiescent(self):
        return set(self.stopped) | {
            r
            for r, v in self.results.items()
            if v.get("finalized")
            and v.get("state") in {"COMPLETED", "FAILED", "CANCELLED"}
        }

    def apply(self, command, index):
        """Validate against a copy first: rejection never partly mutates state."""
        if command["type"] == "NOOP":
            self.last_index = index
            return False
        identity = command["id"]
        if identity in self.applied:
            if self.applied[identity] != digest(command):
                raise ValueError("command id reused with different content")
            self.last_index = index
            return False
        candidate = copy.deepcopy(self)
        candidate._apply(command, index)
        candidate.applied[identity] = digest(command)
        candidate.last_index = index
        self.__dict__.update(candidate.__dict__)
        return True

    def _identity(self, c, route=True):
        robot = c["robot_id"]
        if self.sessions.get(robot, {}).get("session_id") != c.get("session_id"):
            raise ValueError("old or unauthorized Agent boot")
        if route:
            if c.get("revision") != self.revision or robot not in self.assignments:
                raise ValueError("old revision or no assigned route")
            if c.get("route_name") != self.route_name(robot):
                raise ValueError("route name mismatch")
        return robot

    def route_name(self, robot):
        # Hash avoids leaking path separators or overflowing TaskHub identifiers.
        return f"sw{self.epoch}-{digest(self.mission.get('mission_id', ''))[:16]}-r{self.revision}-{robot}"

    def _apply(self, c, index):
        kind = c["type"]
        if kind == "MISSION":
            if self.state != "EMPTY":
                raise ValueError("mission already initialized")
            self.mission = public_mission(c["mission"])
            self.epoch = self.mission["round_epoch"]
            self.tasks = {t["id"]: "PENDING" for t in self.mission["tasks"]}
            self.context = copy.deepcopy(c.get("context", {}))
            self.report_key = c.get("report_key", "")
            self.state, self.revision, self.deadline = "PREPARING", 1, c["deadline"]
        else:
            if c.get("round_epoch") != self.epoch or self.state == "EMPTY":
                raise ValueError("wrong application round")
            if self.state in TERMINAL:
                if kind == "STOPPED":
                    self._identity(c)
                    if c.get("success") is True:
                        self.stopped[c["robot_id"]] = copy.deepcopy(c)
                    return
                raise ValueError("terminal mission is immutable")
            if kind == "ACTIVATE":
                if self.state != "PREPARING" or set(c["sessions"]) != set(
                    self.mission["robot_ids"]
                ):
                    raise ValueError("activation requires all initial members")
                if not c.get("map_digest"):
                    raise ValueError("map digest required")
                self.sessions = copy.deepcopy(c["sessions"])
                self.map_digest, self.state, self.deadline = (
                    c["map_digest"],
                    "PLANNING",
                    c["deadline"],
                )
            elif kind == "PLAN":
                if self.state != "PLANNING" or c["revision"] != self.revision:
                    raise ValueError("plan is not for current planning stage")
                plan = c["plan"]
                if plan["map_digest"] != self.map_digest:
                    raise ValueError("planner map changed")
                check = {k: v for k, v in plan.items() if k != "plan_digest"}
                if digest(check) != plan["plan_digest"]:
                    raise ValueError("plan digest mismatch")
                routes = {r["robot_id"]: copy.deepcopy(r) for r in plan["routes"]}
                if len(routes) != len(plan["routes"]) or set(routes) - set(
                    self.members
                ):
                    raise ValueError("invalid route membership")
                assigned = [t for r in routes.values() for t in r["task_ids"]]
                eligible = set(self.unfinished) - set(self.unknown)
                if len(set(assigned)) != len(assigned) or set(assigned) & set(
                    plan["unassignable"]
                ):
                    raise ValueError("duplicate task assignment")
                if set(assigned) | set(plan["unassignable"]) != eligible:
                    raise ValueError("plan must cover all safe unfinished tasks")
                for robot, route in routes.items():
                    if (
                        set(route["dependencies"]) - set(routes)
                        or robot in route["dependencies"]
                    ):
                        raise ValueError("invalid dependencies")
                    checkpoint_tasks = [
                        t for p in route["checkpoints"] for t in p["task_ids"]
                    ]
                    if set(checkpoint_tasks) != set(route["task_ids"]):
                        raise ValueError("checkpoint task mapping mismatch")
                remaining = set(routes)
                while remaining:
                    ready = {
                        r
                        for r in remaining
                        if not set(routes[r]["dependencies"]) & remaining
                    }
                    if not ready:
                        raise ValueError("cyclic route dependencies")
                    remaining -= ready
                self.assignments, self.unassignable = (
                    routes,
                    copy.deepcopy(plan["unassignable"]),
                )
                (
                    self.results,
                    self.prepared,
                    self.started,
                    self.start_acks,
                    self.stopped,
                ) = {}, [], [], [], {}
                self.start_deadlines = {}
                for t in assigned:
                    self.tasks[t] = "ASSIGNED"
                self.owners.update(
                    {t: r for r, a in routes.items() for t in a["task_ids"]}
                )
                self.plan_digest, self.state, self.deadline = (
                    plan["plan_digest"],
                    "PLANNED",
                    c["deadline"],
                )
            elif kind == "PREPARED":
                robot = self._identity(c)
                if self.state not in {"PLANNED", "ACTIVE"} or robot in self.excluded:
                    raise ValueError("not preparing a plan")
                if c.get("plan_digest") != self.plan_digest:
                    raise ValueError("prepared plan digest mismatch")
                if robot not in self.prepared:
                    self.prepared.append(robot)
            elif kind == "START":
                robot = c["robot_id"]
                if (
                    self.state not in {"PLANNED", "ACTIVE"}
                    or robot not in self.members
                    or not self.executable.issubset(self.prepared)
                    or robot not in self.executable
                ):
                    raise ValueError("start barrier not satisfied")
                if not self.dependencies_finished(robot):
                    raise ValueError("route dependencies not finalized")
                if robot not in self.started:
                    self.started.append(robot)
                    self.start_deadlines[robot] = c["deadline"]
                self.state = "ACTIVE"
            elif kind == "STARTED":
                robot = self._identity(c)
                if robot not in self.started:
                    raise ValueError("no committed start decision")
                if robot not in self.start_acks:
                    self.start_acks.append(robot)
            elif kind == "RESULT":
                robot = self._identity(c)
                if robot not in self.started:
                    raise ValueError("result before committed start")
                value = c["result"]
                self.validate_result(robot, value)
                self.results[robot] = copy.deepcopy(value)
                for task in value["reached"]:
                    self.tasks[task] = "COMPLETED"
                    self.unknown.pop(task, None)
                if value["state"] == "UNKNOWN":
                    for task in value["pending"]:
                        self.unknown[task] = {
                            "robot_id": robot,
                            "kind": "navigation_unknown",
                            "reason": value.get("reason", "累计导航结果未知"),
                        }
                for task, reason in value["skipped"].items():
                    if self.tasks[task] != "COMPLETED":
                        self.tasks[task] = "SKIPPED"
                        record = {
                            "task_id": task,
                            "robot_id": robot,
                            "revision": self.revision,
                            "reason": reason,
                        }
                        if record not in self.skip_history:
                            self.skip_history.append(record)
            elif kind == "EXCLUDE":
                robot = c["robot_id"]
                if robot not in self.sessions:
                    raise ValueError("unknown excluded member")
                if robot not in self.excluded:
                    self.excluded.append(robot)
                    self.exclusion_pending.append(robot)
                for task in self.assignments.get(robot, {}).get("task_ids", []):
                    if (
                        self.tasks[task] != "COMPLETED"
                        and robot in self.started
                        and robot not in self.quiescent
                    ):
                        self.unknown[task] = self.unknown.get(
                            task,
                            {
                                "robot_id": robot,
                                "kind": "stop_unconfirmed",
                                "reason": "失联设备停止／最终结果未确认",
                            },
                        )
            elif kind == "PAUSE":
                if self.state not in {"ACTIVE", "PLANNED", "PAUSING"}:
                    raise ValueError("cannot pause this stage")
                if c.get("fallback"):
                    if self.skip_retry_used:
                        raise ValueError("round-wide retry already consumed")
                    self.skip_retry_used = True
                self.state, self.pause_reason, self.deadline = (
                    "PAUSING",
                    c["reason"],
                    c["deadline"],
                )
            elif kind == "STOPPED":
                robot = self._identity(c, False)
                if c.get("revision") != self.revision or c.get(
                    "route_name"
                ) != self.route_name(robot):
                    raise ValueError("stop evidence route mismatch")
                if c.get("success") is not True:
                    raise ValueError("failed stop is not evidence")
                if c.get("navigation_result"):
                    self._apply(
                        {**c, "type": "RESULT", "result": c["navigation_result"]}, index
                    )
                self.stopped[robot] = copy.deepcopy(c)
                for task, info in list(self.unknown.items()):
                    if (
                        info["robot_id"] == robot
                        and info.get("kind") != "navigation_unknown"
                    ):
                        del self.unknown[task]
            elif kind == "ADD":
                if self.state not in {"ACTIVE", "PLANNED", "PAUSING"}:
                    raise ValueError("addition not allowed now")
                additions = [normalize_task(t) for t in c["tasks"]]
                ids = [t["id"] for t in additions]
                if len(ids) != len(set(ids)) or set(ids) & set(self.tasks):
                    raise ValueError("duplicate added task")
                self.mission["tasks"].extend(additions)
                self.tasks.update({t["id"]: "PENDING" for t in additions})
                self.pending_additions.extend(ids)
            elif kind == "REPLAN":
                if self.state != "PAUSING" or not (
                    set(self.started) - set(self.excluded)
                ).issubset(self.quiescent):
                    raise ValueError(
                        "replan requires surviving route stop confirmations"
                    )
                self.revision += 1
                self.state, self.deadline = "PLANNING", c["deadline"]
                (
                    self.assignments,
                    self.results,
                    self.prepared,
                    self.started,
                    self.start_acks,
                ) = {}, {}, [], [], []
                self.pending_additions = []
                self.exclusion_pending = []
            elif kind == "STOP":
                self.state, self.reason, self.deadline = (
                    "STOPPING",
                    c["reason"],
                    c["deadline"],
                )
            elif kind == "FINISH":
                if c["state"] not in TERMINAL:
                    raise ValueError("invalid terminal state")
                if c["state"] == "COMPLETED" and self.unfinished:
                    raise ValueError("cannot complete without arrival evidence")
                self.state, self.reason, self.terminal_index = (
                    c["state"],
                    c["reason"],
                    index,
                )
            else:
                raise ValueError("unknown command: " + kind)
        self.events.append(
            {
                "index": index,
                "kind": kind,
                "time": c.get("time", 0),
                "revision": self.revision,
                "state": self.state,
                "message": c.get("reason", ""),
                "fallback": bool(c.get("fallback")),
                **({"task_ids": [task["id"] for task in c["tasks"]]} if kind == "ADD" else {}),
                **(
                    {"routes": copy.deepcopy(self.assignments)}
                    if kind == "PLAN"
                    else {}
                ),
                **(
                    {
                        "skipped_tasks": [
                            t for t, s in self.tasks.items() if s == "SKIPPED"
                        ]
                    }
                    if c.get("fallback")
                    else {}
                ),
            }
        )

    def dependency_blockers(self, robot):
        for predecessor in self.assignments[robot]["dependencies"]:
            result = self.results.get(predecessor, {})
            if result.get("finalized"):
                final_task = self.assignments[predecessor]["task_ids"][-1:]
                if result["state"] != "COMPLETED" or not set(final_task).issubset(result["reached"]):
                    yield predecessor

    def dependencies_finished(self, robot):
        return not any(self.dependency_blockers(robot)) and all(
            self.results.get(r, {}).get("finalized")
            for r in self.assignments[robot]["dependencies"]
        )

    def validate_result(self, robot, value):
        reached, skipped, pending = (
            value.get("reached"),
            value.get("skipped"),
            value.get("pending"),
        )
        if (
            not isinstance(reached, list)
            or not isinstance(skipped, dict)
            or not isinstance(pending, list)
            or any(not isinstance(t, str) for t in reached + pending)
            or len(reached) != len(set(reached))
            or len(pending) != len(set(pending))
        ):
            raise ValueError("invalid result partitions")
        sets = [set(reached), set(skipped), set(pending)]
        if any(sets[i] & sets[j] for i in range(3) for j in range(i)) or set.union(
            *sets
        ) != set(self.assignments[robot]["task_ids"]):
            raise ValueError("result partitions do not cover assigned tasks")
        if (
            not value.get("execution_id")
            or type(value.get("sequence")) is not int
            or value["sequence"] <= 0
            or type(value.get("finalized")) is not bool
            or value.get("state")
            not in {"RUNNING", "PAUSED", "COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"}
        ):
            raise ValueError("invalid result identity/state")
        if value["finalized"] != (
            value["state"] in {"COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"}
        ):
            raise ValueError("finalization mismatch")
        if value["state"] == "COMPLETED" and pending:
            raise ValueError("completed route has pending points")
        old = self.results.get(robot)
        if old:
            if old == value:
                return
            if (
                old["finalized"]
                or old["execution_id"] != value["execution_id"]
                or old["sequence"] >= value["sequence"]
                or not set(old["reached"]).issubset(reached)
            ):
                raise ValueError("stale/conflicting navigation evidence")

    def summary(self):
        completed_ids = [task_id for task_id, status in self.tasks.items()
                         if status == "COMPLETED"]
        return {
            "mission_id": self.mission.get("mission_id", ""),
            "map_package": self.mission.get("map_package", ""),
            "round_epoch": self.epoch,
            "state": self.state,
            "plan_revision": self.revision,
            "message": self.reason,
            "total_tasks": len(self.tasks),
            "completed_tasks": len(completed_ids),
            "completed_task_ids": completed_ids,
            "unfinished_tasks": len(self.unfinished),
            "retry_performed": self.skip_retry_used,
            "retry_limit": 1,
            "unconfirmed_tasks": copy.deepcopy(self.unknown),
            "unassignable_tasks": copy.deepcopy(self.unassignable),
            "skipped_tasks": [t for t, s in self.tasks.items() if s == "SKIPPED"],
            "commit_index": self.last_index,
            **({"report_key": self.report_key} if self.report_key else {}),
        }
