"""Leader workflow driven by replicated facts, with injected IO and clocks."""

from __future__ import annotations

import copy
import uuid

from swarm_protocol.contracts import TERMINAL, public_mission

from .model import MissionState


class Coordinator:
    def __init__(
        self,
        robot,
        propose,
        request_plan,
        local_decision,
        ack,
        *,
        phase_timeout=30.0,
        planning_timeout=60.0,
        offline_timeout=10.0,
        stale_timeout=3.0,
    ):
        self.robot = robot
        self.propose_io, self.plan_io, self.local_io, self.ack_io = (
            propose,
            request_plan,
            local_decision,
            ack,
        )
        self.phase_timeout, self.planning_timeout = phase_timeout, planning_timeout
        self.offline_timeout, self.stale_timeout = offline_timeout, stale_timeout
        self.model = MissionState()
        self.consensus = {}
        self.peers = {}
        self.ingress = None
        self.pending = None
        self.events = {}
        self.controls = []
        self.additions = []
        self.plan_request, self.plan_response = None, None
        self.local_at = -float("inf")
        self.error = ""

    def receive_consensus(self, value):
        previous = self.consensus
        if previous and value["round_epoch"] > previous.get("round_epoch", 0):
            if self.model.state not in TERMINAL | {"EMPTY"}:
                raise ValueError("cannot replace active application round")
            self.model = MissionState()
            self.pending = None
            self.events.clear()
            self.additions.clear()
            self.controls.clear()
            self.plan_request, self.plan_response = None, None
        self.consensus = copy.deepcopy(value)
        if (
            previous.get("term") != value["term"]
            or previous.get("leader_id") != value["leader_id"]
        ):
            self.pending = None
            self.plan_request, self.plan_response = None, None
        for entry in value["entries"]:
            if entry["index"] <= self.model.last_index:
                continue
            command = entry["command"]
            self.model.apply(command, entry["index"])
            if self.pending and self.pending["id"] == command["id"]:
                self.pending = None
            if command.get("event_id"):
                self.ack_io(
                    command["robot_id"], command["session_id"], command["event_id"]
                )
                self.events.pop(command["event_id"], None)

    def heartbeat(self, value, monotonic):
        robot = value.get("robot_id")
        if not robot or not isinstance(value.get("sequence"), int):
            return
        old = self.peers.get(robot)
        if (
            old
            and old.get("coordinator_boot") == value.get("coordinator_boot")
            and old["sequence"] >= value["sequence"]
        ):
            return
        self.peers[robot] = {**copy.deepcopy(value), "received": monotonic}

    def authority(self):
        c = self.consensus
        return bool(
            c.get("leader_id") == self.robot and c.get("authority") and not self.error
        )

    def receive_event(self, value):
        identity = value.get("event_id")
        if (
            not isinstance(identity, str)
            or len(identity) > 256
            or value.get("robot_id") not in self.model.sessions
        ):
            return
        if identity not in self.events and len(self.events) >= 1024:
            return
        self.events[identity] = copy.deepcopy(value)

    def receive_plan(self, value):
        if self.plan_request and value.get("request_id") == self.plan_request:
            self.plan_response = copy.deepcopy(value)

    def _command(self, kind, wall, **fields):
        value = {
            "type": kind,
            "id": uuid.uuid4().hex,
            "round_epoch": self.consensus["round_epoch"],
            "time": wall,
            **fields,
        }
        # Fail before proposing, and never partially mutate authoritative state.
        candidate = copy.deepcopy(self.model)
        candidate.apply(value, self.model.last_index + 1)
        self.pending = value
        self.propose_io(value)

    def _ready(self, robot, now):
        peer = self.peers.get(robot, {})
        if (
            now - peer.get("received", -float("inf")) > self.stale_timeout
            or not peer.get("ready")
            or not peer.get("pose")
            or not peer.get("services_ready")
            or not peer.get("map_digest")
        ):
            return False
        expected = self.model.sessions.get(robot)
        if expected and any(
            peer.get(k) != expected.get(k) for k in ("session_id", "consensus_boot")
        ):
            return False
        return True

    def _finish(self, state, reason, wall):
        self._command("FINISH", wall, state=state, reason=reason)

    def _local(self, now):
        m = self.model
        if not m.mission or now - self.local_at < 0.5:
            return
        self.local_at = now
        if self.robot in m.assignments and m.sessions.get(self.robot, {}).get(
            "session_id"
        ) == self.peers.get(self.robot, {}).get("session_id"):
            fields = {
                "robot_id": self.robot,
                "round_epoch": m.epoch,
                "revision": m.revision,
                "route_name": m.route_name(self.robot),
                "session_id": m.sessions[self.robot]["session_id"],
            }
            if m.state in {"PLANNED", "ACTIVE"} and self.robot not in m.excluded:
                if self.robot not in m.prepared:
                    self.local_io(
                        {
                            "command": "PREPARE",
                            "mission": m.mission,
                            "route": m.assignments[self.robot],
                            "plan_digest": m.plan_digest,
                            **fields,
                        }
                    )
                if (
                    self.robot in m.started
                    and self.robot not in m.stopped
                    and self.robot not in m.start_acks
                ):
                    self.local_io({"command": "START", **fields})
            if (
                m.state in {"PAUSING", "STOPPING"}
                or m.state in TERMINAL
                or self.robot in m.excluded
            ):
                finished = m.results.get(self.robot, {}).get("finalized", False)
                if (
                    self.robot in m.started
                    and self.robot not in m.stopped
                    and not finished
                ):
                    self.local_io(
                        {
                            "command": "CANCEL"
                            if m.state in TERMINAL or m.state == "STOPPING"
                            else "PAUSE",
                            **fields,
                        }
                    )

    def tick(self, now, wall):
        self._local(now)
        if (
            not self.authority()
            or self.pending
            or self.consensus.get("last_index", 0)
            > self.consensus.get("commit_index", 0)
        ):
            return
        m = self.model
        if m.state == "EMPTY":
            if self.ingress:
                mission, context, key = self.ingress
                self._command(
                    "MISSION",
                    wall,
                    mission=public_mission(mission),
                    context=context,
                    report_key=key,
                    deadline=wall + self.phase_timeout,
                )
            return
        # Durable event IDs make ACK loss and leader takeover harmless.
        for identity, value in list(self.events.items()):
            if identity in m.applied:
                self.ack_io(value["robot_id"], value["session_id"], identity)
                del self.events[identity]
                continue
            kind = {
                "prepared": "PREPARED",
                "started": "STARTED",
                "result": "RESULT",
                "stopped": "STOPPED",
            }.get(value["kind"])
            if value["kind"] == "error" and m.state not in TERMINAL | {"STOPPING"}:
                try:
                    if value.get("round_epoch") != m.epoch:
                        raise ValueError("old round")
                    m._identity(value)
                except ValueError:
                    del self.events[identity]
                    continue
                self._finish(
                    "FAILED",
                    f"{value['robot_id']} 路线执行异常：{value.get('message', '')}",
                    wall,
                )
                del self.events[identity]
                return
            if kind:
                command = {
                    "type": kind,
                    "id": identity,
                    "time": wall,
                    **{
                        k: v
                        for k, v in value.items()
                        if k not in {"kind", "id", "time"}
                    },
                }
                candidate = copy.deepcopy(m)
                try:
                    candidate.apply(command, m.last_index + 1)
                except ValueError:
                    del self.events[identity]
                    continue
                self.pending = command
                self.propose_io(command)
                return
            del self.events[identity]
        if m.state in TERMINAL:
            return
        if self.controls:
            control = self.controls.pop(0)
            self.controls.clear()
            self._command(
                "STOP",
                wall,
                **({"id": control["request_id"]} if isinstance(control, dict) else {}),
                reason="平台请求停止／重置本轮任务",
                deadline=wall + self.phase_timeout,
            )
            return
        safety_stops = [
            r
            for r, e in m.stopped.items()
            if e.get("cause")
            in {"permit_expired", "task_hub_timeout", "local_stop", "local_error"}
        ]
        if safety_stops and m.state != "STOPPING":
            self._finish(
                "FAILED",
                "执行授权过期或本机安全停止：" + ",".join(sorted(safety_stops)),
                wall,
            )
            return
        if self.additions and m.state in {"ACTIVE", "PLANNED", "PAUSING"}:
            item = self.additions.pop(0)
            tasks = item["tasks"] if isinstance(item, dict) else item
            identity = item.get("request_id") if isinstance(item, dict) else None
            if identity in m.applied:
                return
            self._command(
                "ADD", wall, tasks=tasks, **({"id": identity} if identity else {})
            )
            return
        # A boot change is a failure even if fresh heartbeats still arrive.
        if m.state in {"PLANNED", "ACTIVE", "PAUSING", "PLANNING"}:
            for robot in m.members:
                peer = self.peers.get(robot, {})
                session = m.sessions[robot]
                reboot = peer and any(
                    peer.get(k) != session[k] for k in ("session_id", "consensus_boot")
                )
                if robot != self.robot and (
                    reboot
                    or now - peer.get("received", -float("inf")) > self.offline_timeout
                ):
                    self._command(
                        "EXCLUDE",
                        wall,
                        robot_id=robot,
                        reason="启动会话变化" if reboot else "心跳OFFLINE",
                    )
                    return
        if m.state == "PREPARING":
            ids = m.mission["robot_ids"]
            if all(self._ready(r, now) for r in ids):
                hashes = {self.peers[r]["map_digest"] for r in ids}
                if len(hashes) != 1:
                    self._finish("FAILED", "初始成员地图内容或规划净空参数不一致", wall)
                    return
                sessions = {
                    r: {k: self.peers[r][k] for k in ("session_id", "consensus_boot")}
                    for r in ids
                }
                self._command(
                    "ACTIVATE",
                    wall,
                    sessions=sessions,
                    map_digest=next(iter(hashes)),
                    deadline=wall + self.planning_timeout,
                )
            elif wall >= m.deadline:
                missing = [r for r in ids if not self._ready(r, now)]
                self._finish("FAILED", "初始成员准备超时：" + ",".join(missing), wall)
        elif m.state == "PLANNING":
            if wall >= m.deadline:
                self._finish("FAILED", "规划阶段原期限已到（Leader切换不续期）", wall)
                return
            if self.plan_response:
                result, self.plan_response = self.plan_response, None
                if not result["success"]:
                    self._finish("FAILED", "规划失败：" + result["error"], wall)
                elif m.revision == 1 and result["plan"]["unassignable"]:
                    self._finish(
                        "FAILED",
                        "首次任务存在能力不足点："
                        + ",".join(result["plan"]["unassignable"]),
                        wall,
                    )
                else:
                    self._command(
                        "PLAN",
                        wall,
                        revision=m.revision,
                        plan=result["plan"],
                        deadline=wall + self.phase_timeout,
                    )
                return
            if not self.plan_request and all(self._ready(r, now) for r in m.members):
                self.plan_request = f"{m.epoch}-r{m.revision}-t{self.consensus['term']}-{uuid.uuid4().hex}"
                mission = copy.deepcopy(m.mission)
                mission["tasks"] = [
                    t
                    for t in mission["tasks"]
                    if t["id"] in m.unfinished and t["id"] not in m.unknown
                ]
                robots = {
                    r: {k: self.peers[r][k] for k in ("pose", "capabilities", "speed")}
                    for r in m.members
                }
                self.plan_io(
                    {
                        "request_id": self.plan_request,
                        "mission": mission,
                        "robots": robots,
                    }
                )
        elif m.state in {"PLANNED", "ACTIVE"}:
            if m.pending_additions or m.exclusion_pending:
                self._command(
                    "PAUSE",
                    wall,
                    reason="新增任务或设备掉线触发重分配",
                    deadline=wall + self.phase_timeout,
                )
                self.plan_request = None
                return
            unexpected = [
                r
                for r in m.started
                if r in m.stopped and not m.results.get(r, {}).get("finalized")
            ]
            if unexpected:
                self._finish(
                    "FAILED",
                    "执行授权过期或路线非预期停止：" + ",".join(sorted(unexpected)),
                    wall,
                )
                return
            for r in m.started:
                if r not in m.start_acks and wall >= m.start_deadlines.get(r, wall + 1):
                    self._finish(
                        "FAILED", f"{r} 启动响应超时；安全取消可能已启动路线", wall
                    )
                    return
            if not m.executable.issubset(m.prepared):
                if wall >= m.deadline:
                    self._finish("FAILED", "路线生成或短暂STALE启动就绪等待超时", wall)
                return
            for r in sorted(m.executable - set(m.started) - set(m.excluded)):
                if m.dependencies_finished(r) and all(
                    self._ready(p, now) for p in m.executable - set(m.excluded)
                ):
                    self._command(
                        "START", wall, robot_id=r, deadline=wall + self.phase_timeout
                    )
                    return
            expected = m.executable - set(m.excluded)
            abnormal = [
                r
                for r in expected
                if m.results.get(r, {}).get("finalized")
                and m.results[r]["state"] != "COMPLETED"
            ]
            if abnormal:
                self._finish("FAILED", "导航结果异常：" + ",".join(abnormal), wall)
                return
            if not all(m.results.get(r, {}).get("finalized") for r in expected):
                if m.state == "PLANNED" and wall >= m.deadline:
                    self._finish("FAILED", "启动就绪等待超时", wall)
                return
            if not m.unfinished:
                self._finish(
                    "COMPLETED", "所有任务点导航成功到达；不代表挂载操作全部成功", wall
                )
            elif (
                any(
                    m.tasks[t] == "SKIPPED"
                    and t not in m.unknown
                    and t not in m.unassignable
                    for t in m.unfinished
                )
                and not m.skip_retry_used
            ):
                self._command(
                    "PAUSE",
                    wall,
                    reason="整轮唯一一次最终跳点补执行",
                    fallback=True,
                    deadline=wall + self.phase_timeout,
                )
                self.plan_request = None
            else:
                self._finish(
                    "FAILED",
                    f"本轮任务部分完成：已到达{len(m.tasks) - len(m.unfinished)}/{len(m.tasks)}点；仍有跳点、能力不足或结果未知",
                    wall,
                )
        elif m.state == "PAUSING":
            expected = set(m.started) - set(m.excluded)
            if expected.issubset(m.quiescent):
                self._command("REPLAN", wall, deadline=wall + self.planning_timeout)
                self.plan_request = None
            elif wall >= m.deadline:
                self._finish("FAILED", "安全暂停确认超时，禁止提交替代路线", wall)
        elif m.state == "STOPPING":
            expected = set(m.started) - set(m.excluded)
            if expected.issubset(m.quiescent) or wall >= m.deadline:
                self._finish("CANCELLED", "本轮取消；停止未确认的设备仍保持隔离", wall)
