"""Device-local coordinator: replicated decisions, standard ROS String IO."""

from __future__ import annotations

import copy
import json
import queue
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile
from std_msgs.msg import String
from std_srvs.srv import Trigger
from swarm_mission_planner.maps import MapBundle
from swarm_protocol.contracts import (
    TERMINAL,
    decode,
    digest,
    normalize_mission,
    public_mission,
)
from swarm_protocol.ros import main_node, publish, runtime_path
from swarm_protocol.storage import ProcessLock, atomic_json
from swarm_protocol.topics import DEFAULT_CLUSTER_ID

from inspection_interfaces.msg import PlatformEvent

from .api import MissionActions
from .coordinator import Coordinator
from .media import MediaStore
from .model import MissionState
from .platform import snapshot_events
from .reporting import ReportWorker


class CoordinatorNode(Node):
    def __init__(self):
        super().__init__("swarm_mission_coordinator")
        for key, value in [
            ("robot_id", ""),
            ("cluster_id", DEFAULT_CLUSTER_ID),
            ("phase_timeout", 30.0),
            ("planning_timeout", 60.0),
            ("offline_timeout", 10.0),
            ("stale_timeout", 3.0),
            ("footprint_radius", 0.3),
            ("free_thresh", 0.05),
        ]:
            self.declare_parameter(key, value)
        self.declare_parameter("media_roots", [""])
        self.declare_parameter("agent_data_root", "")
        self.robot, self.cluster = (
            self.get_parameter("robot_id").value,
            self.get_parameter("cluster_id").value,
        )
        self.root = runtime_path(self, "coordinator")
        self.lock = ProcessLock(self.root / "process.lock")
        self.boot, self.sequence = uuid.uuid4().hex, 0
        self.outputs = {
            name: self.create_publisher(String, topic, 100)
            for name, topic in {
                "configure": "/swarm/consensus/configure",
                "proposal": "/swarm/consensus/propose",
                "raft_in": "/swarm/consensus/inbound",
                "read": "/swarm/consensus/read",
                "wire": "/swarm/network/outbound",
                "platform": "/swarm/platform/outbound",
                "planner": "/swarm/planner/request",
                "decision": "/swarm/agent/decision",
                "permit": "/swarm/agent/permit",
                "ack": "/swarm/agent/ack",
            }.items()
        }
        self.status_pub = self.create_publisher(
            String,
            "/swarm/mission/state",
            QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL),
        )
        self.core = Coordinator(
            self.robot,
            lambda c: self.pub("proposal", c),
            lambda c: self.pub("planner", c),
            lambda c: self.pub("decision", c),
            self.ack,
            phase_timeout=self.get_parameter("phase_timeout").value,
            planning_timeout=self.get_parameter("planning_timeout").value,
            offline_timeout=self.get_parameter("offline_timeout").value,
            stale_timeout=self.get_parameter("stale_timeout").value,
        )
        self.agent_state = {}
        self.pending_request, self.credentials = None, {}
        self.fingerprint, self.map_error, self.map_working = "", "", False
        self.read_requests = {}
        self.grants = {}
        self.media = MediaStore(
            self.root / "media", self.get_parameter("media_roots").value or []
        )
        self.history = {}
        for path in sorted((self.root / "history").glob("*.json")):
            try:
                historical = MissionState(**json.loads(path.read_text()))
                self.history[historical.epoch] = historical
            except (OSError, ValueError, TypeError):
                self.get_logger().warning("历史报告状态不可读，保留文件：" + str(path))
        self.media_manifests = {}
        self.media_serving = []
        self.last_media, self.last_manifest = 0.0, 0.0
        self.agent_root = self.get_parameter("agent_data_root").value
        self.create_subscription(
            PlatformEvent, "/platform/action_events", self.photo_event, 100
        )
        self.report_worker = ReportWorker(self.root / "reports", self.get_logger())
        self.pool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="swarm-preflight"
        )
        self.completed = queue.SimpleQueue()
        self.last_hello, self.last_platform, self.last_event_log = 0.0, 0.0, 0
        self.report_error = ""
        self.request_seen = set()
        self.relay = {}
        self.last_relay = 0.0
        self.commands_path = self.root / "private_commands.json"
        self.command_outbox = (
            json.loads(self.commands_path.read_text())
            if self.commands_path.exists()
            else {}
        )
        pending_path = self.root / "private_request.json"
        if pending_path.exists():
            self.pending_request = json.loads(pending_path.read_text())
            self.restore_ingress(self.pending_request)
        for topic, callback in [
            ("/swarm/platform/inbound", self.platform_request),
            ("/swarm/network/inbound", self.network),
            ("/swarm/consensus/outbound", self.raft_out),
            ("/swarm/consensus/state", self.consensus),
            ("/swarm/consensus/read_confirmed", self.read_confirmed),
            ("/swarm/agent/state", self.agent_hello),
            ("/swarm/agent/events", self.agent_event),
            ("/swarm/agent/permit_request", self.permit_request),
            ("/swarm/planner/result", self.core.receive_plan),
        ]:
            self.create_subscription(
                String, topic, lambda msg, cb=callback: self.safe(cb, msg), 100
            )
        self.create_service(Trigger, "/swarm/mission/stop", self.local_stop)
        self.create_service(Trigger, "/swarm/mission/reset", self.local_stop)
        self.actions = MissionActions(self)
        self.create_timer(0.1, self.tick)

    def pub(self, name, value):
        publish(self.outputs[name], value)

    def safe(self, callback, message):
        try:
            callback(decode(message.data))
        except Exception as error:
            self.get_logger().warning(f"协同消息拒绝：{type(error).__name__}: {error}")

    def wire(self, destination, kind, data):
        if destination == self.robot:
            self.handle_peer(self.robot, kind, data)
        else:
            epoch = self.core.consensus.get("round_epoch", 0)
            self.pub(
                "wire",
                {"to": destination, "kind": kind, "data": data, "round_epoch": epoch},
            )

    def restore_ingress(self, envelope):
        mission = normalize_mission(envelope["data"], envelope["bid"])
        if self.robot not in mission["robot_ids"]:
            return
        if "storage" in mission:
            self.credentials[mission["round_epoch"]] = mission["storage"]
        key = (
            mission["storage"]["objectKeyPrefix"]
            + "/"
            + mission["mission_id"]
            + ".html"
            if "storage" in mission
            else ""
        )
        self.core.ingress = (
            public_mission(mission),
            {"bid": envelope["bid"], "tid": envelope.get("tid", "")},
            key,
        )

    def platform_request(self, envelope):
        try:
            self.accept_request(envelope)
        except (ValueError, KeyError, TypeError) as error:
            self.emit_platform(
                "services_reply",
                {
                    "bid": envelope.get("bid", ""),
                    "tid": envelope.get("tid", ""),
                    "method": envelope.get("method", ""),
                    "timestamp": int(time.time() * 1000),
                    "_result": 1,
                    "_error": str(error),
                    "data": {"message": str(error), "robot_id": self.robot},
                },
            )
            raise

    def request_control(self, method):
        pending = self.pending_request
        if pending and pending["data"]["round_epoch"] > self.core.model.epoch:
            bid, epoch = pending["bid"], pending["data"]["round_epoch"]
        else:
            bid, epoch = (
                self.core.model.mission.get("mission_id", ""),
                self.core.model.epoch,
            )
        envelope = {
            "method": method,
            "bid": bid,
            "tid": uuid.uuid4().hex,
            "data": {"round_epoch": epoch},
        }
        self.accept_request(envelope)
        self.pub("decision", {"robot_id": self.robot, "command": "CANCEL"})

    def accept_request(self, envelope, relay=False):
        method = envelope.get("method")
        if method in {"task.stop", "task.reset"}:
            pending = self.pending_request
            if pending and pending["data"]["round_epoch"] > self.core.model.epoch:
                bid, epoch = pending["bid"], pending["data"]["round_epoch"]
            elif self.core.model.mission:
                bid, epoch = (
                    self.core.model.mission["mission_id"],
                    self.core.model.epoch,
                )
            else:
                return
            if envelope.get("bid") not in {None, "", bid}:
                raise ValueError("stop/reset mission_id mismatch")
            if envelope.get("data", {}).get("round_epoch", epoch) != epoch:
                raise ValueError("stop/reset belongs to another round")
            envelope = copy.deepcopy(envelope)
            envelope.setdefault("data", {})["round_epoch"] = epoch
            self.enqueue_request(envelope)
            self.pub("decision", {"robot_id": self.robot, "command": "CANCEL"})
            return
        if method == "task.add":
            data = envelope["data"]
            m = self.core.model
            if envelope.get("bid") != m.mission.get("mission_id"):
                raise ValueError("addition mission_id mismatch")
            if (
                data.get("round_epoch") != m.epoch
                or set(data.get("robot_ids", [])) != set(m.mission.get("robot_ids", []))
                or data.get("map_package") != m.mission.get("map_package")
            ):
                raise ValueError("addition changes round, roster or map")
            from swarm_protocol.contracts import normalize_task

            tasks = [normalize_task(t) for t in data["tasks"]]
            if m.state not in {"ACTIVE", "PLANNED", "PAUSING"}:
                raise ValueError("addition not allowed in current stage")
            identity = digest(
                {
                    "epoch": m.epoch,
                    "request": {**envelope, "data": {**data, "tasks": tasks}},
                }
            )
            if identity in m.applied:
                return
            if any(t["id"] in m.tasks for t in tasks) or len(
                {t["id"] for t in tasks}
            ) != len(tasks):
                raise ValueError("duplicate added task ids")
            self.enqueue_request({**envelope, "data": {**data, "tasks": tasks}})
            return
        if method != "task.execute":
            raise ValueError("unsupported platform method")
        mission = normalize_mission(envelope.get("data"), envelope.get("bid"))
        if self.robot not in mission["robot_ids"]:
            return
        if relay:
            self.relay[digest(envelope)] = (
                copy.deepcopy(envelope),
                time.monotonic() + 120.0,
            )
        current_epoch = self.core.consensus.get("round_epoch", 0)
        current = self.core.model
        if mission["round_epoch"] < current_epoch:
            raise ValueError("old platform round")
        if (
            self.pending_request
            and mission["round_epoch"] == self.pending_request["data"]["round_epoch"]
        ):
            previous = normalize_mission(
                self.pending_request["data"], self.pending_request["bid"]
            )
            if digest(public_mission(previous)) != digest(public_mission(mission)):
                raise ValueError("conflicting payload at same round_epoch")
            return
        if current.state not in TERMINAL | {"EMPTY"}:
            raise ValueError("active round cannot be replaced")
        if self.agent_state.get("start_phase") in {"starting", "started"}:
            raise ValueError("local previous execution not stopped")
        self.archive_terminal()
        self.pending_request = copy.deepcopy(envelope)
        atomic_json(self.root / "private_request.json", envelope)
        self.restore_ingress(envelope)
        self.fingerprint, self.map_error = "", ""
        self.pub(
            "configure",
            {"round_epoch": mission["round_epoch"], "robot_ids": mission["robot_ids"]},
        )

    def raft_out(self, value):
        self.wire(value["to"], "raft", value["data"])

    def network(self, value):
        if value.get("cluster_id") != self.cluster:
            raise ValueError("wrong cluster")
        self.handle_peer(value["robot_id"], value["kind"], value["data"])

    def handle_peer(self, sender, kind, value):
        if kind == "round_input":
            roster = value.get("data", {}).get("robot_ids", [])
            if sender not in roster or self.robot not in roster:
                raise ValueError("round input source is not in roster")
            self.accept_request(value)
        elif kind == "request":
            allowed = (
                self.core.model.mission.get("robot_ids", [])
                if self.core.model.state == "PREPARING"
                else self.core.model.members
            )
            if sender not in allowed:
                raise ValueError("control source is not a current member")
            self.accept_request(value)
        elif kind in {"media_manifest", "media_get", "media_blob"}:
            if kind == "media_manifest":
                if (
                    isinstance(value.get("records"), list)
                    and len(value["records"]) <= 200
                ):
                    for record in value["records"]:
                        historical = self.evidence_model(record.get("round_epoch"))
                        if (
                            historical
                            and sender in historical.sessions
                            and MediaStore.expected(historical, record)
                            and record["id"] not in self.media.records
                            and len(self.media_manifests) < 1800
                        ):
                            self.media_manifests[record["id"]] = (sender, record)
            elif kind == "media_get":
                identity = value.get("id")
                record = self.media.records.get(identity)
                historical = self.evidence_model((record or {}).get("round_epoch"))
                if (
                    historical
                    and sender in historical.sessions
                    and record
                    and MediaStore.expected(historical, record)
                    and len(self.media_serving) < 32
                ):
                    if (sender, identity) not in self.media_serving:
                        self.media_serving.append((sender, identity))
            elif kind == "media_blob":
                historical = self.evidence_model(value["record"].get("round_epoch"))
                if not historical or sender not in historical.sessions:
                    return
                self.media.receive(historical, value["record"], value["content"])
                self.media_manifests.pop(value["record"]["id"], None)
        elif kind == "raft":
            if value.get("sender") != sender:
                raise ValueError("Raft source identity mismatch")
            self.pub("raft_in", {"sender": sender, "data": value})
        elif kind == "hello":
            if value.get("robot_id") != sender:
                raise ValueError("heartbeat source mismatch")
            self.core.heartbeat(value, time.monotonic())
        elif kind == "agent_event":
            if value.get("robot_id") != sender:
                raise ValueError("execution source mismatch")
            if self.core.authority():
                self.core.receive_event(value)
        elif kind == "event_ack":
            if sender != self.core.consensus.get("leader_id"):
                return
            self.pub("ack", value)
        elif kind == "permit_request":
            m = self.core.model
            if (
                not self.core.authority()
                or value.get("robot_id") != sender
                or sender not in m.members
                or sender not in m.started
                or m.state != "ACTIVE"
                or sender in m.stopped
                or value.get("session_id") != m.sessions[sender]["session_id"]
                or value.get("round_epoch") != m.epoch
                or value.get("revision") != m.revision
                or value.get("route_name") != m.route_name(sender)
            ):
                return
            self.read_requests[value["request_id"]] = (
                copy.deepcopy(value),
                time.monotonic(),
            )
            self.pub("read", {"request_id": value["request_id"]})
        elif kind == "permit_grant":
            self.grants[value["request_id"]] = (sender, value, time.monotonic())
        else:
            raise ValueError("unsupported peer message")

    def consensus(self, value):
        try:
            if value["round_epoch"] != self.core.consensus.get("round_epoch"):
                self.archive_terminal()
                self.last_event_log = 0
            self.core.receive_consensus(value)
        except Exception as error:
            self.core.error = str(error)
            self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})
            raise

    def read_confirmed(self, proof):
        item = self.read_requests.pop(proof["request_id"], None)
        if (
            item
            and self.core.authority()
            and proof["term"] == self.core.consensus["term"]
        ):
            value, _ = item
            self.wire(
                value["robot_id"],
                "permit_grant",
                {**value, "term": proof["term"], "commit_index": proof["commit_index"]},
            )

    def agent_hello(self, value):
        if value["robot_id"] == self.robot:
            self.agent_state = value

    def agent_event(self, value):
        leader = self.core.consensus.get("leader_id")
        if leader:
            self.wire(leader, "agent_event", value)

    def permit_request(self, value):
        leader = self.core.consensus.get("leader_id")
        if leader:
            self.wire(leader, "permit_request", value)

    def ack(self, robot, session, event):
        value = {"session_id": session, "event_id": event}
        if robot == self.robot:
            self.pub("ack", value)
        elif self.core.authority():
            self.wire(robot, "event_ack", value)

    def local_stop(self, _request, response):
        self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})
        self.request_control("task.stop")
        response.success = True
        response.message = "已请求本机安全停止和全局取消；全局结论须多数派提交"
        return response

    def archive_terminal(self):
        m = self.core.model
        if m.state in TERMINAL and m.epoch and m.epoch not in self.history:
            atomic_json(self.root / "history" / f"{m.epoch}.json", m.__dict__)
            self.history[m.epoch] = copy.deepcopy(m)

    def evidence_model(self, epoch):
        return (
            self.core.model
            if epoch == self.core.model.epoch
            else self.history.get(epoch)
        )

    def report_storage(self, epoch):
        if epoch in self.credentials:
            return self.credentials[epoch]
        path = self.root / "reports" / f"round-{epoch}" / "credentials.json"
        if path.exists():
            self.credentials[epoch] = json.loads(path.read_text())
        return self.credentials.get(epoch)

    def control_reply(self, envelope):
        self.emit_platform(
            "services_reply",
            {
                "bid": envelope.get("bid", ""),
                "tid": envelope.get("tid", ""),
                "method": envelope["method"],
                "timestamp": int(time.time() * 1000),
                "_result": 0,
                "data": {
                    "round_epoch": self.core.model.epoch,
                    "commit_index": self.core.model.last_index,
                },
            },
        )

    def photo_event(self, event):
        if (
            event.identifier not in {"photo.complete", "algorithm.result"}
            or not self.agent_root
        ):
            return
        fields = {f.identifier: f.value for f in event.output_data}
        route = fields.get("route_name", "")
        from swarm_protocol.contracts import identifier

        try:
            identifier(route)
            path = (
                Path(self.agent_root).expanduser() / "route_archive" / (route + ".json")
            )
            record = json.loads(path.read_text())
            waypoint = fields.get("waypoint_id")
            if waypoint is None and fields.get("task_id", "").isdigit():
                waypoint = str(int(fields["task_id"]) // 100)
            tasks = record["mapping"].get(str(waypoint))
            if tasks is None:
                return
            identity = {
                k: record[k]
                for k in ("round_epoch", "revision", "route_name", "session_id")
            }
            identity.update(
                robot_id=self.robot, task_ids=tasks, waypoint_id=str(waypoint)
            )
            historical = self.evidence_model(identity["round_epoch"])
            if not historical or not MediaStore.expected(historical, identity):
                return
            urls = json.loads(fields.get("media_urls", "[]"))
            if fields.get("media_url"):
                urls.append(fields["media_url"])
            for uri in dict.fromkeys(urls):
                self.media.enqueue(identity, uri)
        except (OSError, ValueError, KeyError, TypeError):
            return

    def replicate_media(self, now):
        self.media.tick()
        m = self.core.model
        if not m.mission:
            return
        if now - self.last_manifest > 5.0:
            for historical in [m] + sorted(
                self.history.values(), key=lambda old: old.epoch
            )[-8:]:
                self.wire(
                    "*", "media_manifest", {"records": self.media.manifest(historical)}
                )
            self.last_manifest = now
        if now - self.last_media < 0.5:
            return
        self.last_media = now
        if self.media_serving:
            robot, identity = self.media_serving.pop(0)
            record = self.media.records[identity]
            self.wire(
                robot,
                "media_blob",
                {"record": record, "content": self.media.encoded(identity)},
            )
        elif self.media_manifests:
            identity, (robot, record) = next(iter(self.media_manifests.items()))
            self.wire(robot, "media_get", {"id": identity})
            # Rotate requests, so one missing source cannot starve others.
            del self.media_manifests[identity]
            self.media_manifests[identity] = (robot, record)

    def enqueue_request(self, envelope):
        identity = digest(
            {
                "epoch": envelope.get("data", {}).get(
                    "round_epoch", self.core.model.epoch
                ),
                "request": envelope,
            }
        )
        if identity in self.core.model.applied:
            self.control_reply(envelope)
            return
        if identity not in self.command_outbox and len(self.command_outbox) >= 128:
            raise ValueError("too many pending platform controls")
        self.command_outbox[identity] = copy.deepcopy(envelope)
        atomic_json(self.commands_path, self.command_outbox)

    def forward_requests(self, now):
        if now - self.last_relay < 1.0:
            return
        self.last_relay = now
        for identity, (envelope, deadline) in list(self.relay.items()):
            if now > deadline or envelope["data"].get(
                "round_epoch", 0
            ) < self.core.consensus.get("round_epoch", 0):
                del self.relay[identity]
                continue
            for robot in envelope["data"].get("robot_ids", []):
                if robot != self.robot:
                    self.wire(robot, "round_input", envelope)
        m = self.core.model
        changed = False
        for identity, envelope in list(self.command_outbox.items()):
            epoch = envelope.get("data", {}).get("round_epoch", m.epoch)
            if (
                epoch != m.epoch
                and self.pending_request
                and epoch == self.pending_request["data"]["round_epoch"]
            ):
                continue  # initial consensus admission is still in progress
            if identity in m.applied or m.state in TERMINAL or epoch != m.epoch:
                if identity in m.applied:
                    self.control_reply(envelope)
                del self.command_outbox[identity]
                changed = True
            elif self.core.authority():
                if envelope["method"] == "task.add":
                    if not any(
                        isinstance(x, dict) and x.get("request_id") == identity
                        for x in self.core.additions
                    ):
                        self.core.additions.append(
                            {"request_id": identity, "tasks": envelope["data"]["tasks"]}
                        )
                elif not self.core.controls:
                    self.core.controls.append({"request_id": identity})
            elif self.core.consensus.get("leader_id"):
                self.wire(self.core.consensus["leader_id"], "request", envelope)
        if changed:
            atomic_json(self.commands_path, self.command_outbox)

    def preflight(self):
        if (
            not self.core.ingress
            or self.fingerprint
            or self.map_working
            or self.map_error
        ):
            return
        self.map_working = True
        mission = self.core.ingress[0]
        epoch = mission["round_epoch"]
        radius, free = (
            self.get_parameter("footprint_radius").value,
            self.get_parameter("free_thresh").value,
        )

        def calculate():
            try:
                bundle = MapBundle(
                    mission["map_package"], radius, None if free < 0 else free
                )
                self.completed.put((epoch, bundle.fingerprint, ""))
            except Exception as error:
                self.completed.put((epoch, "", str(error)))

        self.pool.submit(calculate)

    def emit_platform(self, channel, payload):
        self.pub("platform", {"channel": channel, "payload": payload})

    def tick(self):
        now, wall = time.monotonic(), time.time()
        try:
            if self.pending_request:
                data = self.pending_request["data"]
                if data["round_epoch"] > self.core.consensus.get("round_epoch", 0):
                    self.pub(
                        "configure",
                        {
                            "round_epoch": data["round_epoch"],
                            "robot_ids": data["robot_ids"],
                        },
                    )
            self.preflight()
            while not self.completed.empty():
                epoch, fingerprint, error = self.completed.get()
                self.map_working = False
                if self.core.ingress and self.core.ingress[0]["round_epoch"] == epoch:
                    self.fingerprint, self.map_error = fingerprint, error
                    if error:
                        self.get_logger().error("地图准备失败：" + error)
            if now - self.last_hello >= 1.0:
                self.sequence += 1
                hello = {
                    **self.agent_state,
                    "robot_id": self.robot,
                    "coordinator_boot": self.boot,
                    "consensus_boot": self.core.consensus.get("boot_id", ""),
                    "sequence": self.sequence,
                    "map_digest": self.fingerprint,
                }
                self.core.heartbeat(hello, now)
                self.wire("*", "hello", hello)
                self.last_hello = now
            for key, (sender, value, at) in list(self.grants.items()):
                c, m = self.core.consensus, self.core.model
                if now - at > 3.0:
                    del self.grants[key]
                elif (
                    sender == c.get("leader_id")
                    and value["term"] == c.get("term")
                    and m.last_index >= value["commit_index"]
                    and m.state == "ACTIVE"
                    and self.robot in m.members
                    and self.robot not in m.stopped
                ):
                    self.pub("permit", value)
                    del self.grants[key]
            self.read_requests = {
                k: v for k, v in self.read_requests.items() if now - v[1] < 3.0
            }
            try:
                self.replicate_media(now)
            except (ValueError, OSError, KeyError) as error:
                self.get_logger().warning(
                    "图片证据后台失败，不改变任务状态：" + type(error).__name__
                )
            self.forward_requests(now)
            self.core.tick(now, wall)
            self.actions.tick()
            m = self.core.model
            summary = m.summary()
            summary.update(
                leader_id=self.core.consensus.get("leader_id", ""),
                quorum_available=self.core.authority()
                if self.core.consensus.get("role") == "leader"
                else self.core.consensus.get("leader_contact_recent", False),
                local_error=self.core.error,
            )
            publish(self.status_pub, summary)
            for event in m.events:
                if event["index"] > self.last_event_log:
                    self.get_logger().info(
                        f"任务提交：epoch={m.epoch} index={event['index']} "
                        f"revision={event['revision']} type={event['kind']} state={event['state']} "
                        f"{event['message']}"
                    )
                    self.last_event_log = event["index"]
            if now - self.last_platform >= 2.0:
                self.emit_platform(
                    "heartbeat",
                    {
                        "robot_id": self.robot,
                        "sn": self.robot,
                        **self.agent_state,
                        "round_epoch": m.epoch,
                        "leader_id": self.core.consensus.get("leader_id", ""),
                    },
                )
                if self.core.authority() and m.mission:
                    self.platform_progress(m)
                self.last_platform = now
            try:
                if m.state in TERMINAL and m.terminal_index and self.core.authority():
                    self.archive_terminal()
                    self.report_worker.generate_async(
                        m, self.report_storage(m.epoch), self.media
                    )
                    for historical in sorted(
                        self.history.values(), key=lambda old: old.epoch
                    )[-8:]:
                        self.report_worker.generate_async(
                            historical,
                            self.report_storage(historical.epoch),
                            self.media,
                        )
                self.report_worker.tick()
                self.report_error = ""
            except Exception as error:
                detail = type(error).__name__
                if detail != self.report_error:
                    self.get_logger().error("报告后台失败，任务终态保持：" + detail)
                    self.report_error = detail
        except Exception as error:
            self.get_logger().error(
                f"协调器保护性暂停：{type(error).__name__}: {error}"
            )
            self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})

    def platform_progress(self, m):
        summary = m.summary()
        base = {
            "bid": m.context.get("bid", m.mission["mission_id"]),
            "tid": m.context.get("tid", ""),
            "timestamp": int(time.time() * 1000),
            "type": "info",
        }
        self.emit_platform("state", summary)
        if m.epoch not in self.request_seen:
            self.emit_platform(
                "services_reply", {**base, "method": "task.execute", "_result": 0}
            )
            self.request_seen.add(m.epoch)
        # Committed facts are the durable notification source. MQTT PUBACK is
        # not an application ACK, so replay stable event IDs on every takeover.
        for event in snapshot_events(m, self.core.consensus["term"]):
            self.emit_platform("events", event)

    def destroy_node(self):
        self.actions.close()
        self.pool.shutdown(wait=True, cancel_futures=True)
        self.report_worker.close()
        self.media.close()
        self.lock.close()
        return super().destroy_node()


def main(args=None):
    main_node(CoordinatorNode, args)
