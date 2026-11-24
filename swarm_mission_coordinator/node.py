"""Device-local coordinator: replicated decisions, standard ROS String IO."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import PackageNotFoundError, version
import os
import queue
import re
import sys
import time
import uuid

from rclpy.node import Node
from rclpy.qos import DurabilityPolicy, QoSProfile
from std_msgs.msg import String
from std_srvs.srv import Trigger
from swarm_mission_planner.planner import map_fingerprint
from swarm_protocol.audit import Audit
from swarm_protocol.contracts import decode
from swarm_protocol.ros import main_node, publish, runtime_path
from swarm_protocol.storage import ProcessLock, atomic_json
from swarm_protocol.topics import DEFAULT_CLUSTER_ID

from inspection_interfaces.msg import PlatformEvent

from .api import MissionActions
from .coordinator import Coordinator
from .ingress import MissionIngress
from .mission_evidence import MissionEvidence
from .permits import ExecutionPermits
from .platform import snapshot_events


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
        self.declare_parameter("report_root", "")
        self.declare_parameter("runtime_info_path", "")
        self.robot, self.cluster = (
            self.get_parameter("robot_id").value,
            self.get_parameter("cluster_id").value,
        )
        self.root = runtime_path(self, "coordinator")
        self.lock = ProcessLock(self.root / "process.lock")
        self.boot, self.sequence = uuid.uuid4().hex, 0
        info = self.get_parameter("runtime_info_path").value
        if info:
            software = {"python": sys.version.split()[0], "ros_distro": os.environ.get("ROS_DISTRO", "")}
            for package in ("swarm_protocol", "swarm_consensus", "swarm_mission_planner", "swarm_mission_coordinator",
                            "swarm_execution_agent", "swarm_mqtt_transport"):
                try:
                    software[package] = version(package)
                except PackageNotFoundError:
                    software[package] = "unknown"
            atomic_json(info, {"robot_id": self.robot, "software": software, "coordinator_boot": self.boot})
        self.outputs = {
            name: self.create_publisher(String, topic, 100)
            for name, topic in {
                "configure": "/swarm/consensus/configure",
                "proposal": "/swarm/consensus/propose",
                "raft_in": "/swarm/consensus/inbound",
                "read": "/swarm/consensus/read",
                "fetch": "/swarm/consensus/fetch",
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
        self.fingerprint, self.map_error, self.map_working = "", "", False
        self.last_fetch = 0.0
        self.consensus_received = -float("inf")
        report_root = self.get_parameter("report_root").value
        if not report_root:
            report_root = self.root / "reports" if self.get_parameter("data_root").value else self.root.parent.parent / "reports"
        self.evidence = MissionEvidence(
            self.root, report_root,
            self.get_parameter("agent_data_root").value, self.get_parameter("media_roots").value or [],
            self.robot, self.core, self.wire, self.get_logger(),
            authority=self.report_authority,
        )
        self.permits = ExecutionPermits(self.core, self.robot, self.pub, self.wire)
        self.create_subscription(
            PlatformEvent, "/platform/action_events", self.evidence.photo_event, 100
        )
        self.pool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="swarm-preflight"
        )
        self.completed = queue.SimpleQueue()
        self.last_hello, self.last_platform = 0.0, 0.0
        self.last_event_log = (0, 0)
        self.last_plan_metrics = {}
        self.audit = Audit(self.root, self.robot, self.get_logger())
        self.request_seen = set()
        self.platform_cursor = (0, 0, 0)
        self.last_platform_replay = -float("inf")
        self.ingress = MissionIngress(
            self.root, self.robot, self.core, pub=self.pub, wire=self.wire,
            emit_platform=self.emit_platform, record=self.record,
            save_storage=self.evidence.save_storage, archive_terminal=self.evidence.archive_terminal,
            agent_state=lambda: self.agent_state, reset_preflight=self.reset_preflight,
        )
        for topic, callback in [
            ("/swarm/platform/inbound", self.ingress.platform_request),
            ("/swarm/network/inbound", self.network),
            ("/swarm/consensus/outbound", self.raft_out),
            ("/swarm/consensus/state", self.consensus),
            ("/swarm/consensus/entries", self.committed_entries),
            ("/swarm/consensus/read_confirmed", self.permits.read_confirmed),
            ("/swarm/agent/state", self.agent_hello),
            ("/swarm/agent/events", self.agent_event),
            ("/swarm/agent/permit_request", self.permits.permit_request),
            ("/swarm/planner/result", self.planner_result),
        ]:
            self.create_subscription(
                String, topic, lambda msg, cb=callback: self.safe(cb, msg), 100
            )
        self.create_service(Trigger, "/swarm/mission/stop", self.local_stop)
        self.create_service(Trigger, "/swarm/mission/reset", self.local_stop)
        self.actions = MissionActions(self)
        self.create_timer(0.1, self.tick)

    def record(self, kind, value):
        model, consensus = self.core.model, self.core.consensus
        pending = self.core.ingress[0] if self.core.ingress else {}
        epoch = model.epoch or pending.get("round_epoch", 0)
        revision = model.revision
        plan_id = value.get("request_id", "")
        if kind in {"planner", "planner_result"} and isinstance(plan_id, str):
            match = re.match(r"^(\d+)-r(\d+)-", plan_id)
            if match:
                epoch, revision = map(int, match.groups())
        self.audit.record(kind, value, epoch=epoch, revision=revision,
                          term=consensus.get("term", 0), commit_index=model.last_index,
                          request_id=value.get("request_id") or (
                              f"sw{epoch}-r{revision}" if epoch else ""))

    def planner_result(self, value):
        # Late results still go through the coordinator's request fence.
        if self.core.plan_request and value.get("request_id") == self.core.plan_request:
            self.last_plan_metrics = {
                **value.get("metrics", {}),
                "request_id": value["request_id"],
                "round_epoch": self.core.model.epoch,
                "revision": self.core.model.revision,
            }
        self.record("planner_result", value)
        self.core.receive_plan(value)

    def pub(self, name, value):
        publish(self.outputs[name], value)
        if name in {"planner", "decision", "ack"}:
            self.record(name, value)

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
            self.record("peer_round_input", value)
            self.ingress.accept_request(value)
        elif kind == "request":
            allowed = (
                self.core.model.mission.get("robot_ids", [])
                if self.core.model.state == "PREPARING"
                else self.core.model.members
            )
            if sender not in allowed:
                raise ValueError("control source is not a current member")
            self.record("peer_control", value)
            self.ingress.accept_request(value)
        elif kind in {"media_manifest", "media_get", "media_blob"}:
            self.evidence.receive_media(sender, kind, value)
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
        elif kind in {"permit_request", "permit_grant"}:
            self.permits.receive(sender, kind, value)
        else:
            raise ValueError("unsupported peer message")

    def report_authority(self):
        return self.core.authority() and time.monotonic() - self.consensus_received < 0.5

    def consensus(self, value):
        try:
            if value["round_epoch"] != self.core.consensus.get("round_epoch"):
                self.evidence.archive_terminal()
                self.last_event_log = (0, 0)
            self.core.receive_consensus(value)
            self.consensus_received = time.monotonic()
        except Exception as error:
            self.core.error = str(error)
            self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})
            raise

    def committed_entries(self, value):
        try:
            if self.core.receive_entries(value):
                self.last_fetch = 0.0
        except Exception as error:
            self.core.error = str(error)
            self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})
            raise

    def fetch_committed(self, now):
        consensus = self.core.consensus
        after = self.core.model.last_index
        if after < consensus.get("commit_index", 0) and now - self.last_fetch >= 0.2:
            self.last_fetch = now
            self.pub("fetch", {"round_epoch": consensus["round_epoch"],
                               "boot_id": consensus["boot_id"], "after": after})

    def agent_hello(self, value):
        if value["robot_id"] == self.robot:
            self.agent_state = value

    def agent_event(self, value):
        leader = self.core.consensus.get("leader_id")
        if leader:
            self.wire(leader, "agent_event", value)

    def ack(self, robot, session, event):
        value = {"session_id": session, "event_id": event}
        if robot == self.robot:
            self.pub("ack", value)
        elif self.core.authority():
            self.wire(robot, "event_ack", value)

    def local_stop(self, _request, response):
        self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})
        self.ingress.request_control("task.stop")
        response.success = True
        response.message = "已请求本机安全停止和全局取消；全局结论须多数派提交"
        return response

    def reset_preflight(self):
        self.fingerprint, self.map_error = "", ""

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
                fingerprint = map_fingerprint(
                    mission["map_package"], radius, None if free < 0 else free
                )
                self.completed.put((epoch, fingerprint, ""))
            except Exception as error:
                self.completed.put((epoch, "", str(error)))

        self.pool.submit(calculate)

    def emit_platform(self, channel, payload):
        self.pub("platform", {"channel": channel, "payload": payload})
        if channel in {"events", "services_reply"}:
            self.record("platform_" + channel, payload)

    def tick(self):
        now, wall = time.monotonic(), time.time()
        try:
            self.fetch_committed(now)
            if self.ingress.pending_request:
                data = self.ingress.pending_request["data"]
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
            self.permits.tick(now)
            self.ingress.forward_requests(now)
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
                identity = (m.epoch, event["index"])
                if identity > self.last_event_log:
                    self.record("commit", event)
                    self.get_logger().info(
                        f"任务提交：epoch={m.epoch} index={event['index']} "
                        f"revision={event['revision']} type={event['kind']} state={event['state']} "
                        f"{event['message']}"
                    )
                    self.last_event_log = identity
            if now - self.last_platform >= 2.0:
                self.emit_platform(
                    "heartbeat",
                    {
                        "robot_id": self.robot,
                        "sn": self.robot,
                        **self.agent_state,
                        "round_epoch": m.epoch,
                        "leader_id": self.core.consensus.get("leader_id", ""),
                        "coordinator_boot": self.boot,
                        "sequence": self.sequence,
                        "term": self.core.consensus.get("term", 0),
                        "consensus_role": self.core.consensus.get("role", ""),
                        "authority": self.core.authority(),
                        "voter_count": len(m.mission.get("robot_ids", [])),
                        "quorum_required": len(m.mission["robot_ids"]) // 2 + 1 if m.mission else 0,
                        "commit_index": self.core.consensus.get("commit_index", 0),
                        "applied_index": m.last_index,
                        "planner_metrics": self.last_plan_metrics
                        if self.last_plan_metrics.get("round_epoch") == m.epoch else {},
                    },
                )
                if self.core.authority() and m.mission:
                    self.platform_progress(m)
                self.last_platform = now
        except Exception as error:
            self.get_logger().error(
                f"协调器保护性暂停：{type(error).__name__}: {error}"
            )
            self.pub("decision", {"command": "CANCEL", "robot_id": self.robot})
        self.evidence.tick(now)

    def platform_progress(self, m):
        summary = m.summary()
        base = {
            "bid": m.context.get("bid", m.mission["mission_id"]),
            "tid": m.context.get("tid", ""),
            "timestamp": int(time.time() * 1000),
            "type": "info",
        }
        self.emit_platform("state", {**summary, "term": self.core.consensus.get("term", 0)})
        if m.epoch not in self.request_seen:
            self.emit_platform(
                "services_reply", {**base, "method": "task.execute", "_result": 0}
            )
            self.request_seen.add(m.epoch)
        # Send new facts on each update. Periodic replay repairs delivery loss
        # without retransmitting the full route history every two seconds.
        now = time.monotonic()
        epoch, term, after = self.platform_cursor
        current_term = self.core.consensus["term"]
        if (epoch, term) != (m.epoch, current_term) or now - self.last_platform_replay >= 30.0:
            after = 0
            self.last_platform_replay = now
        for event in snapshot_events(m, current_term, after=after):
            self.emit_platform("events", event)
        self.platform_cursor = (m.epoch, current_term, m.last_index)

    def destroy_node(self):
        self.audit.close()
        self.actions.close()
        self.pool.shutdown(wait=True, cancel_futures=True)
        self.evidence.close()
        self.lock.close()
        return super().destroy_node()


def main(args=None):
    main_node(CoordinatorNode, args)
