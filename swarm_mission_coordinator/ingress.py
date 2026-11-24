"""Request validation, durable admission, and forwarding to the elected leader."""

import copy
import json
import time
import uuid

from swarm_protocol.contracts import TERMINAL, digest, normalize_mission, normalize_task, public_mission
from swarm_protocol.replication import validate_command_size
from swarm_protocol.storage import atomic_json


class MissionIngress:
    def __init__(self, root, robot, core, *, pub, wire, emit_platform, record,
                 save_storage, archive_terminal, agent_state, reset_preflight):
        self.root, self.robot, self.core = root, robot, core
        self.pub, self.wire, self.emit_platform, self.record = pub, wire, emit_platform, record
        self.save_storage, self.archive_terminal = save_storage, archive_terminal
        self.agent_state, self.reset_preflight = agent_state, reset_preflight
        self.pending_request = None
        self.relay = {}
        self.last_relay = 0.0
        self.commands_path = root / "private_commands.json"
        self.command_outbox = (
            json.loads(self.commands_path.read_text())
            if self.commands_path.exists()
            else {}
        )
        pending_path = root / "private_request.json"
        if pending_path.exists():
            self.pending_request = json.loads(pending_path.read_text())
            self.restore_ingress(self.pending_request)

    def restore_ingress(self, envelope):
        mission = normalize_mission(envelope["data"], envelope["bid"])
        if self.robot not in mission["robot_ids"]:
            return
        if "storage" in mission:
            self.save_storage(mission["round_epoch"], mission["storage"])
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
        self.record("platform_in", envelope)
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
            tasks = [normalize_task(t) for t in data["tasks"]]
            validate_command_size({"tasks": tasks, "ingress": "x" * 2048})
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
        validate_command_size({
            "mission": public_mission(mission), "ingress": "x" * 2048,
        })
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
        if self.agent_state().get("start_phase") in {"starting", "started"}:
            raise ValueError("local previous execution not stopped")
        self.archive_terminal()
        atomic_json(self.root / "private_request.json", envelope)
        self.pending_request = copy.deepcopy(envelope)
        self.restore_ingress(envelope)
        self.reset_preflight()
        self.pub(
            "configure",
            {"round_epoch": mission["round_epoch"], "robot_ids": mission["robot_ids"]},
        )

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
        m = self.core.model
        for identity, (envelope, deadline) in list(self.relay.items()):
            epoch = envelope["data"].get("round_epoch", 0)
            activated = epoch == m.epoch and m.state not in {"EMPTY", "PREPARING"}
            if now > deadline or epoch < self.core.consensus.get("round_epoch", 0) or activated:
                del self.relay[identity]
                continue
            for robot in envelope["data"].get("robot_ids", []):
                if robot != self.robot:
                    self.wire(robot, "round_input", envelope)
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
