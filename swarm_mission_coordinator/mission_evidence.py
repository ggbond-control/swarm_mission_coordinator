"""Report history and photo evidence, independent of execution decisions."""

import copy
import json
from pathlib import Path

from swarm_protocol.contracts import TERMINAL, identifier
from swarm_protocol.storage import atomic_json

from .media import MediaStore
from .model import MissionState
from .reporting import ReportWorker


class MissionEvidence:
    def __init__(self, root, report_root, agent_root, media_roots, robot, core, wire, logger, *, authority):
        self.root, self.agent_root, self.robot = root, agent_root, robot
        self.core, self.wire, self.logger = core, wire, logger
        self.authority = authority
        self.upload_root = root / "private_uploads"
        self.report_worker = ReportWorker(report_root, logger, job_root=self.upload_root)
        self.media = MediaStore(root / "media", media_roots)
        self.credentials = {}
        self.history = {}
        for path in sorted((root / "history").glob("*.json")):
            try:
                historical = MissionState(**json.loads(path.read_text()))
                self.history[historical.epoch] = historical
            except (OSError, ValueError, TypeError):
                logger.warning("历史报告状态不可读，保留文件：" + str(path))
        self.media_manifests = {}
        self.media_serving = []
        self.last_media, self.last_manifest = 0.0, 0.0
        self.report_error = ""

    def save_storage(self, epoch, storage):
        atomic_json(self.upload_root / f"round-{epoch}" / "credentials.json", storage)
        self.credentials[epoch] = storage

    def receive_media(self, sender, kind, value):
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
        path = self.upload_root / f"round-{epoch}" / "credentials.json"
        if path.exists():
            self.credentials[epoch] = json.loads(path.read_text())
        return self.credentials.get(epoch)
    def photo_event(self, event):
        if (
            event.identifier not in {"photo.complete", "algorithm.result"}
            or not self.agent_root
        ):
            return
        fields = {f.identifier: f.value for f in event.output_data}
        route = fields.get("route_name", "")

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
            rounds = {old.epoch: old for old in sorted(
                self.history.values(), key=lambda old: old.epoch
            )[-8:]}
            rounds[m.epoch] = m
            for historical in rounds.values():
                records = self.media.manifest(historical)
                if records:
                    self.wire("*", "media_manifest", {"records": records})
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

    def tick(self, now):
        try:
            self.replicate_media(now)
        except (ValueError, OSError, KeyError, TypeError) as error:
            self.logger.warning("图片证据处理失败：" + type(error).__name__)
        try:
            model = self.core.model
            if model.state in TERMINAL and model.terminal_index:
                self.archive_terminal()
                self.report_worker.generate_async(model, self.report_storage(model.epoch), self.media)
            if self.core.authority():
                for historical in sorted(self.history.values(), key=lambda old: old.epoch)[-8:]:
                    if historical.epoch != model.epoch:
                        self.report_worker.generate_async(
                            historical, self.report_storage(historical.epoch), self.media
                        )
            term = tuple(self.core.consensus.get(k) for k in ("round_epoch", "term", "boot_id"))

            def authorized():
                return self.authority() and term == tuple(
                    self.core.consensus.get(k) for k in ("round_epoch", "term", "boot_id")
                )

            self.report_worker.tick(authorized=authorized)
            self.report_error = ""
        except (ValueError, OSError, KeyError, TypeError, RuntimeError) as error:
            detail = type(error).__name__
            if detail != self.report_error:
                self.logger.error("报告后台失败：" + detail)
                self.report_error = detail

    def close(self):
        self.report_worker.close()
        self.media.close()
