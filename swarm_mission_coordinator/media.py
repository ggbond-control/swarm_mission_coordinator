"""Bounded image replication outside the consensus/control stream.

Only local files under explicitly configured roots are read. Remote URLs are
not fetched. Membership and route/session identity are checked before receipt.
"""

import base64
import copy
import hashlib
import io
import json
import queue
import threading
from pathlib import Path
from urllib.parse import unquote, urlsplit

from PIL import Image, ImageOps
from swarm_protocol.contracts import digest
from swarm_protocol.storage import atomic_json

MAX_IMAGE = 512 * 1024
MAX_ITEMS = 200


class MediaStore:
    def __init__(self, root, allowed_roots=()):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.allowed = [
            Path(p).expanduser().resolve() for p in allowed_roots if p.strip()
        ]
        self.records = {}
        for path in self.root.glob("*.json"):
            try:
                record = json.loads(path.read_text())
                self.records[record["id"]] = record
            except (ValueError, KeyError, OSError):
                continue
        self.pending = queue.Queue(maxsize=32)
        self.finished = queue.Queue(maxsize=32)
        self.closed = False
        self.thread = threading.Thread(
            target=self.worker, name="swarm-local-images", daemon=True
        )
        self.thread.start()

    @staticmethod
    def expected(model, record):
        if record.get("round_epoch") != model.epoch:
            return False
        robot = record.get("robot_id")
        revision = record.get("revision")
        if record.get("session_id") != model.sessions.get(robot, {}).get("session_id"):
            return False
        route = f"sw{model.epoch}-{digest(model.mission.get('mission_id', ''))[:16]}-r{revision}-{robot}"
        if record.get("route_name") != route:
            return False
        plan = next(
            (
                e.get("routes", {}).get(robot)
                for e in model.events
                if e["kind"] == "PLAN" and e["revision"] == revision
            ),
            None,
        )
        return plan is not None and set(record.get("task_ids", [])) <= set(
            plan["task_ids"]
        )

    def enqueue(self, identity, uri):
        if self.closed:
            return False
        try:
            self.pending.put_nowait((copy.deepcopy(identity), uri))
        except queue.Full:
            return False
        return True

    def worker(self):
        while not self.closed:
            try:
                item = self.pending.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                return
            identity, uri = item
            try:
                parsed = urlsplit(uri)
                if parsed.scheme not in {"", "file"} or parsed.netloc:
                    continue
                path = Path(unquote(parsed.path)).expanduser().resolve()
                if not any(path.is_relative_to(root) for root in self.allowed):
                    continue
                if not path.is_file() or path.stat().st_size > 20 * 1024 * 1024:
                    continue
                with Image.open(path) as original:
                    if original.width * original.height > 24000000:
                        continue
                    image = ImageOps.exif_transpose(original).convert("RGB")
                    image.thumbnail((1600, 1600))
                    out = io.BytesIO()
                    image.save(out, format="JPEG", quality=80)
                    data = out.getvalue()
                    if len(data) > MAX_IMAGE:
                        image.thumbnail((800, 800))
                        out = io.BytesIO()
                        image.save(out, format="JPEG", quality=65)
                        data = out.getvalue()
                    if len(data) > MAX_IMAGE:
                        continue
                record = {
                    **identity,
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "bytes": len(data),
                    "mime": "image/jpeg",
                }
                record["id"] = digest(record)
                self.finished.put((record, data), timeout=1.0)
            except Exception:
                # Media is supplementary evidence. Never affect navigation result.
                continue

    def tick(self):
        while True:
            try:
                record, data = self.finished.get_nowait()
            except queue.Empty:
                return
            self.save(record, data)

    def save(self, record, data):
        if (
            len(data) > MAX_IMAGE
            or record.get("bytes") != len(data)
            or record.get("mime") != "image/jpeg"
            or hashlib.sha256(data).hexdigest() != record.get("sha256")
            or record.get("id")
            != digest({k: v for k, v in record.items() if k != "id"})
        ):
            raise ValueError("invalid image evidence digest")
        epoch = record["round_epoch"]
        if (
            record["id"] not in self.records
            and sum(r["round_epoch"] == epoch for r in self.records.values())
            >= MAX_ITEMS
        ):
            return False
        path = self.root / (record["id"] + ".jpg")
        path.write_bytes(data)
        atomic_json(path.with_suffix(".json"), record)
        self.records[record["id"]] = copy.deepcopy(record)
        return True

    def manifest(self, model):
        return [r for r in self.records.values() if self.expected(model, r)]

    def receive(self, model, record, encoded):
        if not self.expected(model, record) or len(encoded) > 4 * (
            (MAX_IMAGE + 2) // 3
        ):
            raise ValueError("image source/round/route not authorized")
        return self.save(record, base64.b64decode(encoded, validate=True))

    def encoded(self, identity):
        return base64.b64encode((self.root / (identity + ".jpg")).read_bytes()).decode()

    def report_images(self, model):
        return [
            {**r, "data": self.encoded(r["id"])}
            for r in sorted(self.manifest(model), key=lambda r: r["id"])
        ]

    def close(self):
        self.closed = True
        try:
            self.pending.put_nowait(None)
        except queue.Full:
            pass
        self.thread.join(timeout=4.0)
