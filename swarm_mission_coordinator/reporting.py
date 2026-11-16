"""Standalone HTML, durable background jobs, and conditional S3 object updates."""

import copy
import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from html import escape
from pathlib import Path

from swarm_protocol.storage import atomic_json

from .storage_upload import conditional_upload


def render(model, images=()):
    summary = model.summary()
    rows = []
    for number, task in enumerate(model.mission["tasks"], 1):
        identity = task["id"]
        reason = (
            model.unknown.get(identity, {}) or model.unassignable.get(identity, {})
        ).get("reason", "")
        operations = ", ".join(op["method"] for op in task["tasks"]) or "纯导航"
        rows.append(
            "<tr>"
            + "".join(
                "<td>" + escape(str(v)) + "</td>"
                for v in (
                    number,
                    identity,
                    model.tasks[identity],
                    model.owners.get(identity, ""),
                    operations,
                    reason,
                )
            )
            + "</tr>"
        )
    timeline = "\n".join(
        f"<li>#{e['index']} {escape(e['kind'])} / {escape(e['state'])} {escape(e['message'])}</li>"
        for e in model.events
    )
    manifest = json.dumps([image["id"] for image in images], separators=(",", ":"))
    gallery = "".join(
        '<figure><img style="max-width:100%" src="data:image/jpeg;base64,'
        + image["data"]
        + '">'
        "<figcaption>"
        + escape(
            str(image["robot_id"])
            + " / r"
            + str(image["revision"])
            + " / "
            + ", ".join(image["task_ids"])
        )
        + "</figcaption></figure>"
        for image in images
    )
    return (
        '<!doctype html><html lang="zh"><meta charset="utf-8"><title>多机任务报告</title>'
        "<style>body{font:16px sans-serif;max-width:1200px;margin:36px auto}table{border-collapse:collapse}"
        "td,th{border:1px solid #bbb;padding:8px}pre{white-space:pre-wrap}</style>"
        f"<h1>任务 {escape(model.mission['mission_id'])}</h1><p>{escape(model.reason)}</p>"
        "<p>完成点数依据导航成功到达，不代表挂载子任务全部执行成功。</p>"
        f"<pre>{escape(json.dumps(summary, ensure_ascii=False, indent=2))}</pre>"
        "<table><tr><th>整轮点号</th><th>原始ID</th><th>状态</th><th>设备</th><th>挂载操作</th><th>原因</th></tr>"
        + "".join(rows)
        + f"</table><h2>提交时间线</h2><ol>{timeline}</ol><h2>图片证据</h2>"
        + (gallery or "<p>尚无已同步的图片；不影响导航到达判据。</p>")
        + '<script type="application/json" id="swarm-media-manifest">'
        + manifest
        + "</script></html>"
    ).encode()


class ReportWorker:
    def __init__(self, root, logger, upload=conditional_upload):
        self.root, self.logger, self.upload = Path(root), logger, upload
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="swarm-report")
        self.generation_pool = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="swarm-report-generation"
        )
        self.generation = None
        self.generated_signatures = {}
        self.jobs = {}
        self.lock = threading.Lock()
        self.running = False
        self.closed = False
        for path in self.root.glob("*/job.json"):
            try:
                job = json.loads(path.read_text())
                if job["status"] not in {"succeeded", "failed"}:
                    self.jobs[str(path.parent)] = job
            except (ValueError, KeyError, OSError):
                self.logger.warning("报告任务文件不可读，保留文件供排查：" + str(path))

    def generate_async(self, model, storage, media):
        manifest = media.manifest(model)
        signature = (
            model.epoch,
            model.terminal_index,
            tuple(sorted(r["id"] for r in manifest)),
        )
        if self.generation and not self.generation.done():
            return
        if self.generation:
            previous = self.generation
            self.generation = None
            previous.result()  # caller logs failure independently of control
        if signature == self.generated_signatures.get(model.epoch):
            return
        model = copy.deepcopy(model)
        storage = copy.deepcopy(storage)

        def generate():
            images = [
                {**r, "data": media.encoded(r["id"])}
                for r in sorted(manifest, key=lambda r: r["id"])
            ]
            self.register(model, storage, images)
            self.generated_signatures[model.epoch] = signature

        self.generation = self.generation_pool.submit(generate)

    def register(self, model, storage=None, images=()):
        folder = self.root / f"round-{model.epoch}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / (model.mission["mission_id"] + ".html")
        version = model.terminal_index
        content = render(model, images)
        content_digest = hashlib.sha256(content).hexdigest()
        with self.lock:
            old = self.jobs.get(str(folder))
            if old and (
                old["version"] > version or old.get("content_digest") == content_digest
            ):
                return path
            disk = folder / "job.json"
            if disk.exists():
                disk_job = json.loads(disk.read_text())
                if (
                    disk_job["version"] > version
                    or disk_job.get("content_digest") == content_digest
                ):
                    return path
            path.write_bytes(content)
            if storage:
                atomic_json(folder / "credentials.json", storage)
                snapshot = folder / ("upload-" + content_digest + ".html")
                snapshot.write_bytes(content)
                job = {
                    "epoch": model.epoch,
                    "version": version,
                    "content_digest": content_digest,
                    "key": model.report_key,
                    "path": str(snapshot),
                    "attempts": 0,
                    "next_attempt": 0.0,
                    "status": "pending",
                    "error": "",
                }
                atomic_json(disk, job)
                self.jobs[str(folder)] = job
        self.logger.info(f"报告已生成：round={model.epoch} path={path}")
        return path

    def tick(self):
        with self.lock:
            if self.running or self.closed:
                return
            candidates = [
                (folder, job)
                for folder, job in self.jobs.items()
                if job["status"] in {"pending", "uploading"}
                and time.time() >= job["next_attempt"]
            ]
            if not candidates:
                return
            folder, job = candidates[0]
            job["status"] = "uploading"
            atomic_json(Path(folder) / "job.json", job)
            self.running = True
        self.pool.submit(self._run, folder, job.copy())

    def _run(self, folder, job):
        try:
            storage = json.loads((Path(folder) / "credentials.json").read_text())
            self.logger.info(
                f"报告上传开始：round={job['epoch']} bucket={storage['bucket']} key={job['key']}"
            )
            outcome = self.upload(
                storage,
                job["key"],
                Path(job["path"]).read_bytes(),
                job["epoch"],
                job["version"],
            )
            job["status"], job["error"] = "succeeded", ""
            self.logger.info(
                f"报告上传完成：round={job['epoch']} key={job['key']} outcome={outcome}"
            )
        except Exception as error:
            job["attempts"] += 1
            job["status"] = "failed" if job["attempts"] >= 4 else "pending"
            job["next_attempt"] = time.time() + [5, 15, 30, 30][job["attempts"] - 1]
            job["error"] = type(error).__name__
            self.logger.warning(
                f"报告上传{job['status']}：round={job['epoch']} attempt={job['attempts']} error={job['error']}"
            )
        finally:
            with self.lock:
                try:
                    if self.jobs[folder].get("content_digest") != job.get(
                        "content_digest"
                    ):
                        return  # late completion belongs to a superseded snapshot
                    atomic_json(Path(folder) / "job.json", job)
                    self.jobs[folder] = job
                except OSError:
                    self.logger.warning("报告上传状态持久化失败，保留原任务供重启核对")
                    self.jobs[folder]["status"] = "failed"
                finally:
                    self.running = False

    def close(self):
        self.closed = True
        self.generation_pool.shutdown(wait=True, cancel_futures=True)
        self.pool.shutdown(wait=True, cancel_futures=True)
