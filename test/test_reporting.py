"""Loopback S3 CAS and durable worker tests; no production credentials or storage."""

import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from support import MissionCluster
from swarm_mission_coordinator.reporting import ReportWorker, render
from swarm_mission_coordinator.storage_upload import conditional_upload


@pytest.fixture
def s3():
    objects, requests = {}, []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            if self.path not in objects:
                self.send_response(404)
                self.end_headers()
                return
            item = objects[self.path]
            self.send_response(200)
            for k, v in item["headers"].items():
                if k.startswith("x-amz-meta-"):
                    self.send_header(k, v)
            self.send_header("ETag", item["etag"])
            self.send_header("Content-Length", str(len(item["body"])))
            self.end_headers()

        def do_GET(self):
            item = objects[self.path]
            self.send_response(200)
            self.send_header("Content-Length", str(len(item["body"])))
            self.end_headers()
            self.wfile.write(item["body"])

        def do_PUT(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append((self.path, dict(self.headers), body))
            old = objects.get(self.path)
            if (self.headers.get("If-None-Match") == "*" and old) or (
                self.headers.get("If-Match")
                and (not old or self.headers["If-Match"] != old["etag"])
            ):
                self.send_response(412)
                self.end_headers()
                return
            objects[self.path] = {
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body,
                "etag": '"' + hashlib.md5(body).hexdigest() + '"',
            }
            self.send_response(200)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    storage = {
        "endpoint": f"http://127.0.0.1:{server.server_port}",
        "bucket": "test-bucket",
        "objectKeyPrefix": "tmp/test",
        "stsCredentials": {
            "accessKey": "FAKE_LOCAL_KEY",
            "secretKey": "FAKE_LOCAL_SECRET",
            "sessionToken": "FAKE_TOKEN",
        },
    }
    yield storage, objects, requests
    server.shutdown()
    server.server_close()
    thread.join()


def test_conditional_put_signing_exact_key_and_old_leader_fencing(s3):
    storage, objects, requests = s3
    key = "tmp/test/bid.html"
    assert conditional_upload(storage, key, b"<html>one</html>", 10, 20) == "uploaded"
    path, headers, body = requests[-1]
    assert path == "/test-bucket/tmp/test/bid.html"
    assert headers["If-None-Match"] == "*"
    assert headers["X-Amz-Security-Token"] == "FAKE_TOKEN"
    assert "AWS4-HMAC-SHA256" in headers["Authorization"]
    assert conditional_upload(storage, key, body, 10, 20) == "unchanged"
    assert len(requests) == 1
    assert conditional_upload(storage, key, b"<html>new</html>", 10, 21) == "uploaded"
    assert "If-Match" in requests[-1][1]
    assert conditional_upload(storage, key, b"old leader", 10, 20) == "superseded"
    assert objects[path]["body"] == b"<html>new</html>"
    objects[path]["headers"].clear()
    with pytest.raises(RuntimeError, match="unversioned"):
        conditional_upload(storage, key, b"bad overwrite", 10, 22)


def test_worker_defers_put_when_authority_is_lost_during_s3_probe(tmp_path, s3):
    storage, _, requests = s3
    model = terminal(tmp_path / "map")
    model.report_key = "tmp/test/deferred.html"
    worker = ReportWorker(tmp_path / "reports", Logger(), job_root=tmp_path / "private")
    worker.register(model, storage)
    try:
        decisions = iter([True, True, True, False])
        worker.tick(authorized=lambda: next(decisions))
        settle(worker)
        job = json.loads((tmp_path / "private/round-1/job.json").read_text())
        assert job["status"] == "pending" and job["attempts"] == 0
        assert not requests
        worker.tick()
        settle(worker)
        assert len(requests) == 1
    finally:
        worker.close()


class Logger:
    def __init__(self):
        self.lines = []

    def info(self, text):
        self.lines.append(text)

    def warning(self, text):
        self.lines.append(text)


def terminal(tmp_path):
    cluster = MissionCluster(tmp_path, count=1, tasks=1)
    cluster.run(3)
    cluster.finish_routes()
    cluster.run(1)
    assert cluster.leader().model.state == "COMPLETED"
    return cluster.leader().model


def settle(worker):
    end = time.monotonic() + 5
    while worker.running and time.monotonic() < end:
        time.sleep(0.01)
    assert not worker.running


def test_worker_restart_retry_budget_isolation_and_credentials_private(tmp_path, s3):
    storage, _, _ = s3
    model = terminal(tmp_path / "map")
    model.report_key = "tmp/test/demo.html"
    logger = Logger()
    calls = []

    def failing(*args):
        calls.append(args)
        raise TimeoutError("sensitive library detail")

    root = tmp_path / "reports"
    worker = ReportWorker(root, logger, upload=failing)
    path = worker.register(model, storage)
    worker.tick()
    settle(worker)
    worker.close()
    disk = root / "round-1" / "job.json"
    assert json.loads(disk.read_text())["attempts"] == 1
    assert (root / "round-1" / "credentials.json").stat().st_mode & 0o777 == 0o600
    worker = ReportWorker(root, logger, upload=failing)
    for _ in range(3):
        worker.jobs[str(root / "round-1")]["next_attempt"] = 0
        worker.tick()
        settle(worker)
    worker.tick()
    settle(worker)
    worker.close()
    job = json.loads(disk.read_text())
    assert job["status"] == "failed" and job["attempts"] == 4 and len(calls) == 4
    assert all(
        call[1] == "tmp/test/demo.html" and call[3:] == (1, model.terminal_index)
        for call in calls
    )
    assert (
        "FAKE_LOCAL_SECRET"
        not in path.read_text() + repr(logger.lines) + disk.read_text()
    )
    assert "sensitive library detail" not in repr(logger.lines)


def test_slow_upload_does_not_block_register_or_next_round(tmp_path, s3):
    storage, _, _ = s3
    model = terminal(tmp_path / "map")
    gate = threading.Event()

    def slow(*args):
        gate.wait(3)
        return "uploaded"

    worker = ReportWorker(tmp_path / "reports", Logger(), upload=slow)
    model.report_key = "tmp/test/demo.html"
    worker.register(model, storage)
    before = time.monotonic()
    worker.tick()
    assert time.monotonic() - before < 0.5
    # A later round retains its own registered upload and identity.
    import copy

    later = copy.deepcopy(model)
    later.epoch = 2
    later.report_key = "tmp/test/next.html"
    worker.register(later, storage)
    assert len(worker.jobs) == 2
    gate.set()
    settle(worker)
    worker.tick()
    settle(worker)
    worker.close()
    assert all(j["status"] == "succeeded" for j in worker.jobs.values())
    assert "导航成功到达".encode() in render(model)


def test_corrupt_upload_job_does_not_prevent_coordinator_start(tmp_path):
    folder = tmp_path / "round-1"
    folder.mkdir()
    (folder / "job.json").write_text("bad JSON")
    logger = Logger()
    worker = ReportWorker(tmp_path, logger)
    assert not worker.jobs and logger.lines
    worker.close()


def test_same_terminal_report_cannot_drop_previously_uploaded_images(s3):
    storage, objects, requests = s3
    marker = '<script type="application/json" id="swarm-media-manifest">'
    old = (marker + '["image-a"]</script>').encode()
    newer = (marker + '["image-a","image-b"]</script>').encode()
    assert conditional_upload(storage, "tmp/test/media.html", old, 10, 20) == "uploaded"
    assert (
        conditional_upload(storage, "tmp/test/media.html", newer, 10, 20) == "uploaded"
    )
    assert (
        conditional_upload(storage, "tmp/test/media.html", old, 10, 20) == "superseded"
    )
    assert len(requests) == 2
