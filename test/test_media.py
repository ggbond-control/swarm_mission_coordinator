import base64
import time
from types import SimpleNamespace

import pytest
from PIL import Image
from support import MissionCluster
from swarm_mission_coordinator.media import MediaStore
from swarm_mission_coordinator.mission_evidence import MissionEvidence
from swarm_mission_coordinator.reporting import render


def test_images_preserve_revision_and_reject_old_round_and_paths(tmp_path):
    cluster = MissionCluster(tmp_path / "maps", count=1, tasks=1)
    cluster.run(3)
    m = cluster.leader().model
    root = tmp_path / "photos"
    root.mkdir()
    path = root / "image.png"
    Image.new("RGB", (30, 30), "red").save(path)
    left = MediaStore(tmp_path / "left", [str(root)])
    right = MediaStore(tmp_path / "right")
    identity = {
        "robot_id": "R0",
        "session_id": "R0-agent",
        "round_epoch": m.epoch,
        "revision": m.revision,
        "route_name": m.route_name("R0"),
        "task_ids": ["p0"],
        "waypoint_id": "1",
    }
    try:
        assert left.enqueue(identity, str(path))
        deadline = time.monotonic() + 3
        while not left.records and time.monotonic() < deadline:
            left.tick()
            time.sleep(0.01)
        record = next(iter(left.records.values()))
        assert right.receive(m, record, left.encoded(record["id"]))
        assert "data:image/jpeg;base64," in render(m, right.report_images(m)).decode()
        assert not MediaStore.expected(m, {**record, "round_epoch": m.epoch + 1})
        assert not MediaStore.expected(m, {**record, "route_name": "some-new-route"})
        with pytest.raises(ValueError):
            right.receive(m, record, base64.b64encode(b"bad").decode())
        # URLs are never downloaded, and files outside configured roots are ignored.
        left.enqueue(identity, "http://127.0.0.1:1/private.jpg")
        left.enqueue(identity, str(tmp_path / "outside.jpg"))
        time.sleep(0.05)
        left.tick()
        assert len(left.records) == 1
    finally:
        left.close()
        right.close()


def test_media_manifest_omits_empty_rounds_and_current_history_duplicate():
    current = SimpleNamespace(epoch=3, mission={"mission_id": "demo"})
    history = {2: SimpleNamespace(epoch=2), 3: current}
    records = {2: [], 3: []}
    sent = []
    evidence = SimpleNamespace(
        core=SimpleNamespace(model=current), history=history,
        media=SimpleNamespace(tick=lambda: None, manifest=lambda model: records[model.epoch]),
        last_manifest=0, last_media=0, media_serving=[], media_manifests={},
        wire=lambda *value: sent.append(value),
    )
    MissionEvidence.replicate_media(evidence, 10)
    assert not sent
    records[3] = [{"id": "photo"}]
    MissionEvidence.replicate_media(evidence, 16)
    assert sent == [("*", "media_manifest", {"records": [{"id": "photo"}]})]
