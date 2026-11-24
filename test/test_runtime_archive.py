"""Exercise offline migration and evidence collection on recovery artifacts."""

import json
import tarfile

import pytest
from swarm_mission_coordinator.evidence import export
from swarm_mission_coordinator.runtime_migration import migrate
from swarm_protocol.storage import DurableFile, atomic_json


def test_migrate_preserves_term_execution_and_private_upload(tmp_path):
    root = tmp_path / "runtime"
    old_agent = tmp_path / "legacy/agent"
    old_raft = tmp_path / "legacy/consensus"
    state = DurableFile(old_raft / "raft.json")
    state.save({"term": 8, "vote": "R1"})
    route = old_agent / "routes/route.yaml"
    route.parent.mkdir(parents=True)
    route.write_text("name: route")
    atomic_json(old_agent / "execution.json", {"yaml_path": str(route), "session_id": "old"})
    round_dir = root / "reports/round-101"
    round_dir.mkdir(parents=True)
    (round_dir / "mission.html").write_text("<html>report</html>")
    job_body = round_dir / "upload-digest.html"
    job_body.write_text("<html>retry body</html>")
    atomic_json(round_dir / "job.json", {"path": str(job_body), "status": "uploading", "attempts": 2})
    atomic_json(round_dir / "credentials.json", {"secretKey": "private"})
    sources = {"coordinator": root, "agent": old_agent, "consensus": old_raft}

    assert migrate(sources, root) is None
    assert job_body.is_file() and not (tmp_path / "reports").exists()
    manifest = json.loads(migrate(sources, root, apply=True).read_text())
    assert (tmp_path / "reports/round-101/mission.html").is_file()
    assert (root / "agent/routes/route.yaml").read_text() == route.read_text()
    record = json.loads((root / "agent/execution.json").read_text())
    assert record == {"yaml_path": str(root / "agent/routes/route.yaml"), "session_id": "old"}
    private = root / "coordinator/private_uploads/round-101"
    assert (private / "credentials.json").stat().st_mode & 0o777 == 0o600
    job = json.loads((private / "job.json").read_text())
    assert job == {"path": str(private / job_body.name), "status": "uploading", "attempts": 2}
    assert DurableFile(root / "consensus/raft.json").load() == state.load()
    assert (tmp_path / manifest["backup"] / "reports/round-101/mission.html").is_file()
    with pytest.raises(ValueError, match="已有内容"):
        migrate(sources, root, apply=True)


def test_export_round_excludes_credentials_and_live_recovery(tmp_path):
    runtime = tmp_path / "runtime"
    report_root = tmp_path / "reports"
    events = runtime / "coordinator/audit/round-101/r001/events.jsonl"
    events.parent.mkdir(parents=True)
    events.write_text(json.dumps({"time": 1, "payload": {"command": {"map_digest": "abc"},
                                                       "storage": {"accessKey": "private"}}}) + "\n")
    atomic_json(runtime / "runtime-info.json", {"robot_id": "R1"})
    atomic_json(runtime / "coordinator/private_uploads/round-101/credentials.json", {"a": "private"})
    atomic_json(runtime / "agent/round-101/execution.json", {"live": True})
    report = report_root / "round-101/demo.html"
    report.parent.mkdir(parents=True)
    report.write_text("<html>demo</html>")
    archive = tmp_path / "evidence.tar.gz"
    manifest = export(runtime, archive, 101, report_root)
    assert manifest["robot_id"] == "R1" and manifest["map_digests"] == ["abc"]
    assert manifest["time_range"] == [1, 1] and not manifest["missing"]
    with tarfile.open(archive) as saved:
        assert set(saved.getnames()) == {
            "manifest.json", "reports/round-101/demo.html",
            "runtime/runtime-info.json", "runtime/coordinator/audit/round-101/r001/events.jsonl",
        }
        assert b"private" not in saved.extractfile("runtime/" + str(events.relative_to(runtime))).read()
