import copy

import pytest
from support import MissionCluster
from swarm_mission_coordinator.platform import snapshot_events
from swarm_protocol.contracts import digest


@pytest.mark.parametrize("count", [1, 2, 3, 4, 5])
def test_whole_mission_completes_from_navigation_evidence(tmp_path, count):
    c = MissionCluster(tmp_path, count=count, tasks=count)
    c.run(4)
    for _ in range(count + 2):
        c.finish_routes()
        c.run(0.8)
    assert c.leader().model.state == "COMPLETED"
    assert not c.leader().model.unfinished
    c.run(0.2)
    assert len({digest(v.model.summary()) for v in c.cores.values()}) == 1


@pytest.mark.parametrize("recovered", [True, False])
def test_round_wide_skip_retry_exactly_once(tmp_path, recovered):
    c = MissionCluster(tmp_path, count=1, tasks=2)
    c.run(3)
    c.finish_routes(skipped={"p1"})
    c.run(2)
    assert c.leader().model.revision == 2
    assert c.leader().model.skip_retry_used
    assert c.leader().model.assignments["R0"]["task_ids"] == ["p1"]
    skipped = next(
        event["data"] for event in snapshot_events(c.leader().model, term=1)
        if event["data"]["stage"] == "for_platform_skipped_tasks"
    )
    assert skipped["skipped_tasks"] == [{"task_id": "p1"}]
    c.finish_routes(skipped=set() if recovered else {"p1"})
    c.run(1)
    assert c.leader().model.state == ("COMPLETED" if recovered else "FAILED")
    assert c.leader().model.revision == 2
    assert c.leader().model.summary()["completed_task_ids"] == (["p0", "p1"] if recovered else ["p0"])


def test_leader_crash_preserves_arrivals_and_freezes_unconfirmed_stop(tmp_path):
    c = MissionCluster(tmp_path, count=5, tasks=5)
    c.run(4)
    old = c.net.leader().robot
    model = c.leader().model
    remaining = next(r for r in model.started if r != old)
    task = model.assignments[remaining]["task_ids"][0]
    tasks = model.assignments[remaining]["task_ids"]
    c.event(
        remaining,
        "result",
        result={
            "execution_id": "run",
            "sequence": 1,
            "state": "RUNNING",
            "finalized": False,
            "reached": [task],
            "skipped": {},
            "pending": [t for t in tasks if t != task],
        },
    )
    c.run(0.5)
    c.net.crashed.add(old)
    c.run(5)
    model = c.leader().model
    assert old in model.excluded
    assert model.tasks[task] == "COMPLETED"
    assert all(task not in route["task_ids"] for route in model.assignments.values())
    assert model.revision >= 2


def test_addition_includes_skipped_without_using_final_retry(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=2)
    c.run(3)
    c.event(
        "R0",
        "result",
        result={
            "execution_id": "run",
            "sequence": 1,
            "state": "RUNNING",
            "finalized": False,
            "reached": [],
            "skipped": {"p0": "blocked"},
            "pending": ["p1"],
        },
    )
    c.run(0.3)
    extra = copy.deepcopy(c.mission["tasks"][0])
    extra["id"] = "new"
    c.leader().additions.append([extra])
    c.run(2)
    model = c.leader().model
    assert model.revision == 2
    assert set(model.assignments["R0"]["task_ids"]) == {"p0", "p1", "new"}
    assert not model.skip_retry_used
    added = next(
        event["data"] for event in snapshot_events(model, term=1)
        if event["data"]["stage"] == "for_platform_tasks_added"
    )
    assert added["task_ids"] == ["new"]


def test_no_quorum_cannot_publish_false_terminal(tmp_path):
    c = MissionCluster(tmp_path, count=5, tasks=5)
    c.run(4)
    old = c.net.leader().robot
    c.net.crashed.update(r for r in c.net.ids if r != old)
    c.run(3)
    assert not c.cores[old].authority()
    assert c.cores[old].model.state not in {"COMPLETED", "FAILED", "CANCELLED"}


def test_stop_and_terminal_are_idempotent(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=2)
    c.run(3)
    c.leader().controls.append("stop")
    c.run(2)
    assert c.leader().model.state == "CANCELLED"
    final = c.leader().model.terminal_index
    c.leader().controls.append("reset")
    c.run(0.5)
    assert c.leader().model.state == "CANCELLED"
    assert c.leader().model.terminal_index == final


def test_result_cannot_be_inferred_from_progress(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=1)
    c.run(3)
    m = c.leader().model
    c.event(
        "R0",
        "result",
        result={
            "execution_id": "run",
            "sequence": 1,
            "state": "COMPLETED",
            "finalized": True,
            "reached": [],
            "skipped": {},
            "pending": [],
        },
    )
    c.run(0.3)
    assert m.tasks["p0"] != "COMPLETED"
    assert c.leader().model.state == "ACTIVE"


def test_sealed_routes_satisfy_pause_without_extra_stop_rpc(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=1)
    c.run(3)
    c.finish_routes(skipped={"p0"})
    # No synthetic stop ACKs: finalized route evidence itself is sufficient.
    c.run(2, auto_prepare=False)
    assert c.leader().model.revision == 2
    assert c.leader().model.skip_retry_used


def test_stale_error_from_old_revision_cannot_fail_current_round(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=1)
    c.run(3)
    event = c.event("R0", "error", message="late error")
    event["revision"] = 0
    c.leader().events[event["event_id"]] = event
    c.run(0.4)
    assert c.leader().model.state == "ACTIVE"


def test_stop_ack_contains_arrival_evidence_before_replan(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=2)
    c.run(3)
    extra = copy.deepcopy(c.mission["tasks"][0])
    extra["id"] = "new"
    c.leader().additions.append([extra])
    c.event(
        "R0",
        "stopped",
        success=True,
        navigation_result={
            "execution_id": "run",
            "sequence": 2,
            "state": "PAUSED",
            "finalized": False,
            "reached": ["p0"],
            "skipped": {},
            "pending": ["p1"],
        },
    )
    c.run(2)
    assert c.leader().model.tasks["p0"] == "COMPLETED"
    assert "p0" not in c.leader().model.assignments["R0"]["task_ids"]


def test_unknown_navigation_stays_unknown_after_successful_stop(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=1)
    c.run(3)
    c.event(
        "R0",
        "result",
        result={
            "execution_id": "run",
            "sequence": 2,
            "state": "UNKNOWN",
            "finalized": True,
            "reached": [],
            "skipped": {},
            "pending": ["p0"],
            "reason": "snapshot missing",
        },
    )
    c.run(0.1)
    c.event("R0", "stopped", success=True)
    c.run(0.4)
    assert c.leader().model.state == "FAILED"
    assert "p0" in c.leader().model.unknown


def test_permit_stop_wins_over_offline_reallocation(tmp_path):
    c = MissionCluster(tmp_path, count=3, tasks=3)
    c.run(4)
    m = c.leader().model
    robot = next(iter(m.started))
    # Offline handling may already have initiated PAUSE before stop evidence
    # arrives. A local safety stop must still forbid replacement starts.
    c.leader()._command(
        "PAUSE",
        1000 + c.net.now,
        reason="设备掉线触发重分配",
        deadline=1010 + c.net.now,
    )
    c.run(0.2, auto_prepare=False)
    c.event(robot, "stopped", success=True, cause="permit_expired")
    c.run(1, auto_prepare=False)
    assert c.leader().model.state == "FAILED"
    assert c.leader().model.revision == 1


def test_platform_events_stable_and_do_not_include_removed_categories(tmp_path):
    c = MissionCluster(tmp_path, count=1, tasks=1)
    c.run(3)
    c.finish_routes()
    c.run(1)
    m = c.leader().model
    first = snapshot_events(m, 1)
    second = snapshot_events(m, 2)
    assert [v["data"]["event_id"] for v in first] == [
        v["data"]["event_id"] for v in second
    ]
    stages = {v["data"]["stage"] for v in first}
    assert {
        "for_platform_mission_finished",
        "COMPLETED",
        "for_platform_task_plan",
    } <= stages
    assert (
        not {"for_platform_reallocation_tasks", "for_platform_unassignable_tasks"}
        & stages
    )
    assert all(v["bid"] == "demo" and v["tid"] == "t" for v in first)


def test_platform_history_replays_on_takeover_and_at_bounded_interval(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from swarm_mission_coordinator.node import CoordinatorNode

    cluster = MissionCluster(tmp_path, count=1, tasks=1)
    cluster.run(3)
    model = cluster.leader().model
    sent = []
    node = SimpleNamespace(
        core=SimpleNamespace(consensus={"term": 1}), request_seen=set(),
        platform_cursor=(0, 0, 0), last_platform_replay=-float("inf"),
        emit_platform=lambda channel, payload: sent.append((channel, payload)),
    )
    clock = [100.0]
    monkeypatch.setattr("swarm_mission_coordinator.node.time.monotonic", lambda: clock[0])
    CoordinatorNode.platform_progress(node, model)
    history = [event for channel, event in sent if channel == "events"]
    assert history
    sent.clear()
    clock[0] += 2
    CoordinatorNode.platform_progress(node, model)
    assert [channel for channel, _ in sent] == ["state"]
    cluster.finish_routes()
    cluster.run(1)
    sent.clear()
    CoordinatorNode.platform_progress(node, cluster.leader().model)
    assert {"for_platform_mission_finished", "COMPLETED"} <= {
        event["data"]["stage"] for channel, event in sent if channel == "events"
    }
    for takeover in (False, True):
        sent.clear()
        if takeover:
            node.core.consensus["term"] = 2
        else:
            clock[0] += 30
        CoordinatorNode.platform_progress(node, cluster.leader().model)
        assert history[0]["data"]["event_id"] in {
            event["data"]["event_id"] for channel, event in sent if channel == "events"
        }


def test_dependency_needs_evidence_of_final_parking_waypoint():
    from swarm_mission_coordinator.model import MissionState

    model = MissionState()
    model.assignments = {
        "A": {"task_ids": ["earlier", "parking"], "dependencies": []},
        "B": {"task_ids": ["next"], "dependencies": ["A"]},
    }
    model.results = {
        "A": {"finalized": True, "state": "COMPLETED", "reached": ["earlier"],
              "skipped": {"parking": "navigation timeout"}}
    }
    assert list(model.dependency_blockers("B")) == ["A"]
    assert not model.dependencies_finished("B")
    model.results["A"]["reached"].append("parking")
    assert model.dependencies_finished("B")
