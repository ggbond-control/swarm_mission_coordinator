import copy

import pytest
from support import MissionCluster, mission
from swarm_execution_agent.execution import Execution
from swarm_protocol.contracts import decode, digest, normalize_mission


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
    c.finish_routes(skipped=set() if recovered else {"p1"})
    c.run(1)
    assert c.leader().model.state == ("COMPLETED" if recovered else "FAILED")
    assert c.leader().model.revision == 2


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


@pytest.mark.parametrize(
    "field,value",
    [
        ("robot_ids", []),
        ("robot_ids", ["A", "A"]),
        ("robot_ids", [" A"]),
        ("robot_ids", ["a b"]),
        ("round_epoch", 0),
        ("round_epoch", True),
        ("map_package", "relative"),
        ("tasks", None),
    ],
)
def test_invalid_input_rejected(tmp_path, field, value):
    m = mission(tmp_path)
    m[field] = value
    with pytest.raises(ValueError):
        normalize_mission(m, "demo")


def test_case_sensitive_and_duplicate_json():
    with pytest.raises(ValueError):
        decode('{"a":1,"a":2}')
    assert normalize_mission(
        {
            "round_epoch": 1,
            "robot_ids": ["A", "a"],
            "map_package": "/maps",
            "tasks": [],
        },
        "m",
    )["robot_ids"] == ["A", "a"]


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


def test_execution_rejects_old_snapshot_and_late_permit(tmp_path):
    m = mission(tmp_path, ["R0"], 1)
    route = {
        "robot_id": "R0",
        "task_ids": ["p0"],
        "checkpoints": [
            {"type": "task", "task_ids": ["p0"], "location": m["tasks"][0]["location"]}
        ],
    }
    a = Execution("R0", tmp_path / "agent")
    a.prepare(
        {
            "session_id": a.session,
            "round_epoch": 1,
            "revision": 1,
            "route_name": "route1",
            "plan_digest": "d",
            "route": route,
            "mission": m,
        }
    )
    request = a.request_permit(1)
    assert not a.grant(request, 12)
    request = a.request_permit(13)
    assert a.grant(request, 14)
    assert a.permit_until == 23
    assert a.begin_start(14)
    assert a.started(a.generation, True) == "started"
    snapshot = {
        "protocol_version": 1,
        "route_name": "route1",
        "execution_id": "e",
        "snapshot_sequence": 1,
        "route_state": "COMPLETED",
        "finalized": True,
        "reached_waypoint_ids": [1],
        "skipped_waypoints": [],
        "pending_waypoint_ids": [],
    }
    assert a.observe(snapshot)
    assert not a.observe(snapshot)
    with pytest.raises(ValueError):
        a.observe({**snapshot, "execution_id": "old"})
    assert a.outbox["result"]["result"]["reached"] == ["p0"]
    assert not a.needs_pause(24)  # already finalized; no obsolete pause
    assert a.request_permit(24) is None


def test_start_late_success_requires_compensation(tmp_path):
    a = Execution("R0", tmp_path)
    m = mission(tmp_path / "maps", ["R0"], 0)
    a.prepare(
        {
            "session_id": a.session,
            "round_epoch": 1,
            "revision": 1,
            "route_name": "route1",
            "plan_digest": "d",
            "route": {"checkpoints": [], "task_ids": []},
            "mission": m,
        }
    )
    request = a.request_permit(0)
    a.grant(request, 1)
    a.begin_start(1)
    generation = a.generation
    assert a.needs_pause(11)
    assert a.started(generation, True) == "cancel"


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
    from swarm_mission_coordinator.platform import snapshot_events

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
