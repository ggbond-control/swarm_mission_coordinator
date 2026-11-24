"""Platform projections from committed facts. No control decisions live here."""

import copy
import time


def message(model, term, stage, *, method="mission.events", index=None, **fields):
    index = model.last_index if index is None else index
    return {
        "bid": model.context.get("bid", model.mission["mission_id"]),
        "tid": model.context.get("tid", ""),
        "timestamp": int(time.time() * 1000),
        "method": method,
        "type": "info",
        "data": {
            **model.summary(),
            "stage": stage,
            "term": term,
            "round_id": str(model.epoch),
            "request_id": f"sw{model.epoch}-r{model.revision}",
            "event_id": f"{model.epoch}:{index}:{stage}",
            **fields,
        },
    }


def snapshot_events(model, term, *, after=0):
    """Replay stable identities after takeover; frontend must deduplicate event_id."""
    out = []
    for event in model.events:
        if event["index"] <= after:
            continue
        kind = event["kind"]
        fields = {"message": event["message"], "plan_revision": event["revision"]}
        stage = event["state"].lower()
        method = "mission.events"
        if kind == "MISSION":
            stage, method = "accepted", "goal.events"
        elif kind == "ACTIVATE":
            stage = "planning"
        elif kind == "ADD":
            stage = "for_platform_tasks_added"
            if "task_ids" in event:
                fields["task_ids"] = event["task_ids"]
        elif kind == "PAUSE":
            stage = (
                "for_platform_skipped_tasks" if event.get("fallback") else "pause_task"
            )
            if event.get("fallback"):
                fields["skipped_tasks"] = [
                    {"task_id": task_id} for task_id in event["skipped_tasks"]
                ]
        elif kind == "EXCLUDE":
            stage = "device_isolated"
        elif kind == "FINISH":
            continue  # final pair below has full authoritative summary
        elif kind not in {"PLAN", "START"}:
            continue
        if kind == "PLAN":
            stage = "for_platform_task_plan"
            fields["routes"] = copy.deepcopy(event["routes"])
            fields["message"] = "完整计划已提交；启动与执行结果另行确认"
        out.append(
            message(model, term, stage, method=method, index=event["index"], **fields)
        )
    if model.state in {"COMPLETED", "FAILED", "CANCELLED"} and model.terminal_index > after:
        for stage in ("for_platform_mission_finished", model.state):
            out.append(message(model, term, stage, index=model.terminal_index))
    return out
