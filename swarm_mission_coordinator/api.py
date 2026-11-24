"""Nonblocking local Actions. Every decision still crosses the consensus log."""

import time
from functools import partial
from pathlib import Path

from rclpy.action import ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.task import Future
from swarm_protocol.contracts import (
    MAX_MESSAGE_BYTES,
    TERMINAL,
    decode,
    normalize_mission,
)
from swarm_protocol.storage import atomic_json

from inspection_interfaces.action import PlanMission


class MissionActions:
    def __init__(self, node):
        self.node = node
        self.reserved = False
        self.waiter = None
        self.servers = []
        group = ReentrantCallbackGroup()
        for name in ("plan", "replan"):
            self.servers.append(
                ActionServer(
                    node,
                    PlanMission,
                    "/swarm/mission/" + name,
                    execute_callback=partial(self.execute, kind=name),
                    goal_callback=self.goal,
                    cancel_callback=lambda _: CancelResponse.ACCEPT,
                    callback_group=group,
                )
            )

    def goal(self, request):
        if (
            self.reserved
            or not request.mission_id
            or not Path(request.mission_json_path).is_absolute()
        ):
            return GoalResponse.REJECT
        self.reserved = True
        return GoalResponse.ACCEPT

    async def execute(self, goal, kind):
        result = PlanMission.Result(mission_id=goal.request.mission_id)
        try:
            path = Path(goal.request.mission_json_path)
            if path.stat().st_size > MAX_MESSAGE_BYTES:
                raise ValueError("mission file exceeds size limit")
            data = decode(path.read_text())
            if "mission_id" in data:
                raise ValueError("mission_id belongs in Action goal, not template")
            mission = normalize_mission(data, goal.request.mission_id)
            if self.node.robot not in mission["robot_ids"]:
                raise ValueError("local robot is not a participant")
            if kind == "replan":
                current = self.node.core.model
                if current.mission.get("mission_id") != mission["mission_id"]:
                    raise ValueError("addition mission_id differs from current round")
                revision = current.revision + 1
            else:
                revision = 1
            envelope = {
                "bid": mission["mission_id"],
                "tid": "ros-" + str(goal.goal_id.uuid),
                "method": "task.execute" if kind == "plan" else "task.add",
                "data": {k: v for k, v in mission.items() if k != "mission_id"},
            }
            # Relay only to the explicitly named participants, never an open broadcast.
            self.node.ingress.accept_request(envelope, relay=True)
            result.accepted = True
            future = Future()
            self.waiter = {
                "future": future,
                "goal": goal,
                "result": result,
                "epoch": mission["round_epoch"],
                "revision": revision,
                "deadline": time.monotonic()
                + 2 * self.node.core.phase_timeout
                + self.node.core.planning_timeout
                + 10,
                "feedback": None,
            }
            return await future
        except Exception as error:
            result.message = str(error)
            goal.abort()
            return result
        finally:
            self.reserved = False
            self.waiter = None

    def tick(self):
        item = self.waiter
        if not item:
            return
        goal, result = item["goal"], item["result"]
        model = self.node.core.model
        if goal.is_cancel_requested:
            self.node.ingress.request_control("task.stop")
            result.message = "停止请求已提交；全局停止须等待多数派与本机停止证据"
            goal.canceled()
        elif (
            model.epoch == item["epoch"]
            and model.revision >= item["revision"]
            and (
                model.state in TERMINAL
                or (
                    model.state == "ACTIVE"
                    and model.executable.issubset(model.start_acks)
                )
            )
        ):
            result.request_id = f"sw{model.epoch}-r{model.revision}"
            result.plan_revision = model.revision
            result.planned = bool(model.plan_digest) and (
                model.state == "ACTIVE" or model.state == "COMPLETED"
            )
            result.message = model.reason or "计划与启动确认均已提交；任务执行尚未结束"
            if result.planned:
                path = (
                    self.node.root / f"round-{model.epoch}-r{model.revision}-plan.json"
                )
                atomic_json(
                    path,
                    {
                        "routes": model.assignments,
                        "unassignable": model.unassignable,
                        "plan_digest": model.plan_digest,
                    },
                )
                result.task_plan_json_path = str(path)
                goal.succeed()
            else:
                goal.abort()
        elif time.monotonic() >= item["deadline"]:
            result.message = "等待已提交规划结果超时；此返回不改变全局任务状态，请查询 /swarm/mission/state"
            goal.abort()
        else:
            stage = (
                model.state.lower()
                if model.epoch == item["epoch"]
                else "awaiting_consensus"
            )
            if item["feedback"] != stage:
                goal.publish_feedback(
                    PlanMission.Feedback(
                        mission_id=goal.request.mission_id,
                        request_id=f"sw{item['epoch']}-r{item['revision']}",
                        stage=stage,
                        message=model.reason,
                    )
                )
                item["feedback"] = stage
            return
        item["future"].set_result(result)

    def close(self):
        if self.waiter and not self.waiter["future"].done():
            self.waiter["result"].message = "本地协调器关闭；请从其他副本查询全局任务"
            self.waiter["future"].set_result(self.waiter["result"])
        for server in self.servers:
            server.destroy()
