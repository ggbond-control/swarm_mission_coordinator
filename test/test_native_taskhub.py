"""Real C++ TaskHub and new swarm nodes; local mock navigation only."""

import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SWARM_RUN_ROS") != "1",
    reason="explicit isolated ROS integration opt-in",
)


def wait(predicate, seconds=40):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError("local native TaskHub integration timed out")


@pytest.mark.parametrize("failures", [0, 2, 100])
def test_native_taskhub_navigation_skip_and_single_fallback(tmp_path, failures):
    from ament_index_python.packages import get_package_prefix
    from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus
    from nav2_msgs.action import NavigateToPose
    from nav_msgs.msg import Odometry
    from rcl_interfaces.msg import SetParametersResult
    from rcl_interfaces.srv import SetParameters
    from rclpy.action import ActionClient, ActionServer
    from rclpy.callback_groups import ReentrantCallbackGroup
    from rclpy.context import Context
    from rclpy.executors import MultiThreadedExecutor
    from rclpy.node import Node
    from std_msgs.msg import String
    from std_srvs.srv import Trigger
    from support import mission

    from inspection_interfaces.action import PlanMission
    from inspection_interfaces.srv import ValidateRouteWaypoints

    context = Context()
    context.init(domain_id=187)
    node = Node("swarm_native_navigation_fixture", context=context)
    group = ReentrantCallbackGroup()
    navigation = []
    states = []

    def navigate(goal):
        navigation.append(goal.request.pose.pose.position.x)
        if len(navigation) <= failures:
            goal.abort()
        else:
            goal.succeed()
        return NavigateToPose.Result()

    nav = ActionServer(
        node,
        NavigateToPose,
        "multi_map_navigate_to_pose",
        navigate,
        callback_group=group,
    )

    def valid(request, response):
        response.success = True
        return response

    # ValidateRouteWaypoints has a dedicated success field in the shared interface.
    services = [
        node.create_service(
            ValidateRouteWaypoints,
            "/validate_route_waypoints",
            valid,
            callback_group=group,
        )
    ]

    def trigger(_request, response):
        response.success = True
        response.message = "local mock stopped"
        return response

    for name in (
        "/monitor/acoustic/stop",
        "/monitor/gas/stop",
        "/monitor/thermal_camera/stop",
        "/monitor/smoke/stop",
    ):
        services.append(
            node.create_service(Trigger, name, trigger, callback_group=group)
        )

    def configured(request, response):
        response.results = [
            SetParametersResult(successful=True) for _ in request.parameters
        ]
        return response

    for name in (
        "/alarm_aggregator_node/set_parameters",
        "/light_manager/set_parameters",
        "/next_goal_policy",
    ):
        services.append(
            node.create_service(SetParameters, name, configured, callback_group=group)
        )
    pose = node.create_publisher(Odometry, "/odometry_multi_maps", 10)
    health = node.create_publisher(
        DiagnosticArray, "/inspection_task_hub/heartbeat/navigation", 10
    )

    def pulse():
        value = Odometry()
        value.header.frame_id = "map_000"
        value.header.stamp = node.get_clock().now().to_msg()
        value.pose.pose.position.x = 2.0
        value.pose.pose.position.y = 2.0
        value.pose.pose.orientation.w = 1.0
        pose.publish(value)
        health.publish(
            DiagnosticArray(
                status=[
                    DiagnosticStatus(
                        level=DiagnosticStatus.OK,
                        name="local mocked navigation",
                        message="OK",
                    )
                ]
            )
        )

    node.create_timer(0.1, pulse, callback_group=group)
    node.create_subscription(
        String,
        "/swarm/mission/state",
        lambda msg: states.append(json.loads(msg.data)),
        10,
    )
    executor = MultiThreadedExecutor(num_threads=4, context=context)
    executor.add_node(node)
    thread = threading.Thread(target=executor.spin, daemon=True)
    thread.start()
    env = {
        **os.environ,
        "ROS_DOMAIN_ID": "187",
        "ROS_AUTOMATIC_DISCOVERY_RANGE": "LOCALHOST",
        "ROS_LOG_DIR": str(tmp_path / "ros_logs"),
    }
    specs = [
        (
            "hub",
            str(
                Path(get_package_prefix("inspection_task_hub"))
                / "lib/inspection_task_hub/task_hub_node"
            ),
            {
                "runtime_log_directory": str(tmp_path / "hub"),
                "inspection_report_enabled": False,
                "energy_usage_recording_enabled": False,
            },
        ),
        (
            "consensus",
            "swarm_consensus",
            {
                "robot_id": "R0",
                "election_timeout": 2.0,
                "quorum_timeout": 1.0,
                "heartbeat_period": 0.2,
            },
        ),
        ("planner", "swarm_mission_planner", {"footprint_radius": 0.1}),
        (
            "agent",
            "swarm_execution_agent",
            {"robot_id": "R0", "allow_motion": True, "task_hub_prefix": ""},
        ),
        (
            "coordinator",
            "swarm_mission_coordinator",
            {"robot_id": "R0", "footprint_radius": 0.1, "phase_timeout": 20.0},
        ),
    ]
    processes = []
    logs = []
    client = ActionClient(
        node, PlanMission, "/swarm/mission/plan", callback_group=group
    )
    try:
        for name, package, params in specs:
            if name in {"agent", "consensus", "coordinator"}:
                params["data_root"] = str(tmp_path / name)
            args = (
                [package]
                if name == "hub"
                else ["ros2", "run", package, package + "_node"]
            )
            args += ["--ros-args"]
            for k, v in params.items():
                args += ["-p", k + ":=" + json.dumps(v)]
            log = (tmp_path / (name + ".log")).open("w")
            logs.append(log)
            processes.append(
                subprocess.Popen(
                    args,
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            )
        assert client.wait_for_server(timeout_sec=20)
        value = mission(tmp_path / "maps", ["R0"], 1, 1)
        value.pop("mission_id")
        value["tasks"][0]["location"]["pos"] = [3.25, 3.25, 0.0]
        path = tmp_path / "input.json"
        path.write_text(json.dumps(value))
        future = client.send_goal_async(
            PlanMission.Goal(mission_id="native-local", mission_json_path=str(path))
        )
        wait(future.done)
        assert future.result().accepted
        result = future.result().get_result_async()
        wait(result.done, 65)
        assert result.result().result.planned, result.result().result.message
        state = wait(
            lambda: next(
                (s for s in reversed(states) if s["state"] in {"COMPLETED", "FAILED"}),
                None,
            ),
            65,
        )
        assert state["state"] == ("FAILED" if failures == 100 else "COMPLETED"), state
        assert state["retry_performed"] == bool(failures)
        assert state["plan_revision"] == (2 if failures else 1)
        assert state["completed_tasks"] == (0 if failures == 100 else 1)
        assert len(navigation) == (4 if failures == 100 else 3 if failures else 1)
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGINT)
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)
        client.destroy()
        nav.destroy()
        executor.shutdown(timeout_sec=5)
        thread.join(timeout=5)
        node.destroy_node()
        context.shutdown()
        for log in logs:
            log.close()
