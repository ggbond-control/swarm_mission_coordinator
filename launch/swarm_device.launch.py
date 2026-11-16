"""New-stack launch only. Does not start TaskHub, navigation or legacy packages."""

from pathlib import Path

import yaml
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction, SetEnvironmentVariable
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node
from swarm_protocol.contracts import identifier


def start(context):
    path = LaunchConfiguration("config").perform(context)
    with open(path) as stream:
        config = yaml.safe_load(stream)
    robot = identifier(config["robot_id"], "robot_id")
    root = Path(config.get("data_root", "~/.ros/swarm")).expanduser() / robot
    nodes = [SetEnvironmentVariable("ROS_LOG_DIR", str(root / "ros_logs"))]
    mapping = [
        ("swarm_consensus", "consensus"),
        ("swarm_mission_coordinator", "coordinator"),
        ("swarm_mqtt_transport", "transport"),
        ("swarm_mission_planner", "planner"),
        ("swarm_execution_agent", "agent"),
    ]
    for package, section in mapping:
        params = dict(config.get(section, {}))
        if section != "planner":
            params["robot_id"] = robot
            params["data_root"] = str(root / section)
        if section == "coordinator":
            params["agent_data_root"] = str(root / "agent")
        if section in {"transport", "coordinator"}:
            params["cluster_id"] = config.get("cluster_id", "swarm-local")
        nodes.append(
            Node(
                package=package,
                executable=package + "_node",
                output="screen",
                parameters=[params],
            )
        )
    return nodes


def generate_launch_description():
    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "config", description="Explicit standalone swarm device YAML"
            ),
            OpaqueFunction(function=start),
        ]
    )
