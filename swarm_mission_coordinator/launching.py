"""ROS launch actions from an already merged device configuration."""

from pathlib import Path

from launch.actions import GroupAction, SetEnvironmentVariable
from launch_ros.actions import Node
from swarm_protocol.contracts import identifier
from swarm_protocol.topics import DEFAULT_CLUSTER_ID


def device_nodes(config):
    """Build the five device-local nodes; Bringup owns configuration loading."""
    robot = identifier(config["robot_id"], "robot_id")
    data_root = config.get("data_root")
    root = Path(data_root).expanduser() if data_root else Path.home() / ".ros/swarm" / robot / "runtime"
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
            params["report_root"] = str(root.parent / "reports")
            params["runtime_info_path"] = str(root / "runtime-info.json")
        if section in {"transport", "coordinator"}:
            params["cluster_id"] = config.get("cluster_id", DEFAULT_CLUSTER_ID)
        nodes.append(
            Node(
                package=package,
                executable=package + "_node",
                output="both",
                parameters=[params],
            )
        )
    return [GroupAction(actions=nodes, scoped=True)]
