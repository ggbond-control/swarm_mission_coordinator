"""Opt-in real ROS processes + loopback Mosquitto; never contacts a robot.

SWARM_RUN_ROS=1 and SWARM_BROKER point at a local broker executable. Run from
the workspace environment after building all six new packages and interfaces.
"""

from contextlib import contextmanager
import json
import os
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("SWARM_RUN_ROS") != "1",
    reason="explicit isolated ROS/MQTT integration opt-in",
)


def wait_for(predicate, timeout=45, message="condition"):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError("Timed out: " + message)


class Device:
    def __init__(self, root, index, broker_port, *, domain_base, offline_timeout):
        from rclpy.context import Context
        from rclpy.executors import SingleThreadedExecutor
        from rclpy.node import Node
        from std_msgs.msg import String

        self.robot = f"R{index}"
        self.root = root / self.robot
        self.root.mkdir()
        self.domain = domain_base + index
        self.processes = {}
        self.logs = []
        self.environment = {
            **os.environ,
            "ROS_DOMAIN_ID": str(self.domain),
            "ROS_AUTOMATIC_DISCOVERY_RANGE": "LOCALHOST",
            "ROS_LOCALHOST_ONLY": "1",
            "ROS_LOG_DIR": str(self.root / "ros_logs"),
            "PYTHONDONTWRITEBYTECODE": "1",
        }
        specs = {
            "consensus": (
                "swarm_consensus",
                "swarm_consensus_node",
                {
                    "robot_id": self.robot,
                    "election_timeout": 2.0,
                    "quorum_timeout": 1.2,
                    "heartbeat_period": 0.2,
                },
            ),
            "planner": (
                "swarm_mission_planner",
                "swarm_mission_planner_node",
                {"footprint_radius": 0.1},
            ),
            "agent": (
                "swarm_execution_agent",
                "swarm_execution_agent_node",
                {
                    "robot_id": self.robot,
                    "task_hub_prefix": "/swarm/mock",
                    "nominal_speed": 1.0,
                },
            ),
            "transport": (
                "swarm_mqtt_transport",
                "swarm_mqtt_transport_node",
                {
                    "robot_id": self.robot,
                    "cluster_id": "integration",
                    "mqtt_port": broker_port,
                },
            ),
            "coordinator": (
                "swarm_mission_coordinator",
                "swarm_mission_coordinator_node",
                {
                    "robot_id": self.robot,
                    "cluster_id": "integration",
                    "media_roots": [str(self.root / "photos")],
                    "agent_data_root": str(self.root / "agent"),
                    "footprint_radius": 0.1,
                    "phase_timeout": 15.0,
                    "planning_timeout": 30.0,
                    "offline_timeout": offline_timeout,
                    "stale_timeout": 3.0,
                },
            ),
            "mock": (
                "swarm_execution_agent",
                "swarm_mock_taskhub",
                {"x": 2.0 + index * 8, "y": 2.0 + index * 8},
            ),
        }
        for name, (package, executable, parameters) in specs.items():
            if name in {"agent", "consensus", "transport", "coordinator"}:
                parameters["data_root"] = str(self.root / name)
            args = ["ros2", "run", package, executable, "--ros-args"]
            for key, value in parameters.items():
                args.extend(["-p", key + ":=" + json.dumps(value)])
            log = (self.root / (name + ".log")).open("w")
            self.logs.append(log)
            self.processes[name] = subprocess.Popen(
                args,
                env=self.environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        self.context = Context()
        self.context.init(domain_id=self.domain)
        self.probe = Node("swarm_integration_probe_" + self.robot, context=self.context)
        self.state = {}
        self.audit = {}
        self.agent = {}
        self.probe.create_subscription(
            String,
            "/swarm/mission/state",
            lambda msg: setattr(self, "state", json.loads(msg.data)),
            10,
        )
        self.probe.create_subscription(
            String,
            "/swarm/mock/audit",
            lambda msg: setattr(self, "audit", json.loads(msg.data)),
            10,
        )
        self.probe.create_subscription(
            String,
            "/swarm/agent/state",
            lambda msg: setattr(self, "agent", json.loads(msg.data)),
            10,
        )
        self.inject = self.probe.create_publisher(String, "/swarm/mock/inject", 10)
        self.ingress = self.probe.create_publisher(
            String, "/swarm/platform/inbound", 10
        )
        from inspection_interfaces.msg import PlatformEvent

        self.photo = self.probe.create_publisher(
            PlatformEvent, "/platform/action_events", 10
        )
        self.executor = SingleThreadedExecutor(context=self.context)
        self.executor.add_node(self.probe)
        self.thread = threading.Thread(target=self.executor.spin, daemon=True)
        self.thread.start()

    def finish(self, skip_last=False):
        from std_msgs.msg import String

        route = self.audit.get("route")
        if not route or route["finalized"]:
            return
        all_ids = sorted(
            route["reached_waypoint_ids"]
            + route["pending_waypoint_ids"]
            + [x["waypoint_id"] for x in route["skipped_waypoints"]]
        )
        skipped = all_ids[-1:] if skip_last else []
        self.inject.publish(
            String(
                data=json.dumps(
                    {
                        "reached": [n for n in all_ids if n not in skipped],
                        "skipped": skipped,
                    }
                )
            )
        )

    def stop_process(self, name):
        process = self.processes[name]
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGINT)
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)

    def close(self):
        for name in reversed(list(self.processes)):
            self.stop_process(name)
        self.executor.shutdown(timeout_sec=3)
        self.thread.join(timeout=3)
        self.probe.destroy_node()
        self.context.shutdown()
        for log in self.logs:
            log.close()


@contextmanager
def simulated_fleet(tmp_path_factory, scenario, *, domain_base, offline_timeout=5.0):
    import paho.mqtt.client as mqtt
    from support import make_map

    base = (
        Path(os.environ["SWARM_TEST_EVIDENCE"]) / scenario
        if os.environ.get("SWARM_TEST_EVIDENCE")
        else tmp_path_factory.mktemp(scenario)
    )
    base.mkdir(parents=True, exist_ok=True)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = base / "mosquitto.conf"
    config.write_text(
        f"listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n"
    )
    log = (base / "broker.log").open("w")
    broker = subprocess.Popen(
        [os.environ["SWARM_BROKER"], "-c", str(config)],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    client = mqtt.Client(client_id="swarm-local-validation")
    messages = []
    client.on_message = lambda c, u, msg: messages.append(
        (msg.topic, json.loads(msg.payload))
    )

    def connect():
        try:
            client.connect("127.0.0.1", port)
            return True
        except OSError:
            return False

    wait_for(connect, 10, "local broker startup")
    client.subscribe("fh/#", qos=1)
    client.loop_start()
    devices = []
    try:
        for i in range(3):
            devices.append(Device(base, i, port, domain_base=domain_base,
                                  offline_timeout=offline_timeout))
        wait_for(
            lambda: all(d.agent.get("ready") for d in devices),
            45,
            "all mock TaskHubs ready",
        )
        fleet = {
            "root": base,
            "devices": devices,
            "mqtt": client,
            "messages": messages,
            "maps": make_map(base / "maps"),
            "broker": broker,
            "broker_log": log,
            "broker_config": config,
            "broker_port": port,
        }
        yield fleet
    finally:
        for device in reversed(devices):
            device.close()
        client.disconnect()
        client.loop_stop()
        active = fleet["broker"] if "fleet" in locals() else broker
        active.terminate()
        active.wait(timeout=5)
        log.close()


@pytest.fixture(scope="module")
def local_fleet(tmp_path_factory):
    with simulated_fleet(tmp_path_factory, "workflows", domain_base=181) as fleet:
        yield fleet


@pytest.fixture(scope="module")
def takeover_fleet(tmp_path_factory):
    with simulated_fleet(tmp_path_factory, "leader-takeover", domain_base=191) as fleet:
        yield fleet


@pytest.fixture
def broker_recovery_fleet(tmp_path_factory):
    # Reconnect after the local lease expires and while all voters are eligible.
    with simulated_fleet(tmp_path_factory, "broker-recovery", domain_base=201,
                         offline_timeout=30.0) as fleet:
        yield fleet


def submit(fleet, epoch, tasks=3):
    from support import mission

    value = mission(Path(fleet["maps"]), ["R0", "R1", "R2"], tasks, epoch)
    value["mission_id"] = f"local-{epoch}"
    value = {k: v for k, v in value.items() if k != "mission_id"}
    fleet["mqtt"].publish(
        "fh/swarm/dispatch/services",
        json.dumps(
            {
                "method": "task.execute",
                "bid": f"local-{epoch}",
                "tid": f"t-{epoch}",
                "data": value,
            }
        ),
        qos=1,
    )


def finish_until(fleet, state="COMPLETED", timeout=35):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        for device in fleet["devices"]:
            device.finish()
        if any(d.state.get("state") == state for d in fleet["devices"]):
            return
        time.sleep(0.3)
    raise AssertionError(
        "no terminal " + state + ": " + repr([d.state for d in fleet["devices"]])
    )


def test_real_mqtt_ros_all_arrived(local_fleet):
    f = local_fleet
    wait_for(
        lambda: {item[0] for item in f["messages"]
                 if item[0].startswith("fh/swarm/device/")} >= {
                     f"fh/swarm/device/R{i}/heartbeat" for i in range(3)},
        15, "all devices publish new swarm heartbeats",
    )
    assert not any("/fleet/" in topic for topic, _ in f["messages"])
    submit(f, 1)
    wait_for(
        lambda: any(d.state.get("state") == "ACTIVE" for d in f["devices"]),
        45,
        "mission ACTIVE",
    )
    wait_for(
        lambda: all(d.audit.get("route") for d in f["devices"]),
        15,
        "all TaskHub starts",
    )
    from PIL import Image

    from inspection_interfaces.msg import EventField, PlatformEvent

    device = f["devices"][0]
    folder = device.root / "photos"
    folder.mkdir()
    picture = folder / "local-test.jpg"
    Image.new("RGB", (20, 20), "blue").save(picture)
    route = device.audit["route"]
    event = PlatformEvent(
        identifier="photo.complete",
        output_data=[
            EventField(identifier="route_name", value=route["route_name"]),
            EventField(
                identifier="waypoint_id", value=str(max(route["pending_waypoint_ids"]))
            ),
            EventField(identifier="media_url", value=str(picture)),
        ],
    )
    device.photo.publish(event)
    wait_for(
        lambda: all(
            list((d.root / "coordinator" / "media").glob("*.jpg")) for d in f["devices"]
        ),
        25,
        "separate MQTT image replication",
    )
    finish_until(f)
    wait_for(
        lambda: all(d.state.get("state") == "COMPLETED" for d in f["devices"]),
        10,
        "replicated terminal",
    )
    assert all(d.state["completed_tasks"] == 3 for d in f["devices"])
    assert sum(d.audit["starts"] for d in f["devices"]) == 3
    wait_for(
        lambda: any(
            "data:image/jpeg;base64," in p.read_text()
            for d in f["devices"]
            for p in (d.root / "coordinator" / "reports" / "round-1").glob("*.html")
        ),
        10,
        "self-contained report image",
    )
    wait_for(
        lambda: any(
            v.get("data", {}).get("stage") == "for_platform_mission_finished"
            for t, v in f["messages"]
        ),
        10,
        "platform terminal",
    )


def test_real_mqtt_ros_skip_retry_and_continuous_round(local_fleet):
    f = local_fleet
    submit(f, 2)
    wait_for(
        lambda: all(d.state.get("round_epoch") == 2 for d in f["devices"]),
        20,
        "next round admission",
    )
    # A photo arriving after a new round stays attached to the old route/report.
    from PIL import Image

    from inspection_interfaces.msg import EventField, PlatformEvent

    device = f["devices"][0]
    archive = next((device.root / "agent" / "route_archive").glob("sw1-*.json"))
    old = json.loads(archive.read_text())
    picture = device.root / "photos" / "late-old-round.jpg"
    Image.new("RGB", (20, 20), "green").save(picture)
    device.photo.publish(
        PlatformEvent(
            identifier="photo.complete",
            output_data=[
                EventField(identifier="route_name", value=old["route_name"]),
                EventField(
                    identifier="waypoint_id",
                    value=str(max(int(n) for n in old["mapping"])),
                ),
                EventField(identifier="media_url", value=str(picture)),
            ],
        )
    )
    wait_for(
        lambda: all(
            len(list((d.root / "coordinator" / "media").glob("*.jpg"))) >= 2
            for d in f["devices"]
        ),
        25,
        "late old-round photo replication",
    )
    wait_for(
        lambda: all(
            (d.audit.get("route") or {}).get("route_name", "").startswith("sw2-")
            for d in f["devices"]
        ),
        45,
        "second-round routes",
    )
    for device in f["devices"]:
        device.finish(skip_last=device.robot == "R1")
    wait_for(
        lambda: any(
            d.state.get("plan_revision") == 2 and d.state.get("state") == "ACTIVE"
            for d in f["devices"]
        ),
        30,
        "single skip fallback",
    )
    finish_until(f)
    wait_for(
        lambda: all(d.state.get("state") == "COMPLETED" for d in f["devices"]),
        10,
        "fallback replicated",
    )
    assert all(d.state["retry_performed"] for d in f["devices"])
    assert all(d.state["completed_tasks"] == 3 for d in f["devices"])
    wait_for(
        lambda: any(
            p.read_text().count("<figure>") == 2
            for d in f["devices"]
            for p in (d.root / "coordinator" / "reports" / "round-1").glob(
                "local-1.html"
            )
        ),
        15,
        "old report gets late image",
    )
    wait_for(
        lambda: any(
            list((d.root / "coordinator" / "reports" / "round-2").glob("local-2.html"))
            for d in f["devices"]
        ),
        10,
        "new report generated",
    )
    assert all(
        "data:image/jpeg;base64," not in p.read_text()
        for d in f["devices"]
        for p in (d.root / "coordinator" / "reports" / "round-2").glob("local-2.html")
    )


def test_ros_action_accepts_on_follower_without_executor_deadlock(local_fleet):
    from rclpy.action import ActionClient
    from support import mission

    from inspection_interfaces.action import PlanMission

    f = local_fleet
    receiver = f["devices"][0]
    data = mission(Path(f["maps"]), ["R0", "R1", "R2"], 3, 3)
    data.pop("mission_id", None)
    path = f["root"] / "action-input.json"
    path.write_text(json.dumps(data))
    client = ActionClient(receiver.probe, PlanMission, "/swarm/mission/plan")
    try:
        assert client.wait_for_server(timeout_sec=10)
        future = client.send_goal_async(
            PlanMission.Goal(mission_id="local-3", mission_json_path=str(path))
        )
        wait_for(future.done, 10, "ROS goal accepted")
        assert future.result().accepted
        result = future.result().get_result_async()
        wait_for(result.done, 45, "nonblocking ROS plan result")
        value = result.result().result
        assert value.accepted and value.planned, value.message
        assert Path(value.task_plan_json_path).is_file()
        finish_until(f)
        wait_for(
            lambda: all(d.state.get("state") == "COMPLETED" for d in f["devices"]),
            10,
            "action terminal",
        )
    finally:
        client.destroy()


def test_follower_addition_and_local_stop_are_replicated(local_fleet):
    from std_msgs.msg import String
    from std_srvs.srv import Trigger
    from support import mission

    f = local_fleet
    submit(f, 4)
    wait_for(
        lambda: all(
            (d.audit.get("route") or {}).get("route_name", "").startswith("sw4-")
            for d in f["devices"]
        ),
        45,
        "fourth round",
    )
    leader = next(d.state["leader_id"] for d in f["devices"])
    follower = next(d for d in f["devices"] if d.robot != leader)
    data = mission(Path(f["maps"]), ["R0", "R1", "R2"], 1, 4)
    data["tasks"][0]["id"] = "added-point"
    follower.ingress.publish(
        String(
            data=json.dumps(
                {"method": "task.add", "bid": "local-4", "tid": "add-4", "data": data}
            )
        )
    )
    wait_for(
        lambda: all(
            d.state.get("total_tasks") == 4 and d.state.get("plan_revision") == 2
            for d in f["devices"]
        ),
        35,
        "follower addition consensus",
    )
    wait_for(
        lambda: any(d.state.get("state") == "ACTIVE" for d in f["devices"]),
        15,
        "reallocated routes",
    )
    stop = follower.probe.create_client(Trigger, "/swarm/mission/stop")
    try:
        assert stop.wait_for_service(timeout_sec=5)
        future = stop.call_async(Trigger.Request())
        wait_for(future.done, 10, "local follower stop response")
        assert future.result().success
        wait_for(
            lambda: all(d.state.get("state") == "CANCELLED" for d in f["devices"]),
            30,
            "global cancel via follower",
        )
    finally:
        follower.probe.destroy_client(stop)


def test_broker_loss_expires_permits_without_false_completion(broker_recovery_fleet):
    f = broker_recovery_fleet
    submit(f, 5)
    wait_for(
        lambda: all(
            (d.audit.get("route") or {}).get("route_name", "").startswith("sw5-")
            for d in f["devices"]
        ),
        45,
        "fifth-round starts",
    )
    starts = sum(d.audit["starts"] for d in f["devices"])
    f["broker"].terminate()
    f["broker"].wait(timeout=5)
    wait_for(
        lambda: all(
            d.agent.get("expired") and d.agent.get("start_phase") == "stopped"
            for d in f["devices"]
        ),
        20,
        "all permits expire locally",
    )
    assert all(
        d.state["state"] not in {"COMPLETED", "FAILED", "CANCELLED"}
        for d in f["devices"]
    )
    f["broker"] = subprocess.Popen(
        [os.environ["SWARM_BROKER"], "-c", str(f["broker_config"])],
        stdout=f["broker_log"],
        stderr=subprocess.STDOUT,
    )
    wait_for(
        lambda: all(d.state.get("state") == "FAILED" for d in f["devices"]),
        45,
        "restored quorum commits stopped execution failure",
    )
    assert (
        sum(d.audit["starts"] for d in f["devices"]) == starts
    )  # no stale permit resurrection


def test_action_cancel_during_initial_admission(local_fleet):
    from rclpy.action import ActionClient
    from support import mission

    from inspection_interfaces.action import PlanMission

    f = local_fleet
    device = f["devices"][0]
    data = mission(Path(f["maps"]), ["R0", "R1", "R2"], 3, 6)
    data.pop("mission_id", None)
    path = f["root"] / "cancel-input.json"
    path.write_text(json.dumps(data))
    starts = sum(d.audit["starts"] for d in f["devices"])
    client = ActionClient(device.probe, PlanMission, "/swarm/mission/plan")
    try:
        assert client.wait_for_server(timeout_sec=10)
        sent = client.send_goal_async(
            PlanMission.Goal(mission_id="cancel-6", mission_json_path=str(path))
        )
        wait_for(sent.done, 10, "initial cancel goal accepted")
        handle = sent.result()
        assert handle.accepted
        cancelled = handle.cancel_goal_async()
        wait_for(cancelled.done, 10, "action cancel accepted")
        assert cancelled.result().goals_canceling
        result = handle.get_result_async()
        wait_for(result.done, 10, "cancel action result")
        wait_for(
            lambda: all(
                d.state.get("round_epoch") == 6 and d.state.get("state") == "CANCELLED"
                for d in f["devices"]
            ),
            35,
            "initial-admission global cancellation",
        )
        assert sum(d.audit["starts"] for d in f["devices"]) == starts
    finally:
        client.destroy()


def test_real_mqtt_ros_leader_loss_keeps_unknown_task(takeover_fleet):
    f = takeover_fleet
    submit(f, 7)
    wait_for(
        lambda: all(
            (d.audit.get("route") or {}).get("route_name", "").startswith("sw7-")
            and d.agent.get("start_phase") == "started"
            for d in f["devices"]
        ),
        45,
        "seventh-round routes",
    )
    leader_id = wait_for(
        lambda: next(
            (
                d.state.get("leader_id")
                for d in f["devices"]
                if d.state.get("state") == "ACTIVE"
                and d.state.get("leader_id") == d.robot
                and d.state.get("quorum_available")
            ),
            None,
        )
    )
    lost = next(d for d in f["devices"] if d.robot == leader_id)
    lost.stop_process("consensus")
    lost.stop_process("coordinator")
    lost.stop_process("transport")
    survivors = [d for d in f["devices"] if d is not lost]
    wait_for(
        lambda: any(
            d.state.get("leader_id") not in {"", leader_id}
            and d.state.get("plan_revision", 0) >= 2
            for d in survivors
        ),
        35,
        "new leader and safe replan",
    )
    wait_for(
        lambda: (
            lost.agent.get("expired") and lost.agent.get("start_phase") == "stopped"
        ),
        15,
        "lost device local permit pause",
    )
    end = time.monotonic() + 30
    while time.monotonic() < end:
        for d in survivors:
            d.finish()
        if any(d.state.get("state") == "FAILED" for d in survivors):
            break
        time.sleep(0.3)
    wait_for(
        lambda: all(d.state.get("state") == "FAILED" for d in survivors),
        10,
        "replicated failure",
    )
    assert any(d.state["unconfirmed_tasks"] for d in survivors)


def test_web_console_drives_a_new_round_after_takeover(takeover_fleet, tmp_path, monkeypatch):
    """Run this last, since the console allocates a persistent monotonic epoch."""
    from threading import Thread
    from urllib.request import Request, urlopen

    tool = Path(__file__).resolve().parents[2] / "swarm_dev_console"
    monkeypatch.syspath_prepend(str(tool))
    console_module = pytest.importorskip("swarm_dev_console.server")
    from support import mission

    fleet = takeover_fleet
    survivors = [d for d in fleet["devices"] if d.state.get("state") == "FAILED"]
    assert len(survivors) == 2
    data = mission(fleet["maps"], [d.robot for d in survivors], tasks=2)
    data.pop("mission_id", None)
    with console_module.ThreadingHTTPServer(("127.0.0.1", 0), console_module.Handler) as http:
        http.console = console_module.Console(tmp_path, "127.0.0.1", fleet["broker_port"])
        thread = Thread(target=http.serve_forever)
        thread.start()
        address = f"http://127.0.0.1:{http.server_port}"
        try:
            wait_for(lambda: http.console.connected, 10, "console MQTT ready")
            data["round_epoch"] = http.console.new_epoch()
            outgoing = Request(
                address + "/api/request",
                headers={"X-Swarm-Session": http.console.token},
                data=json.dumps({"bid": "console-e2e", "method": "task.execute",
                                 "data": data}).encode(),
            )
            with urlopen(outgoing, timeout=5) as response:
                epoch = json.load(response)["round_epoch"]
            wait_for(lambda: any(
                d.state.get("round_epoch") == epoch and d.state.get("state") == "ACTIVE"
                for d in survivors
            ), 45, "console-issued round running")
            end = time.monotonic() + 20
            while time.monotonic() < end:
                for device in survivors:
                    device.finish()
                if (http.console.snapshot().get("mission") or {}).get("state") == "COMPLETED":
                    break
                time.sleep(0.15)
            with urlopen(address + "/api/evidence", timeout=5) as response:
                snapshot = json.load(response)
            assert snapshot["mission"]["round_epoch"] == epoch
            assert snapshot["mission"]["state"] == "COMPLETED"
            assert snapshot["mission"]["completed_tasks"] == 2
            assert set(snapshot["mission"]["completed_task_ids"]) == {"p0", "p1"}
            assert snapshot["mission"]["map_package"] == data["map_package"]
            assert any(topic == "fh/swarm/dispatch/services"
                       and frame["data"] == {**data, "round_epoch": epoch}
                       for topic, frame in fleet["messages"] if frame.get("bid") == "console-e2e")
            assert snapshot["plan"]["round_epoch"] == epoch
            assert any(item.get("authority") for item in snapshot["devices"])
            assert snapshot["requests"][0]["status"] == "已受理"
            # The device workflow continues after its temporary platform exits.
            data["round_epoch"] = http.console.new_epoch()
            detached = Request(
                address + "/api/request", headers={"X-Swarm-Session": http.console.token},
                data=json.dumps({"bid": "console-detached", "method": "task.execute",
                                 "data": data}).encode(),
            )
            with urlopen(detached, timeout=5) as response:
                assert json.load(response)["round_epoch"] == data["round_epoch"]
            wait_for(lambda: all(
                device.state.get("round_epoch") == data["round_epoch"]
                and device.state.get("state") == "ACTIVE" for device in survivors
            ), 45, "second task active")
            http.console.client.disconnect()
            http.console.client.loop_stop()

            def completed_without_console():
                for device in survivors:
                    device.finish()
                return all(device.state.get("state") == "COMPLETED" for device in survivors)

            wait_for(completed_without_console, 30, "devices finish after console disconnect")
            assert all(device.state["completed_tasks"] == 2 for device in survivors)
        finally:
            http.shutdown()
            thread.join(timeout=5)
            http.console.close()
