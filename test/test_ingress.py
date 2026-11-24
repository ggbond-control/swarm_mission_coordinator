"""Control requests must remain bound while an epoch enters consensus."""

from types import SimpleNamespace

import pytest
from swarm_mission_coordinator.model import MissionState
from swarm_mission_coordinator.ingress import MissionIngress


class Ingress:
    accept_request = MissionIngress.accept_request
    request_control = MissionIngress.request_control
    enqueue_request = MissionIngress.enqueue_request
    forward_requests = MissionIngress.forward_requests

    def __init__(self, root, previous=False):
        model = MissionState()
        if previous:
            model.epoch = 1
            model.state = "COMPLETED"
            model.mission = {"mission_id": "old"}
        self.core = SimpleNamespace(
            model=model, consensus={}, controls=[], additions=[], authority=lambda: True
        )
        self.pending_request = {
            "bid": "new",
            "data": {"round_epoch": 2, "robot_ids": ["A"]},
        }
        self.commands_path = root / "private_commands.json"
        self.command_outbox = {}
        self.robot = "A"
        self.sent = []
        self.relay = {}
        self.last_relay = 0.0

    def pub(self, kind, value):
        self.sent.append((kind, value))


@pytest.mark.parametrize("previous", [False, True])
def test_cancel_before_mission_commit_survives_admission(tmp_path, previous):
    node = Ingress(tmp_path, previous)
    node.request_control("task.stop")
    assert len(node.command_outbox) == 1
    envelope = next(iter(node.command_outbox.values()))
    assert envelope["bid"] == "new" and envelope["data"]["round_epoch"] == 2
    node.forward_requests(2.0)
    assert len(node.command_outbox) == 1 and not node.core.controls
    # The old terminal record must not discard a newer pending cancellation.
    node.core.model.epoch = 2
    node.core.model.mission = {"mission_id": "new"}
    node.core.model.state = "PREPARING"
    node.forward_requests(4.0)
    assert len(node.core.controls) == 1
    identity = next(iter(node.command_outbox))
    assert node.core.controls[0]["request_id"] == identity
    node.forward_requests(6.0)
    assert len(node.core.controls) == 1
    node.core.model.applied[identity] = 1
    node.control_reply = lambda value: None
    node.forward_requests(8.0)
    assert not node.command_outbox


@pytest.mark.parametrize(
    "envelope",
    [
        {"method": "task.stop", "bid": "old", "data": {"round_epoch": 2}},
        {"method": "task.stop", "bid": "new", "data": {"round_epoch": 1}},
    ],
)
def test_old_control_cannot_cancel_new_admission(tmp_path, envelope):
    node = Ingress(tmp_path, True)
    with pytest.raises(ValueError):
        node.accept_request(envelope)
    assert not node.command_outbox and not node.sent


def test_full_request_relay_stops_after_round_activation(tmp_path):
    node = Ingress(tmp_path)
    node.core.model.epoch = 2
    node.core.model.state = "PREPARING"
    node.relay = {"input": ({"data": {"round_epoch": 2, "robot_ids": ["A", "B"]}}, 120)}
    forwarded = []
    node.wire = lambda *args: forwarded.append(args)
    node.forward_requests(2)
    assert len(forwarded) == 1 and forwarded[0][:2] == ("B", "round_input")
    node.core.model.state = "PLANNING"
    node.forward_requests(4)
    assert len(forwarded) == 1 and not node.relay
