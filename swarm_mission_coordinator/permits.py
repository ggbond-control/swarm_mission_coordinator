"""Bind execution permits to a fresh quorum proof and the applied route."""

import copy
import time


class ExecutionPermits:
    def __init__(self, core, robot, pub, wire):
        self.core, self.robot, self.pub, self.wire = core, robot, pub, wire
        self.read_requests = {}
        self.grants = {}

    def receive(self, sender, kind, value):
        if kind == "permit_request":
            m = self.core.model
            if (
                not self.core.authority()
                or value.get("robot_id") != sender
                or sender not in m.members
                or sender not in m.started
                or m.state != "ACTIVE"
                or sender in m.stopped
                or value.get("session_id") != m.sessions[sender]["session_id"]
                or value.get("round_epoch") != m.epoch
                or value.get("revision") != m.revision
                or value.get("route_name") != m.route_name(sender)
            ):
                return
            self.read_requests[value["request_id"]] = (
                copy.deepcopy(value),
                time.monotonic(),
            )
            self.pub("read", {"request_id": value["request_id"]})
        elif kind == "permit_grant":
            self.grants[value["request_id"]] = (sender, value, time.monotonic())

    def read_confirmed(self, proof):
        item = self.read_requests.pop(proof["request_id"], None)
        if (
            item
            and self.core.authority()
            and proof["term"] == self.core.consensus["term"]
        ):
            value, _ = item
            self.wire(
                value["robot_id"],
                "permit_grant",
                {**value, "term": proof["term"], "commit_index": proof["commit_index"]},
            )
    def permit_request(self, value):
        leader = self.core.consensus.get("leader_id")
        if leader:
            self.wire(leader, "permit_request", value)
    def tick(self, now):
        for key, (sender, value, at) in list(self.grants.items()):
            c, m = self.core.consensus, self.core.model
            if now - at > 3.0:
                del self.grants[key]
            elif (
                sender == c.get("leader_id")
                and value["term"] == c.get("term")
                and m.last_index >= value["commit_index"]
                and m.state == "ACTIVE"
                and self.robot in m.members
                and self.robot not in m.stopped
            ):
                self.pub("permit", value)
                del self.grants[key]
        self.read_requests = {
            k: v for k, v in self.read_requests.items() if now - v[1] < 3.0
        }
