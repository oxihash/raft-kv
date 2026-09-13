"""The replicated application: a plain dict, mutated only by commands
that have gone through Raft and been committed. This is deliberately
trivial -- the point of the project is the consensus layer underneath
it, not the application on top, and a trivial state machine makes it
obvious that any test failure is a Raft bug, not an application bug.
"""
from __future__ import annotations


class KVStateMachine:
    def __init__(self):
        self.data = {}

    def apply(self, command):
        op = command[0]
        if op == "SET":
            _, key, value = command
            self.data[key] = value
            return value
        if op == "DELETE":
            _, key = command
            return self.data.pop(key, None)
        if op == "GET":
            _, key = command
            return self.data.get(key)
        raise ValueError(f"unknown command {command!r}")

    def snapshot(self):
        return dict(self.data)

    def restore(self, snapshot):
        self.data = dict(snapshot)
