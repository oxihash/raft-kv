"""Wires N real RaftNode instances to N real RpcServers over real TCP
sockets on localhost, and gives tests a way to submit client commands
and inject network partitions. Everything a test does through this class
is indistinguishable, from any single node's point of view, from talking
to genuinely separate machines.
"""
from __future__ import annotations

import time

from .kvstore import KVStateMachine
from .node import RaftNode, Role
from .transport import Network, RpcServer, send_rpc


class ClusterNode:
    def __init__(self, node_id, port, peers, network):
        self.node_id = node_id
        self.port = port
        self.kv = KVStateMachine()
        self.raft = RaftNode(
            node_id, peers, network,
            apply_callback=self.kv.apply,
            snapshot_callback=self.kv.snapshot,
            restore_callback=self.kv.restore,
        )
        self.server = RpcServer(node_id, port, self.raft.handle_rpc, network)

    def start(self):
        self.server.start()
        self.raft.start()

    def stop(self):
        self.raft.stop()
        self.server.stop()


class Cluster:
    def __init__(self, size: int, base_port: int = 17000):
        self.network = Network()
        self.node_ids = [f"n{i}" for i in range(size)]
        addrs = {nid: ("127.0.0.1", base_port + i) for i, nid in enumerate(self.node_ids)}
        self.nodes = {}
        for nid in self.node_ids:
            peers = {pid: addr for pid, addr in addrs.items() if pid != nid}
            self.nodes[nid] = ClusterNode(nid, addrs[nid][1], peers, self.network)
        self.addrs = addrs

    def start(self):
        for node in self.nodes.values():
            node.start()

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def raft(self, node_id) -> RaftNode:
        return self.nodes[node_id].raft

    def leader(self, timeout: float = 5.0):
        """Waits for exactly one node to be a leader that nothing
        reachable from it is about to depose, and returns its node_id.

        A higher term existing SOMEWHERE reachable is not "agreement" --
        it means a term change is still in flight and this leader is
        about to step down the moment it finds out, which is exactly the
        transient state observed right after healing a partition (the
        old leader keeps reporting itself as leader for one more instant
        while an isolated node's stale-but-higher term hasn't reached it
        yet). Only a term that is the maximum among reachable nodes, with
        every reachable node either following it or still mid-election,
        counts as stable -- and "reachable" alone isn't enough either: a
        node is always trivially reachable from itself, so a leader that
        has just been partitioned into a minority of one would otherwise
        pass this check purely by agreeing with itself. Require the
        reachable-and-agreeing set to actually be a majority of the whole
        cluster.

        Note this deliberately does NOT require there to be only one
        self-reported leader cluster-wide: a node that was just
        partitioned away while leading correctly keeps believing it's
        still leader (nothing has told it otherwise, which is exactly
        real Raft behavior, not a bug) and will coexist with a genuine
        new leader elected by the remaining majority. Every node
        currently claiming Role.LEADER is checked; only the one with
        real majority backing (if any) can pass."""
        deadline = time.monotonic() + timeout
        total = len(self.nodes)
        while time.monotonic() < deadline:
            for leader_id, node in self.nodes.items():
                if node.raft.role != Role.LEADER:
                    continue
                leader_term = node.raft.current_term
                reachable = [n for nid, n in self.nodes.items() if self.network.is_connected(nid, leader_id)]
                stable = (
                    len(reachable) * 2 > total
                    and all(n.raft.current_term <= leader_term and n.raft.leader_id in (leader_id, None) for n in reachable)
                )
                if stable:
                    return leader_id
            time.sleep(0.02)
        raise TimeoutError(f"no stable leader emerged within {timeout}s (current roles: {self.status()})")

    def submit(self, command, timeout: float = 3.0, retries: int = 10):
        """Submits a command to whichever node is currently believed to
        be the leader, following leader_hint redirects, exactly like a
        real client would (no cluster-internal shortcuts)."""
        node_id = self.leader(timeout=timeout)
        for _ in range(retries):
            addr = self.addrs[node_id]
            response = send_rpc("client", addr, node_id, self.network, {"type": "ClientRequest", "command": command}, timeout=timeout)
            if response is None:
                node_id = self.leader(timeout=timeout)
                continue
            if response.get("success"):
                return response.get("result")
            hint = response.get("leader_hint")
            if hint:
                node_id = hint
            else:
                node_id = self.leader(timeout=timeout)
        raise RuntimeError(f"submit({command!r}) failed after {retries} retries")

    def status(self) -> dict:
        return {nid: n.raft.status() for nid, n in self.nodes.items()}

    def partition(self, group_a: list, group_b: list):
        self.network.partition(group_a, group_b)

    def heal(self):
        self.network.heal()
