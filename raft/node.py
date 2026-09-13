"""The Raft consensus state machine: leader election, log replication,
and the safety rules that make both correct together. Follows the paper
(Ongaro & Ousterhout, "In Search of an Understandable Consensus
Algorithm") directly enough that most of this maps one rule to one
`if`.

Every RPC handler and every background action (election timeout,
heartbeats, replication) runs under a single `threading.Condition` that
also guards all node state -- multiple RPC handler threads and the
periodic tick thread all touch this state concurrently, and Raft's
safety properties depend on none of them seeing a half-updated view of
it. A client request blocks (releasing the lock while it waits) until
its log entry is actually committed and applied, woken by the same
condition variable that commit-index advancement signals.
"""
from __future__ import annotations

import random
import threading
import time
from enum import Enum

from .log import Log, LogEntry
from .transport import send_rpc

ELECTION_TIMEOUT_RANGE = (0.15, 0.30)  # seconds; randomized so simultaneous candidates rarely keep re-splitting the vote
HEARTBEAT_INTERVAL = 0.05              # well under the minimum election timeout
CLIENT_REQUEST_TIMEOUT = 3.0
TICK_INTERVAL = 0.02


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, node_id, peers: dict, network, apply_callback, snapshot_callback=None, restore_callback=None):
        """peers: {node_id: (host, port)} for every OTHER node.
        apply_callback(command) -> result: applies a committed command to the state machine.
        snapshot_callback() -> data: returns a serializable snapshot of the current state machine.
        restore_callback(data): replaces the state machine's state with a snapshot's contents."""
        self.node_id = node_id
        self.peers = peers
        self.network = network
        self.apply_callback = apply_callback
        self.snapshot_callback = snapshot_callback
        self.restore_callback = restore_callback

        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)

        self.role = Role.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = Log()

        self.commit_index = 0
        self.last_applied = 0
        self._last_apply_result = None

        self.next_index = {}
        self.match_index = {}

        self.leader_id = None
        self.election_deadline = 0.0
        self.last_leader_contact = 0.0  # last time a valid AppendEntries/InstallSnapshot arrived; see _handle_prevote
        self._next_heartbeat_time = 0.0
        self._reset_election_timer()

        self._running = False
        self._tick_thread = None

    # ---- lifecycle ----

    def start(self):
        self._running = True
        self._tick_thread = threading.Thread(target=self._tick_loop, daemon=True)
        self._tick_thread.start()

    def stop(self):
        self._running = False
        if self._tick_thread:
            self._tick_thread.join(timeout=1)

    def _reset_election_timer(self):
        self.election_deadline = time.monotonic() + random.uniform(*ELECTION_TIMEOUT_RANGE)

    def _tick_loop(self):
        while self._running:
            time.sleep(TICK_INTERVAL)
            with self.cv:
                if not self._running:
                    return
                if self.role == Role.LEADER:
                    self._send_heartbeats_if_due()
                elif time.monotonic() >= self.election_deadline:
                    self._reset_election_timer()  # reset immediately so the tick loop doesn't fire another prevote round every tick while these are outstanding
                    self._start_prevote_phase()

    # ---- election ----
    #
    # PreVote (Ongaro's dissertation ยง9.6, used in production by etcd,
    # CockroachDB, and TiKV): a node that suspects the leader is gone asks
    # around WITHOUT incrementing its real term or becoming a candidate
    # first. Only if a majority signal they'd actually vote for it does it
    # commit to a real election. This exists specifically to fix a
    # concrete, testable failure mode of vanilla Raft: a node isolated by
    # a network partition retries elections in a loop, its term climbing
    # far past the rest of the cluster's, with nobody around to ever grant
    # it a vote. The moment the partition heals, that inflated term alone
    # forces the current, perfectly healthy leader to step down (Raft's
    # term rule doesn't care WHY a higher term shows up) and triggers a
    # real, disruptive re-election -- even though the node that caused it
    # still can't win. This project's own test suite hit exactly this
    # scenario (see test_log_compaction_and_snapshot_transfer_to_lagging_node's
    # history) before PreVote was added: a leadership change occurred that
    # had nothing to do with the scenario the test was trying to exercise.
    # A node refusing to grant a prevote while it's actively hearing from a
    # real leader (see _handle_prevote's use of last_leader_contact) means
    # an isolated node's climbing term never leaves its own head.

    def _start_prevote_phase(self):
        prospective_term = self.current_term + 1
        last_log_index = self.log.last_index()
        last_log_term = self.log.last_term()
        prevotes = {self.node_id}
        for peer_id in self.peers:
            threading.Thread(
                target=self._prevote_from,
                args=(peer_id, prospective_term, last_log_index, last_log_term, prevotes),
                daemon=True,
            ).start()

    def _prevote_from(self, peer_id, prospective_term, last_log_index, last_log_term, prevotes: set):
        if not self._running:
            return
        response = send_rpc(self.node_id, self.peers[peer_id], peer_id, self.network, {
            "type": "PreVote", "term": prospective_term, "candidate_id": self.node_id,
            "last_log_index": last_log_index, "last_log_term": last_log_term,
        })
        if response is None or not self._running:
            return
        with self.cv:
            if response["term"] > self.current_term:
                self._become_follower(response["term"])
                return
            if self.role != Role.FOLLOWER or self.current_term + 1 != prospective_term:
                return  # our own state moved on since this prevote round started -- abandon it, a fresh timeout will retry if still needed
            if response.get("vote_granted"):
                prevotes.add(peer_id)
                if len(prevotes) * 2 > len(self.peers) + 1:
                    self._become_candidate()

    def _become_candidate(self):
        if self.role != Role.FOLLOWER:
            return
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._reset_election_timer()
        term_at_start = self.current_term
        last_log_index = self.log.last_index()
        last_log_term = self.log.last_term()
        votes = {self.node_id}

        for peer_id in self.peers:
            threading.Thread(
                target=self._request_vote_from,
                args=(peer_id, term_at_start, last_log_index, last_log_term, votes),
                daemon=True,
            ).start()

    def _request_vote_from(self, peer_id, term, last_log_index, last_log_term, votes: set):
        if not self._running:
            return
        response = send_rpc(self.node_id, self.peers[peer_id], peer_id, self.network, {
            "type": "RequestVote", "term": term, "candidate_id": self.node_id,
            "last_log_index": last_log_index, "last_log_term": last_log_term,
        })
        if response is None or not self._running:
            return
        with self.cv:
            if response["term"] > self.current_term:
                self._become_follower(response["term"])
                return
            if self.role != Role.CANDIDATE or self.current_term != term:
                return  # a stale response from an election we've since moved past
            if response.get("vote_granted"):
                votes.add(peer_id)
                if len(votes) * 2 > len(self.peers) + 1:
                    self._become_leader()

    def _become_follower(self, term: int):
        self.role = Role.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._reset_election_timer()
        self.cv.notify_all()

    def _become_leader(self):
        if self.role != Role.CANDIDATE:
            return
        self.role = Role.LEADER
        self.leader_id = self.node_id
        for peer_id in self.peers:
            self.next_index[peer_id] = self.log.last_index() + 1
            self.match_index[peer_id] = 0
        self._next_heartbeat_time = 0.0  # send heartbeats immediately, don't wait for the next tick

    # ---- replication ----

    def _send_heartbeats_if_due(self):
        now = time.monotonic()
        if now < self._next_heartbeat_time:
            return
        self._next_heartbeat_time = now + HEARTBEAT_INTERVAL
        for peer_id in self.peers:
            threading.Thread(target=self._replicate_to, args=(peer_id,), daemon=True).start()

    def _replicate_to(self, peer_id):
        """Gathers everything needed for one round of replication while
        holding the lock, then makes the actual (potentially slow)
        network call after releasing it -- a blocking send_rpc() call
        made while still holding self.cv would freeze every other RPC
        handler and the tick loop on this node for the duration, which
        for a snapshot transfer in particular could be long enough to
        cause spurious election timeouts and vote flapping."""
        if not self._running:
            return
        with self.cv:
            if self.role != Role.LEADER:
                return
            term = self.current_term
            next_idx = self.next_index[peer_id]
            sending_snapshot = next_idx <= self.log.snapshot_index
            if sending_snapshot:
                snapshot_index = self.log.snapshot_index
                snapshot_term = self.log.snapshot_term
                snapshot_data = self.log.snapshot_data
            else:
                prev_log_index = next_idx - 1
                prev_log_term = self.log.term_at(prev_log_index)
                entries = [{"term": e.term, "command": e.command} for e in self.log.slice_from(next_idx)]
                leader_commit = self.commit_index

        if sending_snapshot:
            response = send_rpc(self.node_id, self.peers[peer_id], peer_id, self.network, {
                "type": "InstallSnapshot", "term": term, "leader_id": self.node_id,
                "last_included_index": snapshot_index, "last_included_term": snapshot_term,
                "data": snapshot_data,
            })
        else:
            response = send_rpc(self.node_id, self.peers[peer_id], peer_id, self.network, {
                "type": "AppendEntries", "term": term, "leader_id": self.node_id,
                "prev_log_index": prev_log_index, "prev_log_term": prev_log_term,
                "entries": entries, "leader_commit": leader_commit,
            })

        if response is None or not self._running:
            return
        with self.cv:
            if response["term"] > self.current_term:
                self._become_follower(response["term"])
                return
            if self.role != Role.LEADER or self.current_term != term:
                return
            if sending_snapshot:
                self.match_index[peer_id] = max(self.match_index[peer_id], snapshot_index)
                self.next_index[peer_id] = snapshot_index + 1
                self._advance_commit_index()
            elif response["success"]:
                self.match_index[peer_id] = prev_log_index + len(entries)
                self.next_index[peer_id] = self.match_index[peer_id] + 1
                self._advance_commit_index()
            else:
                conflict_index = response.get("conflict_index")
                self.next_index[peer_id] = max(1, conflict_index if conflict_index is not None else self.next_index[peer_id] - 1)

    def _advance_commit_index(self):
        match_indices = list(self.match_index.values()) + [self.log.last_index()]
        match_indices.sort(reverse=True)
        candidate_n = match_indices[len(match_indices) // 2]
        # The defining Raft safety rule: a leader may only advance commit_index
        # to an entry from its OWN current term via this majority count.
        # Committing an earlier-term entry this way (just because a majority
        # happens to have replicated it) can be unsafe if that leader is later
        # deposed; it gets committed indirectly instead, once a current-term
        # entry after it is committed by this same rule.
        if candidate_n > self.commit_index and self.log.term_at(candidate_n) == self.current_term:
            self.commit_index = candidate_n
            self._apply_committed()

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is not None:
                self._last_apply_result = self.apply_callback(entry.command)
        self.cv.notify_all()

    # ---- log compaction ----

    def compact_log(self):
        """Snapshots the state machine as of the last applied entry and
        discards everything up to it. Safe to call at any time (it's a
        no-op if there's nothing new to compact); a real deployment would
        call this periodically or once the log grows past some size."""
        with self.cv:
            if self.last_applied <= self.log.snapshot_index or self.snapshot_callback is None:
                return
            snapshot = self.snapshot_callback()
            self.log.compact(self.last_applied, snapshot)

    # ---- RPC dispatch ----

    def handle_rpc(self, msg: dict) -> dict:
        msg_type = msg.get("type")
        if msg_type == "ClientRequest":
            return self._handle_client_request(msg)
        with self.cv:
            if msg_type == "RequestVote":
                return self._handle_request_vote(msg)
            if msg_type == "PreVote":
                return self._handle_prevote(msg)
            if msg_type == "AppendEntries":
                return self._handle_append_entries(msg)
            if msg_type == "InstallSnapshot":
                return self._handle_install_snapshot(msg)
        return {"error": f"unknown RPC type {msg_type!r}"}

    def _handle_prevote(self, msg: dict) -> dict:
        """Never mutates current_term or voted_for -- a prevote is
        non-binding by design, exactly so an isolated node's repeated
        prevote attempts cost the rest of the cluster nothing."""
        prospective_term = msg["term"]
        if prospective_term <= self.current_term:
            return {"term": self.current_term, "vote_granted": False}
        if self.role == Role.FOLLOWER and (time.monotonic() - self.last_leader_contact) < ELECTION_TIMEOUT_RANGE[0]:
            # We've heard from a real leader recently enough that we don't
            # believe anything is actually wrong -- refusing here is what
            # stops a merely-isolated (not actually leaderless) node from
            # ever collecting a majority of prevotes.
            return {"term": self.current_term, "vote_granted": False}
        log_ok = (msg["last_log_term"] > self.log.last_term()
                  or (msg["last_log_term"] == self.log.last_term() and msg["last_log_index"] >= self.log.last_index()))
        return {"term": self.current_term, "vote_granted": log_ok}

    def _handle_request_vote(self, msg: dict) -> dict:
        term = msg["term"]
        if term > self.current_term:
            self._become_follower(term)
        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        candidate_id = msg["candidate_id"]
        log_ok = (msg["last_log_term"] > self.log.last_term()
                  or (msg["last_log_term"] == self.log.last_term() and msg["last_log_index"] >= self.log.last_index()))
        if self.voted_for in (None, candidate_id) and log_ok:
            self.voted_for = candidate_id
            self._reset_election_timer()
            return {"term": self.current_term, "vote_granted": True}
        return {"term": self.current_term, "vote_granted": False}

    def _handle_append_entries(self, msg: dict) -> dict:
        term = msg["term"]
        if term > self.current_term:
            self._become_follower(term)
        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self.role = Role.FOLLOWER  # a candidate that hears from a valid current-term leader steps down
        self.leader_id = msg["leader_id"]
        self._reset_election_timer()
        self.last_leader_contact = time.monotonic()

        prev_log_index = msg["prev_log_index"]
        prev_log_term = msg["prev_log_term"]
        if prev_log_index > 0:
            actual_term = self.log.term_at(prev_log_index)
            if actual_term != prev_log_term:
                conflict_term = actual_term
                conflict_index = prev_log_index
                floor = self.log.snapshot_index + 1
                while conflict_index > floor and self.log.term_at(conflict_index - 1) == conflict_term:
                    conflict_index -= 1
                return {"term": self.current_term, "success": False, "conflict_index": conflict_index, "conflict_term": conflict_term}

        entries = [LogEntry(e["term"], e["command"]) for e in msg["entries"]]
        insert_index = prev_log_index + 1
        for i, entry in enumerate(entries):
            idx = insert_index + i
            existing_term = self.log.term_at(idx)
            if existing_term == 0:
                self.log.append(entries[i:])
                break
            if existing_term != entry.term:
                self.log.truncate_from(idx)
                self.log.append(entries[i:])
                break
            # existing_term == entry.term: already have this exact entry (a retried/duplicate RPC) -- keep scanning

        if msg["leader_commit"] > self.commit_index:
            self.commit_index = min(msg["leader_commit"], self.log.last_index())
            self._apply_committed()

        return {"term": self.current_term, "success": True}

    def _handle_install_snapshot(self, msg: dict) -> dict:
        term = msg["term"]
        if term > self.current_term:
            self._become_follower(term)
        if term < self.current_term:
            return {"term": self.current_term}

        self.leader_id = msg["leader_id"]
        self._reset_election_timer()
        self.last_leader_contact = time.monotonic()
        last_included_index = msg["last_included_index"]
        last_included_term = msg["last_included_term"]
        if last_included_index <= self.log.snapshot_index:
            return {"term": self.current_term}  # already have this snapshot or a newer one

        existing = self.log.get(last_included_index)
        if existing is not None and existing.term == last_included_term:
            self.log.entries = self.log.slice_from(last_included_index + 1)
        else:
            self.log.entries = []
        self.log.snapshot_index = last_included_index
        self.log.snapshot_term = last_included_term
        self.log.snapshot_data = msg["data"]

        if self.restore_callback is not None:
            self.restore_callback(msg["data"])
        self.commit_index = max(self.commit_index, last_included_index)
        self.last_applied = last_included_index
        self.cv.notify_all()
        return {"term": self.current_term}

    def _handle_client_request(self, msg: dict) -> dict:
        with self.cv:
            if self.role != Role.LEADER:
                return {"success": False, "leader_hint": self.leader_id}
            entry_term = self.current_term
            self.log.append([LogEntry(entry_term, msg["command"])])
            my_index = self.log.last_index()

        # Kick off replication immediately rather than waiting for the next
        # heartbeat tick, so a client isn't stuck idling for up to
        # HEARTBEAT_INTERVAL before anything even gets sent.
        with self.cv:
            if self.role == Role.LEADER:
                self._next_heartbeat_time = 0.0

        with self.cv:
            deadline = time.monotonic() + CLIENT_REQUEST_TIMEOUT
            while self.last_applied < my_index:
                if self.role != Role.LEADER or self.current_term != entry_term:
                    return {"success": False, "leader_hint": self.leader_id}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {"success": False, "error": "timed out waiting for commit"}
                self.cv.wait(timeout=remaining)
            return {"success": True, "result": self._last_apply_result}

    # ---- introspection (for tests/demo only, not part of the protocol) ----

    def status(self) -> dict:
        with self.cv:
            return {
                "node_id": self.node_id, "role": self.role.value, "term": self.current_term,
                "leader_id": self.leader_id, "log_length": self.log.last_index(),
                "commit_index": self.commit_index, "last_applied": self.last_applied,
                "snapshot_index": self.log.snapshot_index,
            }
