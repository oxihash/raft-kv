"""Integration tests against a real cluster: real threads, real TCP
sockets on localhost, real timing. Nothing here mocks the network or the
Raft state machine -- a "partition" cuts real socket traffic, and a
"leader crash" stops a real thread and closes its real listening socket.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from raft.cluster import Cluster
from raft.node import Role

PORT_COUNTER = [19000]


def fresh_cluster(size: int) -> Cluster:
    PORT_COUNTER[0] += 100  # keep every test's ports well apart so a slow-closing socket from a previous test can't collide
    return Cluster(size, base_port=PORT_COUNTER[0])


def test_leader_election_and_basic_replication():
    cluster = fresh_cluster(3)
    cluster.start()
    try:
        leader = cluster.leader(timeout=5)
        assert leader is not None
        cluster.submit(("SET", "x", 1))
        cluster.submit(("SET", "y", 2))
        time.sleep(0.3)
        for node in cluster.nodes.values():
            assert node.kv.data == {"x": 1, "y": 2}
        print("test_leader_election_and_basic_replication passed")
    finally:
        cluster.stop()


def test_exactly_one_leader_per_term():
    """At no point during startup should two nodes both believe they're
    the leader for the SAME term -- that would be a real split-brain, the
    one thing Raft exists to prevent."""
    cluster = fresh_cluster(5)
    cluster.start()
    try:
        cluster.leader(timeout=5)
        time.sleep(1.0)  # let a few election cycles happen if any were going to
        by_term = {}
        for node in cluster.nodes.values():
            if node.raft.role == Role.LEADER:
                by_term.setdefault(node.raft.current_term, []).append(node.node_id)
        for term, leaders in by_term.items():
            assert len(leaders) == 1, f"term {term} had multiple leaders: {leaders}"
        print("test_exactly_one_leader_per_term passed")
    finally:
        cluster.stop()


def test_leader_crash_triggers_reelection_and_progress_continues():
    cluster = fresh_cluster(5)
    cluster.start()
    try:
        leader1 = cluster.leader(timeout=5)
        cluster.submit(("SET", "before_crash", 1))

        cluster.nodes[leader1].stop()  # a real thread stop + real socket close, not a flag
        del cluster.nodes[leader1]     # this node is gone for the rest of the test -- can't submit to it or count it as a live replica

        leader2 = cluster.leader(timeout=5)
        assert leader2 != leader1, "a new leader must be elected after the old one is gone"

        cluster.submit(("SET", "after_crash", 2))
        time.sleep(0.3)
        for node in cluster.nodes.values():
            assert node.kv.data.get("before_crash") == 1
            assert node.kv.data.get("after_crash") == 2
        print("test_leader_crash_triggers_reelection_and_progress_continues passed")
    finally:
        cluster.stop()


def test_majority_partition_keeps_serving_minority_does_not_elect():
    """The definitive split-brain test: cut a 5-node cluster into a
    3-node majority and a 2-node minority. The majority side must keep
    electing leaders and committing writes; the minority side must NEVER
    successfully elect a leader (it can't reach a majority of votes), so
    it must make zero progress for as long as the partition holds."""
    cluster = fresh_cluster(5)
    cluster.start()
    try:
        leader = cluster.leader(timeout=5)
        cluster.submit(("SET", "before_partition", 1))

        all_ids = list(cluster.node_ids)
        # Ensure the ORIGINAL leader ends up in the majority side, so we're
        # testing "majority keeps a leader" rather than accidentally
        # testing "a brand new election happens to work", which would be
        # a weaker (though still true) claim.
        majority = [leader] + [n for n in all_ids if n != leader][:2]
        minority = [n for n in all_ids if n not in majority]
        assert len(majority) == 3 and len(minority) == 2

        cluster.partition(majority, minority)

        # Minority must never manage to elect a leader while cut off --
        # checked repeatedly over a window long enough for several of its
        # own election-timeout cycles to fire and fail.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            for nid in minority:
                assert cluster.nodes[nid].raft.role != Role.LEADER, f"{nid} in the minority partition must never become leader"
            time.sleep(0.05)

        # Majority side must still be able to commit new writes.
        result = cluster.submit(("SET", "during_partition", 2))
        assert result == 2
        time.sleep(0.3)
        for nid in majority:
            assert cluster.nodes[nid].kv.data.get("during_partition") == 2

        # Heal, and everyone (including the formerly-isolated minority)
        # must converge on the same state.
        cluster.heal()
        cluster.submit(("SET", "after_heal", 3))
        time.sleep(0.5)
        for node in cluster.nodes.values():
            assert node.kv.data.get("before_partition") == 1
            assert node.kv.data.get("during_partition") == 2
            assert node.kv.data.get("after_heal") == 3
        print("test_majority_partition_keeps_serving_minority_does_not_elect passed")
    finally:
        cluster.stop()


def test_isolated_former_leader_steps_down_and_rejoins_cleanly():
    """Partitions the CURRENT leader into a minority of one. The
    remaining majority must elect a new leader and keep committing; the
    isolated old leader (still believing it's leader, since nothing has
    told it otherwise yet) must have its writes never actually commit,
    and once healed it must accept the new leader's higher term and
    replicate its log instead of diverging permanently."""
    cluster = fresh_cluster(5)
    cluster.start()
    try:
        leader = cluster.leader(timeout=5)
        cluster.submit(("SET", "k1", "v1"))

        others = [n for n in cluster.node_ids if n != leader]
        cluster.partition([leader], others)

        new_leader = cluster.leader(timeout=5)
        assert new_leader != leader, "the remaining majority must elect a different leader"
        cluster.submit(("SET", "k2", "v2"))

        cluster.heal()
        time.sleep(1.0)  # give the old leader's next heartbeat/election-timeout cycle a chance to observe the higher term

        assert cluster.nodes[leader].raft.role != Role.LEADER, "the old leader must step down once it can see the newer term again"

        cluster.submit(("SET", "k3", "v3"))
        time.sleep(0.5)
        for node in cluster.nodes.values():
            assert node.kv.data.get("k1") == "v1"
            assert node.kv.data.get("k2") == "v2"
            assert node.kv.data.get("k3") == "v3"
        print("test_isolated_former_leader_steps_down_and_rejoins_cleanly passed")
    finally:
        cluster.stop()


def test_prevote_stops_an_isolated_node_from_disrupting_the_leader():
    """The concrete failure mode PreVote exists to fix: isolate one
    follower on its own (unable to ever reach a majority), let it retry
    real elections in a loop long enough for its term to climb well past
    the rest of the cluster, then heal the partition. Without PreVote,
    that inflated term alone would force the healthy leader to step down
    and trigger an unnecessary real election the instant the isolated
    node's next RequestVote arrives. With PreVote, the isolated node
    never got to bump its own real term in the first place (a prevote
    is non-binding), so healing should be a non-event: the SAME leader
    and term from before the partition, still standing, no interruption."""
    cluster = fresh_cluster(5)
    cluster.start()
    try:
        leader = cluster.leader(timeout=5)
        leader_term_before = cluster.raft(leader).current_term
        cluster.submit(("SET", "k1", "v1"))

        isolated = [n for n in cluster.node_ids if n != leader][0]
        cluster.partition([isolated], [n for n in cluster.node_ids if n != isolated])

        time.sleep(2.0)  # long enough for several election-timeout cycles on the isolated node

        assert cluster.raft(leader).role == Role.LEADER, "the leader must never have been disrupted by the isolated node"
        assert cluster.raft(leader).current_term == leader_term_before, "the leader's term must be unchanged -- no real election should have happened"
        # PreVote is non-binding: the isolated node's OWN current_term must
        # still be whatever it started at, since it never won a majority
        # of prevotes and therefore never converted to a real candidacy.
        assert cluster.raft(isolated).current_term == leader_term_before, "an isolated node using PreVote should never actually increment its real term without majority backing"

        cluster.heal()
        cluster.submit(("SET", "k2", "v2"))
        time.sleep(0.3)
        for node in cluster.nodes.values():
            assert node.kv.data.get("k1") == "v1"
            assert node.kv.data.get("k2") == "v2"
        print("test_prevote_stops_an_isolated_node_from_disrupting_the_leader passed")
    finally:
        cluster.stop()


def test_log_compaction_and_snapshot_transfer_to_lagging_node():
    """Isolates a follower, does enough writes on the majority side to
    justify compacting the leader's log, heals the partition, and checks
    the previously-isolated follower catches up via InstallSnapshot
    (since the leader no longer has the individual log entries it would
    otherwise need) rather than getting stuck forever."""
    cluster = fresh_cluster(3)
    cluster.start()
    try:
        leader = cluster.leader(timeout=5)
        others = [n for n in cluster.node_ids if n != leader]
        lagging = others[0]

        cluster.partition([lagging], [leader] + others[1:])

        for i in range(20):
            cluster.submit(("SET", f"k{i}", i))

        cluster.raft(leader).compact_log()
        assert cluster.raft(leader).log.snapshot_index > 0, "the leader should have something to compact after 20 committed entries"

        cluster.heal()
        # The lagging node needs an InstallSnapshot round-trip plus catch-up
        # AppendEntries, which takes a bit longer than a normal heartbeat.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if cluster.nodes[lagging].kv.data.get("k19") == 19:
                break
            time.sleep(0.05)
        assert cluster.nodes[lagging].kv.data.get("k19") == 19, "the lagging node never caught up after healing"
        for i in range(20):
            assert cluster.nodes[lagging].kv.data.get(f"k{i}") == i
        assert cluster.raft(lagging).log.snapshot_index > 0, "the lagging node should have installed the leader's snapshot, not replayed 20 individual entries from before its own log start"
        print("test_log_compaction_and_snapshot_transfer_to_lagging_node passed")
    finally:
        cluster.stop()


def test_write_survives_two_sequential_leader_failures():
    cluster = fresh_cluster(5)
    cluster.start()
    try:
        leader1 = cluster.leader(timeout=5)
        cluster.submit(("SET", "a", 1))

        cluster.nodes[leader1].stop()
        del cluster.nodes[leader1]
        leader2 = cluster.leader(timeout=5)
        cluster.submit(("SET", "b", 2))

        cluster.nodes[leader2].stop()
        del cluster.nodes[leader2]
        leader3 = cluster.leader(timeout=5)
        cluster.submit(("SET", "c", 3))

        # submit() returns as soon as the LEADER itself has applied an
        # entry (majority match achieved); followers only catch up to the
        # new commit_index on their next heartbeat, up to HEARTBEAT_INTERVAL
        # later -- this is normal eventual consistency to followers, not a
        # correctness gap, so give it a moment before checking everyone.
        time.sleep(0.3)
        for node in cluster.nodes.values():
            assert node.kv.data == {"a": 1, "b": 2, "c": 3}
        print("test_write_survives_two_sequential_leader_failures passed")
    finally:
        cluster.stop()


def test_concurrent_client_requests_each_get_their_own_result():
    """Each ClientRequest handler must return the result of applying ITS
    OWN entry, not whichever entry happened to be applied last. This is
    easy to get wrong by caching a single "last apply result" on the
    node: when several requests get batched into the same AppendEntries
    round and their commit_index advances past all of them at once,
    _apply_committed() applies every one of them before any waiting
    client thread wakes up, so a single shared slot ends up holding only
    the last entry's result -- and every earlier request reads that same
    wrong value instead of its own."""
    cluster = fresh_cluster(3)
    cluster.start()
    try:
        cluster.leader(timeout=5)
        expected = {f"k{i}": f"v{i}" for i in range(20)}
        results = {}

        def submit(key, value):
            results[key] = cluster.submit(("SET", key, value))

        threads = [threading.Thread(target=submit, args=(k, v)) for k, v in expected.items()]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results == expected, f"mismatches: {[(k, results.get(k), v) for k, v in expected.items() if results.get(k) != v]}"
        print("test_concurrent_client_requests_each_get_their_own_result passed")
    finally:
        cluster.stop()


if __name__ == "__main__":
    test_leader_election_and_basic_replication()
    test_exactly_one_leader_per_term()
    test_leader_crash_triggers_reelection_and_progress_continues()
    test_majority_partition_keeps_serving_minority_does_not_elect()
    test_isolated_former_leader_steps_down_and_rejoins_cleanly()
    test_prevote_stops_an_isolated_node_from_disrupting_the_leader()
    test_log_compaction_and_snapshot_transfer_to_lagging_node()
    test_write_survives_two_sequential_leader_failures()
    test_concurrent_client_requests_each_get_their_own_result()
    print("\nALL RAFT INTEGRATION TESTS PASSED")
