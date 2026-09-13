# raft-kv

A real implementation of Raft consensus, tested by actually breaking it: killing the leader mid-cluster, cutting the network into a majority and a minority and checking the minority never elects anyone, and healing the partition back together and checking every node converges on the same state.

Nodes talk over real TCP sockets on localhost, not in-process function calls. A "network partition" in this test suite is a genuine severed connection: the transport layer refuses to open or accept a socket between two nodes marked as cut off, both on the sending side and the receiving side. From any single node's point of view, this is indistinguishable from being on a different machine.

## Try it

```bash
python demo.py
```

Starts a 5-node cluster, submits a write, kills the leader and watches a new one get elected, partitions the network 3-2 and proves the 2-node minority can never elect a leader while the 3-node majority keeps serving writes, then heals the partition and confirms everyone converges.

## What this actually covers

Most portfolio Raft implementations stop at leader election. This one implements the parts of the paper that actually get exercised in a real deployment:

**Leader election and log replication**, per the paper: randomized election timeouts, the log-completeness voting restriction (a candidate whose log is behind can't win, even with a higher term), and the specific safety rule for advancing commit index (a leader can only directly commit an entry from its own current term; earlier-term entries commit indirectly once a current-term entry after them does).

**Log compaction via snapshotting.** A leader can compact everything up to its last-applied index into a single snapshot. A follower that's fallen behind far enough that the leader no longer has the individual log entries it would need gets an `InstallSnapshot` RPC instead, and catches up in one transfer rather than being stuck forever. Verified by isolating a follower, doing 20 writes, compacting the leader's log, healing, and checking the follower actually receives a snapshot rather than 20 individually-replayed entries.

**PreVote.** This is the one most toy implementations skip, and skipping it has a real, reproducible consequence. Isolate a follower, and it retries real elections in a loop while cut off, its term climbing far past the rest of the cluster's, with nobody around to grant it a vote. The instant the partition heals, that inflated term alone forces the current, perfectly healthy leader to step down (Raft's term rule doesn't check *why* a higher term showed up) and triggers a real, unnecessary election, even though the node that caused it still can't win anything. This project's own test suite hit exactly this failure during development: a log-compaction test kept failing because a leadership change it never asked for happened right when the partition healed. The fix is PreVote (from Diego Ongaro's dissertation, used in production by etcd, CockroachDB, and TiKV): before incrementing its real term, a node first asks around non-bindingly, and a peer that's heard from a real leader recently just refuses. An isolated node's term never leaves its own head. There's a dedicated test proving this directly: isolate a node for two full seconds of election-timeout cycles, and the leader's term must not move at all.

## How it's structured

```
raft/
  log.py          the replicated log, 1-based indexing matching the paper directly, with snapshot support
  transport.py    real TCP sockets, length-prefixed JSON, partition injection on both send and receive
  node.py         the state machine: election, PreVote, replication, safety rules
  kvstore.py      the trivial application on top -- a dict, mutated only by committed commands
  cluster.py      wires N real nodes together for tests/demos, with partition/heal helpers
demo.py           narrated walkthrough
tests/test_raft.py  integration tests against a real running cluster
```

## Running the tests

```bash
python tests/test_raft.py
```

Eight tests, each against a real cluster with real threads and real sockets, no mocking of the network or the state machine:

- basic election and replication to all nodes
- no two nodes are ever leader in the same term
- leader crash triggers re-election and writes keep flowing
- a 3-2 partition: the minority never elects anyone, the majority keeps serving, healing converges everyone
- a partitioned leader steps down once it can see the newer term again after healing
- PreVote: an isolated node's climbing term never disrupts the real leader
- log compaction and snapshot transfer to a node that fell behind
- writes survive two sequential leader failures in a row

Two real bugs surfaced during development and are worth being upfront about, since this is exactly the kind of thing a rushed distributed-systems project gets wrong silently: an early version of `_replicate_to` called the blocking network send while still holding the node's own lock for a snapshot transfer, which would have frozen all other RPC handling on that node for the duration of a slow transfer; and the log-compaction test itself was flaky before PreVote existed, for the real reason described above, not because of a bug in the test harness's timing.

## Design choices that are deliberately simple

Every intermediate value in the log uses 1-based indexing exactly matching the Raft paper, specifically so the code reads as a direct transcription of the paper's rules rather than an off-by-one translation exercise. Client requests block synchronously on the node they were submitted to until the entry commits, using a condition variable woken on every commit-index advance, rather than a callback-based or async API. There's no cluster membership change support (adding or removing nodes from a running cluster) -- the cluster size is fixed at startup. Snapshots are just a `dict` copy of the whole key-value store; a real system would chunk large snapshots across multiple RPCs instead of sending one potentially-large blob.
