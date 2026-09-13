#!/usr/bin/env python3
"""A narrated walkthrough of the cluster surviving the failures Raft is
actually for: a leader crash, then a real network partition with
split-brain prevention, then healing. Run it and read along.
"""
from __future__ import annotations

import time

from raft.cluster import Cluster
from raft.node import Role


def status_line(cluster: Cluster) -> str:
    parts = []
    for nid in cluster.node_ids:
        if nid not in cluster.nodes:
            parts.append(f"{nid}=DEAD")
            continue
        s = cluster.raft(nid).status()
        tag = {"leader": "LEADER", "candidate": "candidate", "follower": "follower"}[s["role"]]
        parts.append(f"{nid}={tag}(term={s['term']})")
    return "  ".join(parts)


def main():
    print("Starting a 5-node cluster on real TCP sockets (127.0.0.1:20000-20004)...\n")
    cluster = Cluster(5, base_port=20000)
    cluster.start()

    leader = cluster.leader(timeout=5)
    print(f"Leader elected: {leader}")
    print(status_line(cluster))

    print("\nSubmitting SET orders 42 = 'pending' ...")
    cluster.submit(("SET", "order:42", "pending"))
    time.sleep(0.2)
    print("Replicated to all 5 nodes:", {nid: cluster.nodes[nid].kv.data.get("order:42") for nid in cluster.node_ids})

    print(f"\nKilling the leader ({leader}) -- a real thread stop and socket close...")
    cluster.nodes[leader].stop()
    del cluster.nodes[leader]

    new_leader = cluster.leader(timeout=5)
    print(f"New leader elected: {new_leader}")
    print(status_line(cluster))

    print("\nSubmitting SET order:42 = 'shipped' to the new leader...")
    cluster.submit(("SET", "order:42", "shipped"))
    time.sleep(0.2)
    print("State on every surviving node:", {nid: cluster.nodes[nid].kv.data.get("order:42") for nid in cluster.nodes})

    remaining = list(cluster.nodes.keys())
    majority = remaining[:3]
    minority = remaining[3:]
    print(f"\nPartitioning the network: majority={majority}  minority={minority}")
    cluster.partition(majority, minority)

    print("Watching the minority for 2 seconds -- it must NEVER elect a leader (it can't reach a majority of votes):")
    for _ in range(4):
        time.sleep(0.5)
        minority_roles = {nid: cluster.raft(nid).status()["role"] for nid in minority}
        print(" ", minority_roles)
        assert all(r != "leader" for r in minority_roles.values())

    print("\nMajority side still accepts writes:")
    cluster.submit(("SET", "order:42", "in transit"))
    time.sleep(0.2)
    for nid in majority:
        print(f"  {nid}: {cluster.nodes[nid].kv.data.get('order:42')}")

    print("\nHealing the partition...")
    cluster.heal()
    cluster.submit(("SET", "order:42", "delivered"))
    time.sleep(0.3)
    print("Final state, every remaining node:")
    for nid, node in cluster.nodes.items():
        print(f"  {nid}: {node.kv.data.get('order:42')}")

    cluster.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
