"""Real TCP sockets between nodes, length-prefixed JSON messages, one
connection per RPC. Nothing here is simulated at the message level --
every RPC is an actual socket send/recv across localhost between real
threads. What IS simulated is whether a link is allowed to carry traffic
at all: `Network.partition()` marks a pair of nodes as unreachable, and
both the sending side (before even opening a connection) and the
receiving side (after accepting one, in case a partition changed mid-
flight) check it. From Raft's perspective this looks exactly like a real
network partition -- messages across a cut link simply never arrive.
"""
from __future__ import annotations

import json
import socket
import struct
import threading


class Network:
    """Shared across every node in a test: which pairs currently cannot
    reach each other."""

    def __init__(self):
        self._lock = threading.Lock()
        self._cut = set()  # set of frozenset({node_a, node_b})

    def partition(self, group_a, group_b):
        """Cuts every link between a node in group_a and a node in group_b."""
        with self._lock:
            for a in group_a:
                for b in group_b:
                    if a != b:
                        self._cut.add(frozenset((a, b)))

    def heal(self):
        with self._lock:
            self._cut.clear()

    def is_connected(self, a, b) -> bool:
        if a == b:
            return True
        with self._lock:
            return frozenset((a, b)) not in self._cut


def _send_message(conn: socket.socket, obj: dict):
    data = json.dumps(obj).encode("utf-8")
    conn.sendall(struct.pack(">I", len(data)) + data)


def _recv_exact(conn: socket.socket, n: int):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def _recv_message(conn: socket.socket):
    header = _recv_exact(conn, 4)
    if header is None:
        return None
    (length,) = struct.unpack(">I", header)
    body = _recv_exact(conn, length)
    if body is None:
        return None
    return json.loads(body.decode("utf-8"))


class RpcServer:
    def __init__(self, node_id, port: int, handler, network: Network):
        self.node_id = node_id
        self.port = port
        self.handler = handler  # callable(msg: dict) -> dict
        self.network = network
        self._sock = None
        self._thread = None
        self._running = False

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", self.port))
        self._sock.listen(32)
        self._sock.settimeout(0.2)
        self._running = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while self._running:
            try:
                conn, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _handle_conn(self, conn: socket.socket):
        try:
            msg = _recv_message(conn)
            if msg is None:
                return
            sender_id = msg.get("_from")
            if sender_id is not None and not self.network.is_connected(sender_id, self.node_id):
                return  # partitioned: drop silently, exactly like a real cut link would
            response = self.handler(msg)
            _send_message(conn, response)
        except (OSError, ValueError):
            pass
        finally:
            conn.close()

    def stop(self):
        self._running = False
        if self._sock:
            try:
                self._sock.close()
            except OSError:
                pass
        if self._thread:
            self._thread.join(timeout=1)


def send_rpc(from_id, to_addr, to_id, network: Network, message: dict, timeout: float = 0.5):
    """Returns the response dict, or None on any failure (partitioned,
    connection refused, timeout, malformed response) -- Raft treats a
    missing response identically to a partitioned link, which is
    realistic: a real caller can't tell the difference between "the
    network is down" and "the reply got lost" either."""
    if not network.is_connected(from_id, to_id):
        return None
    payload = dict(message)
    payload["_from"] = from_id
    try:
        with socket.create_connection(to_addr, timeout=timeout) as conn:
            conn.settimeout(timeout)
            _send_message(conn, payload)
            return _recv_message(conn)
    except (OSError, socket.timeout, ValueError):
        return None
