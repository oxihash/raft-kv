"""The replicated log. Indexed 1-based throughout, matching the Raft
paper exactly (index 0 means "before the first entry", with term 0),
which keeps every comparison in node.py a direct transcription of the
paper's rules instead of an off-by-one translation exercise.

Supports compaction via a snapshot: once every node has applied entries
up to some index, those entries can be discarded and replaced by a
single snapshot of the state machine at that point. After compaction,
`entries[0]` no longer corresponds to log index 1, so every index-based
access goes through `_pos()` to translate.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LogEntry:
    term: int
    command: object


class Log:
    def __init__(self):
        self.entries: list = []
        self.snapshot_index = 0   # the log index of the last entry folded into the snapshot (0 = no snapshot yet)
        self.snapshot_term = 0
        self.snapshot_data = None

    def _pos(self, index: int) -> int:
        """Translates a 1-based log index into a position in `entries`,
        accounting for however much has already been compacted away."""
        return index - self.snapshot_index - 1

    def last_index(self) -> int:
        return self.snapshot_index + len(self.entries)

    def last_term(self) -> int:
        if self.entries:
            return self.entries[-1].term
        return self.snapshot_term

    def get(self, index: int):
        if index <= self.snapshot_index:
            return None  # already compacted away -- caller should be using the snapshot instead
        pos = self._pos(index)
        if pos < 0 or pos >= len(self.entries):
            return None
        return self.entries[pos]

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if index == self.snapshot_index:
            return self.snapshot_term
        entry = self.get(index)
        return entry.term if entry else 0

    def append(self, entries: list):
        self.entries.extend(entries)

    def truncate_from(self, index: int):
        """Removes every entry from `index` (1-based) onward -- used when
        a follower's log conflicts with the leader's and has to be
        overwritten."""
        pos = self._pos(index)
        if pos < 0:
            pos = 0
        self.entries = self.entries[:pos]

    def slice_from(self, index: int) -> list:
        """Entries from `index` (1-based) to the end."""
        pos = max(self._pos(index), 0)
        return self.entries[pos:]

    def has_entries_before(self, index: int) -> bool:
        """True if `index` refers to something already folded into the
        snapshot (so the leader must send a snapshot instead of trying to
        replicate individual entries)."""
        return index <= self.snapshot_index

    def compact(self, up_to_index: int, state_machine_snapshot):
        """Discards every entry up to and including `up_to_index`,
        replacing them with a snapshot of the application state at that
        point. `up_to_index` must not exceed what's actually been
        committed and applied -- compacting an uncommitted entry would
        silently lose it forever."""
        if up_to_index <= self.snapshot_index:
            return
        term = self.term_at(up_to_index)
        pos = self._pos(up_to_index)
        self.entries = self.entries[pos + 1:]
        self.snapshot_index = up_to_index
        self.snapshot_term = term
        self.snapshot_data = state_machine_snapshot
