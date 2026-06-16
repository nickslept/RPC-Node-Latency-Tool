"""Durable record of mid-run node disconnects.

When a node drops, the listener returns a ListenerExit and the runner hands it
here. Each event is appended to a per-run ``.txt`` file (the durable record,
paired with the run's parquet in data/raw/) and echoed to the console so whoever
is watching sees it immediately.

Each line records both clocks: wall-clock UTC for human reading, and elapsed
seconds since the run's start_ref so the event can be lined up against the
arrival offsets stored in the parquet. A one-time header records the run start,
which is what makes those elapsed values interpretable. The header is written
lazily on the first disconnect, so a clean run with no drops leaves no file.

No reconnection is attempted anywhere -- by design. This module only records.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from .listener import ListenerExit
from .state import RunState


class DisconnectLogger:
    def __init__(self, path: str, state: RunState):
        self.path = path
        self.state = state
        self._header_written = False

    def _ensure_header(self) -> None:
        if self._header_written:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "a") as fh:
            fh.write(
                f"# run started {self.state.run_started_utc} "
                f"| start_ref_ns={self.state.start_ref_ns}\n"
            )
        self._header_written = True

    def log(self, event: ListenerExit) -> None:
        """Append one disconnect event and echo it to the console."""
        self._ensure_header()
        start_ref = self.state.start_ref_ns or event.monotonic_ns
        elapsed = (event.monotonic_ns - start_ref) / 1e9
        wall = datetime.now(timezone.utc).isoformat()
        node = event.node

        line = (
            f"{wall} | node_{node.index} ({node.name}) | DISCONNECT "
            f"| elapsed={elapsed:.3f}s | reason: {event.reason}"
        )
        with open(self.path, "a") as fh:
            fh.write(line + "\n")  # flushed on close -> durable per event

        print(
            f"[DISCONNECT] node_{node.index} ({node.name}) "
            f"after {elapsed:.3f}s: {event.reason}"
        )