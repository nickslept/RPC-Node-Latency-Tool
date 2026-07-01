from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from .listener import ListenerExit
from .state import RunState


class DisconnectLogger:
    def __init__(self, path: str, state: RunState):
        self.path = path
        self.state = state
        self._header_written = False

    def _ensure_header(self) -> None:
        """
        Ensures that the header is written to the log file. If it isn't, then the header is written.
        """
        if self._header_written:
            return
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        with open(self.path, "a") as fh:
            fh.write(
                f"Run started: {self.state.run_started_utc} "
                f"| start_ref_ns={self.state.start_ref_ns}\n"
            )
        self._header_written = True

    def log(self, event: ListenerExit) -> None:
        """
        Adds one disconnect event to the log file and prints it to the console.
        """
        self._ensure_header()
        elapsed_seconds = (time.monotonic_ns() - self.state.start_ref_ns) // 1_000_000_000
        curr_time = datetime.now(timezone.utc).isoformat()
        node = event.node

        line = (
            f"{curr_time} | node_{node.index} ({node.name}) | DISCONNECT "
            f"| Elapsed seconds: {elapsed_seconds}s | Reason: {event.reason}"
        )
        with open(self.path, "a") as fh:
            fh.write(line + "\n")

        print(
            f"[DISCONNECT] node_{node.index} ({node.name}) "
            f"after {elapsed_seconds:.3f}s: {event.reason}"
        )