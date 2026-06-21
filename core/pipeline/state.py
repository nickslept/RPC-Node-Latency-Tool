"""
    Holds the mutable state (per run) for the ingestion pipeline.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from ..schema import NUM_NODES

# The 5 arrival time slots for a transaction hash, indexed from 0 to 4.
# int for times that have been recorded, None for times that have not yet been recorded.
Slots = list["int | None"]

# Transaction hash paired with its 5 arrival time slots.
WriteItem = tuple[str, Slots]


@dataclass
class Counters:
    """Running counter to give the user information about the current state of the pipeline. Printed to the console.

    ``per_node_reports[i]`` is the number of transactions node_{i+1} has reported (incremented by the processor).
    ``trades_written`` is the number of rows the writer has flushed to parquet.

    Queue depths are not tracked here, they are read live via qsize().
    """
    per_node_reports: list[int]
    trades_written: int = 0


@dataclass
class RunState:
    raw_queue: asyncio.Queue       # listeners -> processor (unbounded)
    write_queue: asyncio.Queue     # processor/scanner -> writer (unbounded)
    entries: dict[str, Slots]      # tx_hash -> Slots
    counters: Counters
    shutdown_event: asyncio.Event  # set by SIGINT/SIGTERM (or a disconnect depending on user config)
    start_recording: asyncio.Event # begins when start_ref_ns is captured; listeners begin sending data together
    start_ref_ns: int | None = None    # time when data collection begins
    run_started_utc: str | None = None  # wall-clock (ISO-8601) when data collection begins; for parquet metadata

    @classmethod
    def create(cls) -> "RunState":
        """Creates a new RunState instance."""
        return cls(
            raw_queue=asyncio.Queue(),     # unbounded
            write_queue=asyncio.Queue(),   # unbounded
            entries={},
            counters=Counters(per_node_reports=[0] * NUM_NODES),
            shutdown_event=asyncio.Event(),
            start_recording=asyncio.Event(),
            start_ref_ns=None,
            run_started_utc=None,
        )