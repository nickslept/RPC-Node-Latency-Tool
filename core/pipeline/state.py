"""RunState -- the single shared mutable surface of the ingestion pipeline.

Several coroutines share state: the processor and the timeout scanner both
mutate the in-memory ``entries`` dict; the writer reads counters and queue
depths to print progress; the runner sets the coordination events. Rather than
scatter these and thread them through call sites individually, the runner builds
one RunState and injects it, so everything shared lives in exactly one place
that can be read and reasoned about together.

Why there is no lock, despite two coroutines mutating ``entries``:
asyncio is single-threaded cooperative multitasking -- only one coroutine runs
at a time, and control only passes at ``await`` points. The processor performs
each entry's create/fill/promote/delete with no ``await`` in the middle, and the
scanner sweeps over a ``list(entries.items())`` snapshot with no ``await`` in its
loop body. Because neither yields part-way through a multi-step mutation, the
other can never observe a half-updated structure. That invariant -- no await
inside a critical section over ``entries`` -- is what makes the lock-free design
correct, and it must be preserved by any future change to either coroutine.

Configuration is intentionally NOT stored here. Config is immutable and
conceptually separate; the runner passes the specific values each coroutine
needs (min_nodes_required, batch_size, timeouts) as explicit arguments. RunState
holds only runtime mutable state.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from ..schema import NUM_NODES

# One transaction's five node slots: raw time.monotonic_ns() per node, or None
# where that node has not (yet) reported. Index i corresponds to node_{i+1}
# (i.e. node_id - 1). The offset-from-start subtraction is deferred to the
# writer, so the values held here are RAW monotonic nanoseconds.
Slots = list["int | None"]

# What the processor and scanner promote onto write_queue and the writer
# consumes: the transaction hash paired with its (raw) slots.
WriteItem = tuple[str, Slots]


@dataclass
class Counters:
    """Running tallies for the console progress line (printed by the writer).

    ``per_node_reports[i]`` is the number of distinct transactions node_{i+1}
    has reported (incremented by the processor on each first slot fill).
    ``trades_written`` is the number of rows the writer has flushed to parquet.
    Queue depths are read live from the queues' qsize(), not tracked here.
    """

    per_node_reports: list[int]
    trades_written: int = 0


@dataclass
class RunState:
    raw_queue: asyncio.Queue       # listeners -> processor (unbounded; see Q on backpressure)
    write_queue: asyncio.Queue     # processor/scanner -> writer (unbounded)
    entries: dict[str, Slots]      # tx_hash -> slots; owned by processor, swept by scanner
    counters: Counters
    shutdown_event: asyncio.Event  # set by SIGINT/SIGTERM or a disconnect; runner tears down
    start_recording: asyncio.Event # opened once start_ref is captured; listeners begin together
    start_ref_ns: int | None = None    # raw monotonic clock at gate-open; set by the runner
    run_started_utc: str | None = None  # wall-clock (ISO-8601) at gate-open; for parquet metadata

    @classmethod
    def create(cls) -> "RunState":
        """Build a fresh RunState with empty, unbounded queues and clean state."""
        return cls(
            raw_queue=asyncio.Queue(),     # no maxsize -> unbounded
            write_queue=asyncio.Queue(),   # no maxsize -> unbounded
            entries={},
            counters=Counters(per_node_reports=[0] * NUM_NODES),
            shutdown_event=asyncio.Event(),
            start_recording=asyncio.Event(),
            start_ref_ns=None,
            run_started_utc=None,
        )