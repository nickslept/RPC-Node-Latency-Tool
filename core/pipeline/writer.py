"""The writer coroutine -- the only coroutine that writes parquet.

It drains write_queue, accumulates promoted transactions into a buffer, and
writes a row group every ``batch_size`` rows into a single held-open
ParquetWriter. The held-open-writer design was chosen deliberately (accepting
the hard-crash exposure: anything not flushed and footer-finalized before a
clean close is unrecoverable), so the single most important guarantee in this
module is that ``close()`` runs -- it is in a ``finally`` that executes even if
the final write raises.

Two transformations happen here, both at the write edge and nowhere else:

* Offset subtraction. Slots hold RAW monotonic nanoseconds; the stored columns
  hold ``arrival - start_ref``. Doing it here keeps the listener hot path free
  of arithmetic, and it is exact integer subtraction, so cross-node differences
  are preserved perfectly.

* Provenance. The run's start_ref and wall-clock start are stamped into the
  parquet file metadata (via the shared schema), so the file is self-describing
  and raw timestamps can be reconstructed if ever needed.

Lifecycle / shutdown: the runner signals end-of-stream by putting WRITE_SENTINEL
on write_queue. The writer drains everything ahead of it, then ``finalize()``
forces a final write of the sub-batch still in the buffer (otherwise up to
batch_size-1 trades would be silently lost) and closes the file. The
ParquetWriter is opened lazily on first write, by which point start_ref is set;
a run that recorded but produced zero rows still emits a valid, empty,
footer-bearing file so downstream stages can open it.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from .. import schema
from .state import RunState, WriteItem

# End-of-stream marker placed on write_queue by the runner at shutdown. A unique
# object (not None) so it can never be confused with a real WriteItem.
WRITE_SENTINEL: object = object()


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class _ParquetSink:
    """Encapsulates the held-open writer, the row buffer, and lifecycle."""

    def __init__(self, output_path: str, state: RunState, batch_size: int):
        self.path = output_path
        self.state = state
        self.batch_size = batch_size
        self.buffer: list[WriteItem] = []
        self._writer: pq.ParquetWriter | None = None
        self._file_schema: pa.Schema | None = None

    # --- writing -----------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._writer is not None:
            return
        start_ref = self.state.start_ref_ns
        if start_ref is None:
            # Writing before the gate opened would be a logic error; the lazy
            # open only ever happens after recording (and thus start_ref) began.
            raise RuntimeError("writer opened before start_ref was set")
        run_utc = self.state.run_started_utc or _now_utc_iso()
        meta = schema.build_run_metadata(start_ref, run_utc)
        # Both the writer schema and every batch table use this exact
        # metadata-bearing schema, so they are identical and can never mismatch.
        self._file_schema = schema.SCHEMA.with_metadata(meta)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._writer = pq.ParquetWriter(self.path, self._file_schema)

    def _build_table(self, rows: list[WriteItem]) -> pa.Table:
        start_ref = self.state.start_ref_ns  # not None once _ensure_open ran
        tx_col: list[str] = []
        node_cols: list[list] = [[] for _ in schema.ARRIVAL_COLUMNS]
        for tx, slots in rows:
            tx_col.append(tx)
            for i, s in enumerate(slots):
                node_cols[i].append(None if s is None else s - start_ref)
        arrays = {schema.TX_HASH_COLUMN: tx_col}
        for name, col in zip(schema.ARRIVAL_COLUMNS, node_cols):
            arrays[name] = col
        return pa.table(arrays, schema=self._file_schema)

    def _write_and_report(self, rows: list[WriteItem]) -> None:
        self._ensure_open()
        self._writer.write_table(self._build_table(rows))
        self.state.counters.trades_written += len(rows)
        print(self._progress_line())

    def add(self, item: WriteItem) -> None:
        self.buffer.append(item)
        if len(self.buffer) >= self.batch_size:
            self._write_and_report(self.buffer)
            self.buffer = []

    # --- shutdown ----------------------------------------------------------

    def finalize(self) -> None:
        """Flush the final sub-batch and close the file. Close is guaranteed."""
        try:
            if self.buffer:
                self._write_and_report(self.buffer)  # forced partial write
                self.buffer = []
            elif self._writer is None and self.state.start_ref_ns is not None:
                # Recorded but produced no rows: still emit a valid empty file.
                self._ensure_open()
        finally:
            if self._writer is not None:
                self._writer.close()   # finalizes the footer; THE critical line
                self._writer = None

    # --- console -----------------------------------------------------------

    def _progress_line(self) -> str:
        state = self.state
        elapsed_s = (time.monotonic_ns() - state.start_ref_ns) / 1e9
        reports = " ".join(
            f"node_{i + 1}={n}" for i, n in enumerate(state.counters.per_node_reports)
        )
        return (
            f"[+{elapsed_s:.1f}s] wrote batch \u2192 "
            f"{state.counters.trades_written:,} trades total | "
            f"queued: raw={state.raw_queue.qsize()} write={state.write_queue.qsize()} | "
            f"reported: {reports}"
        )


async def run_writer(state: RunState, output_path: str, batch_size: int) -> None:
    """Drain write_queue into a held-open parquet file until the sentinel.

    The writer is the one coroutine NOT cancelled at shutdown; it exits cleanly
    when it sees WRITE_SENTINEL. The ``finally`` guarantees the final partial
    flush and the footer-finalizing close run regardless of how the loop ends.
    """
    sink = _ParquetSink(output_path, state, batch_size)
    get = state.write_queue.get
    try:
        while True:
            item = await get()
            if item is WRITE_SENTINEL:
                break
            sink.add(item)
    finally:
        sink.finalize()