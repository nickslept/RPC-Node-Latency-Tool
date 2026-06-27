from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from .. import schema
from .state import RunState, WriteItem

# Object placed in the write queue to signal that shutdown has started. Used like a boolean flag.
STOP_WRITER: object = object()


class _ParquetSink:
    """Encapsulates the held-open writer, the row buffer, and lifecycle."""

    def __init__(self, output_path: str, state: RunState, batch_size: int):
        self.path = output_path
        self.state = state
        self.batch_size = batch_size
        self.buffer: list[WriteItem] = []
        self._writer: pq.ParquetWriter | None = None
        self._file_schema: pa.Schema | None = None

    # --- helpers ---

    def _open_writer(self) -> None:
        """
        Opens the parquet writer (building the file metadata, schema, and output path + keeps the writer open) IF it is not already open.
        """
        if self._writer is not None: # already open
            return

        if self.state.start_ref_ns is None:
            raise RuntimeError("Writer was opened before start_ref was set by the runner.")
        if self.state.run_started_utc is None:
            raise RuntimeError("Writer was opened before run_started_utc was set by the runner.")
        
        meta = schema.build_run_metadata(self.state.start_ref_ns, self.state.run_started_utc)
        self._file_schema = schema.SCHEMA.with_metadata(meta)
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self._writer = pq.ParquetWriter(self.path, self._file_schema)


    def _build_table(self, rows: list[WriteItem]) -> pa.Table:
        start_ref = self.state.start_ref_ns  # not None once _open_writer ran
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
        self._open_writer()
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
                self._open_writer()
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
    when it sees STOP_WRITER. The ``finally`` guarantees the final partial
    flush and the footer-finalizing close run regardless of how the loop ends.
    """
    sink = _ParquetSink(output_path, state, batch_size)
    get = state.write_queue.get
    try:
        while True:
            item = await get()
            if item is STOP_WRITER:
                break
            sink.add(item)
    finally:
        sink.finalize()