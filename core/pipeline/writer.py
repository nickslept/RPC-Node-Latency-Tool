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

    def _ensure_writer_open(self) -> None:
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


    def _rows_to_table(self, rows: list[WriteItem]) -> pa.Table:
        """
        Converts a list of row-based ``WriteItem`` objects into a column-based PyArrow table.

        Returns: A PyArrow table with a ``tx_hash`` column and columns for each node's arrival times.
        """
        start_ref = self.state.start_ref_ns
        tx_col: list[str] = []
        node_cols: list[list] = [[] for _ in schema.ARRIVAL_COLUMNS]
        
        for tx, slots in rows:
            tx_col.append(tx)
            for node_index, arrival_timestamp in enumerate(slots):
                node_cols[node_index].append(None if arrival_timestamp is None else arrival_timestamp - start_ref)
        
        table = {schema.TX_HASH_COLUMN: tx_col}
        for col_name, col_data in zip(schema.ARRIVAL_COLUMNS, node_cols):
            table[col_name] = col_data
        return pa.table(table, schema=self._file_schema)


    def _write_batch(self, rows: list[WriteItem]) -> None:
        """
        Writes a batch of data to the parquet file, updates the state counter of total trades written, and calls ```_progress_line()``` to print a progress update.
        """
        self._ensure_writer_open()
        self._writer.write_table(self._rows_to_table(rows))
        self.state.counters.trades_written += len(rows)
        print(self._progress_line())


    def _add_to_buffer(self, item: WriteItem) -> None:
        """
        Adds a ``WriteItem`` object (previously pulled from the queue) to the buffer. If the buffer reaches or exceeds the batch size (limit), it will be written & the buffer will be cleared.
        """
        self.buffer.append(item)
        if len(self.buffer) >= self.batch_size:
            self._write_batch(self.buffer)
            self.buffer = []


    def _flush_remaining_data_and_close(self) -> None:
        """
        Flushes the remaining data in the buffer and closes the parquet file.
        """
        try:
            if self.buffer:
                self._write_batch(self.buffer)
                self.buffer = []
            elif self._writer is None and self.state.start_ref_ns is not None: # The run started but no data was written.
                self._ensure_writer_open()
        finally:
            if self._writer is not None:
                self._writer.close()
                self._writer = None


    def _progress_line(self) -> str:
        """
        Returns a progress update string.
        """
        state = self.state
        elapsed_seconds = (time.monotonic_ns() - state.start_ref_ns) / 1e9
        reports = ", ".join(
            f"node_{i + 1}={n}" for i, n in enumerate(state.counters.per_node_reports)
        )
        return (
            f"[WRITER UPDATE] Batch wrote successfully. Elapsed time: {elapsed_seconds:.1f}s | Total rows written: {state.counters.trades_written:,} | "
            f"Queued: raw={state.raw_queue.qsize()} write={state.write_queue.qsize()} | "
            f"Per-node reports: {reports}"
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
            sink._add_to_buffer(item)
    finally:
        sink._flush_remaining_data_and_close()