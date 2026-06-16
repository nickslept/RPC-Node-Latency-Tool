"""The runner -- orchestrates one ingestion run from pre-flight to clean exit.

It is the only place the pipeline graph is assembled, the only place signals are
handled, and the only place the shutdown sequence lives. Everything it wires
together (connect, listeners, processor, scanner, writer) is already an
independently-trusted unit, so a fault here is a wiring fault, not a logic one.

Lifecycle:
  1. Pre-flight gate: connect+subscribe all nodes, all-ack-or-abort.
  2. Capture the synchronized start reference (raw monotonic + wall clock).
  3. Spawn the pipeline. The writer, processor, and scanner start immediately but
     idle (their queues are empty); the listeners block on the start gate.
  4. Open the gate so all listeners begin together, and run until a stop is
     requested (SIGINT/SIGTERM -> shutdown_event).
  5. Teardown, in strict flow order.

Teardown (the agreed policy -- discard recent partials, keep what was scheduled):
  a. Stop all producers (listeners, processor, scanner) by cancellation. Nothing
     new can enter the dict or be promoted after this.
  b. Discard the in-memory dict and any unprocessed raw items. Those are the
     most-recent, still-incomplete trades; writing them with Nones would inject
     a fast-node bias at the tail of every run, so they are dropped on purpose.
  c. Put WRITE_SENTINEL on write_queue and let the writer finish. It drains every
     already-promoted ("scheduled to write") row, forces the final sub-batch, and
     finalizes the parquet footer -- the close is guaranteed by the writer's own
     finally.
  d. Close the connections (last; in this module's finally, so it always runs).

A second Ctrl-C during teardown force-quits: the first signal sets the event and
removes the handler, so the next signal hits Python's default and kills.
"""

from __future__ import annotations

import asyncio
import functools
import signal
import time
from datetime import datetime, timezone

from ..config import Config
from ..connect import PreflightError, close_all, open_all
from .listener import run_listener
from .processor import run_processor
from .scanner import run_scanner
from .state import RunState
from .writer import WRITE_SENTINEL, run_writer


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, state: RunState) -> None:
    """Route SIGINT/SIGTERM to a one-shot graceful shutdown.

    The first signal sets shutdown_event and removes the handler, so a second
    signal falls through to the default disposition (force-quit).
    """

    def handle(sig: signal.Signals) -> None:
        print(f"\n[{sig.name}] stopping gracefully (Ctrl-C again to force-quit)")
        state.shutdown_event.set()
        try:
            loop.remove_signal_handler(sig)
        except (NotImplementedError, RuntimeError):
            pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, functools.partial(handle, sig))
        except NotImplementedError:  # e.g. Windows
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(state.shutdown_event.set))


def _print_summary(state: RunState, output_path: str) -> None:
    total = state.counters.trades_written
    duration = (
        (time.monotonic_ns() - state.start_ref_ns) / 1e9 if state.start_ref_ns else 0.0
    )
    print(f"\n[summary] {total:,} trades written to {output_path}")
    print(f"[summary] duration: {duration:.1f}s")
    for i, reported in enumerate(state.counters.per_node_reports):
        rate = (reported / total * 100) if total else 0.0
        print(f"[summary] node_{i + 1}: {reported:,} reported ({rate:.1f}%)")


async def run_ingestion(config: Config, output_path: str) -> int:
    """Run one ingestion to completion. Returns 0 on success, 1 on pre-flight abort."""
    state = RunState.create()
    loop = asyncio.get_running_loop()

    # 1. Pre-flight gate. open_all prints per-node status and, on any failure,
    #    closes the successful sockets before raising.
    try:
        connections = await open_all(config)
    except PreflightError as exc:
        print(f"\nABORT: {exc}")
        return 1

    _install_signal_handlers(loop, state)

    # 2. The synchronized start reference, captured once, shared by all.
    state.start_ref_ns = time.monotonic_ns()
    state.run_started_utc = datetime.now(timezone.utc).isoformat()

    # 3. Spawn the pipeline. Writer/processor/scanner start idle; listeners wait.
    writer_task = asyncio.create_task(
        run_writer(state, output_path, config.writer.batch_size)
    )
    processor_task = asyncio.create_task(
        run_processor(state, config.completion.min_nodes_required)
    )
    scanner_task = asyncio.create_task(
        run_scanner(
            state,
            config.completion.timeout_seconds,
            config.completion.scanner_interval_seconds,
        )
    )
    listener_tasks = [
        asyncio.create_task(
            run_listener(conn.node, conn.websocket, state.raw_queue, state.start_recording)
        )
        for conn in connections
    ]

    try:
        print(f"recording started ({len(connections)} nodes) -- Ctrl-C to stop")
        # 4. Open the gate: every listener begins together.
        state.start_recording.set()
        await state.shutdown_event.wait()

        # 5. Teardown.
        print("shutdown requested -- draining scheduled writes and finalizing...")

        # (a) Stop all producers.
        producers = [*listener_tasks, processor_task, scanner_task]
        for task in producers:
            task.cancel()
        await asyncio.gather(*producers, return_exceptions=True)
        # NOTE: Stage 7 will inspect listener results here for disconnects.

        # (b) Discard the dict (recent incomplete trades dropped by design).
        state.entries.clear()

        # (c) Drain everything already promoted, then let the writer finalize.
        state.write_queue.put_nowait(WRITE_SENTINEL)
        await writer_task
    finally:
        # (d) Connections closed last, unconditionally.
        await close_all(connections)

    _print_summary(state, output_path)
    return 0


def run(config: Config, output_path: str) -> int:
    """Synchronous entry point (used by the CLI)."""
    return asyncio.run(run_ingestion(config, output_path))