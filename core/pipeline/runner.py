from __future__ import annotations

import asyncio
import functools
import os
import signal
import time
from datetime import datetime, timezone

from ..config import Config, NodeConfig
from .connect import PreflightError, close_all, open_all
from .disconnect_logger import DisconnectLogger
from .listener import ListenerExit, run_listener
from .processor import run_processor
from .scanner import run_scanner
from .state import RunState
from .writer import STOP_WRITER, run_writer, format_readable_time


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, state: RunState) -> None:
    """
    Installs signal handlers (SIGINT, SIGTERM) to trigger a graceful shutdown.
    """
    def handle(signum, frame):
        print("[SHUTDOWN] Stopping all processes and flushing data. WARNING: Pressing Ctrl+C again will force-quit and could corrupt the parquet file.")
        loop.call_soon_threadsafe(state.shutdown_event.set)
        signal.signal(signum, signal.SIG_DFL)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handle)

def _print_summary(state: RunState, output_path: str) -> None:
    total = state.counters.trades_written
    duration = (time.monotonic_ns() - state.start_ref_ns) // 1_000_000_000
    print(f"\n[SUMMARY] {total:,} trades written to {output_path}")
    print(f"[SUMMARY] Run time: {format_readable_time(duration)}")
    for i, reported in enumerate(state.counters.per_node_reports):
        rate = (reported / total * 100) if total else 0.0
        print(f"[SUMMARY] node_{i + 1}: {reported:,} trades reported | Rate: {rate:.1f}%")
    print("[SUMMARY] Note that the above numbers are the amount of trades REPORTED, not necessarily the amount of trades that were successfully processed & written. The rate is relative to the total number of trades written.")


def _disconnect_log_path(output_path: str) -> str:
    """
    Returns the path to the run's disconnect .txt file for the given output path as a string.
    """
    base, _ext = os.path.splitext(output_path)
    return base + ".disconnects.txt"


def _make_listener_handler(
    node: NodeConfig,
    logger: DisconnectLogger,
    state: RunState,
    stop_on_disconnect: bool,
):
    """
    Builds a callback function that handles the completion (for any reason, such as a disconnect) of a listener task.

    Returns a handler function for a listener task.
    """

    def handler(task: asyncio.Task) -> None:
        if task.cancelled():
            return # not a disconnect, normal shutdown
        exc = task.exception()
        if exc is not None:
            event = ListenerExit(
                node=node,
                monotonic_ns=time.monotonic_ns(),
                reason=f"Listener error: {exc!r}",
            )
        else:
            event = task.result()
            if event is None:
                return
        logger.log(event) # logs the disconnect in the .txt file
        if stop_on_disconnect and not state.shutdown_event.is_set():
            state.shutdown_event.set()

    return handler


async def run_ingestion(config: Config, output_path: str) -> int:
    """
    Runs the data ingestion pipeline. 
    
    Returns ``0`` on success, and ``1`` on pre-flight abort.
    """
    state = RunState.create()
    loop = asyncio.get_running_loop()

    # 1. Concurrently tries to establish a connection to each node.
    try:
        connections = await open_all(config)
    except PreflightError as exc:
        print(f"[CONNECTION FAILED] Couldn't establish a connection to every node. Reason: {exc}")
        return 1

    # 2. Ensures that a run can be stopped by a signal.
    _install_signal_handlers(loop, state)

    # 3. Captures the run's start time.
    state.start_ref_ns = time.monotonic_ns()
    state.run_started_utc = datetime.now(timezone.utc).isoformat()

    # 4. Creates the tasks for the writer, processor, and scanner.
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

    # 5. Sets up each listener with a disconnect logger.
    disconnect_logger = DisconnectLogger(_disconnect_log_path(output_path), state)
    stop_on_disconnect = config.connection.stop_on_disconnect
    listener_tasks = []
    for conn in connections:
        task = asyncio.create_task(
            run_listener(conn.node, conn.websocket, state.raw_queue, state.start_recording)
        )
        task.add_done_callback(
            _make_listener_handler(conn.node, disconnect_logger, state, stop_on_disconnect)
        )
        listener_tasks.append(task)

    try:
        print(f"[RECORDING] Attempting to start data collection for {len(connections)} nodes... Press Ctrl+C to stop the run.")
        
        # 6. Starts recording (every listener begins together)
        state.start_recording.set()
        await state.shutdown_event.wait()

        # 7. Shutdown the pipeline.
        producers = [*listener_tasks, processor_task, scanner_task]
        for task in producers:
            task.cancel()
        await asyncio.gather(*producers, return_exceptions=True)
        state.entries.clear()
        state.write_queue.put_nowait(STOP_WRITER)
        await writer_task
    finally:
        await close_all(connections)

    _print_summary(state, output_path)
    return 0


def run(config: Config, output_path: str) -> int:
    """
        Entry point (used by the CLI).
    """
    return asyncio.run(run_ingestion(config, output_path))