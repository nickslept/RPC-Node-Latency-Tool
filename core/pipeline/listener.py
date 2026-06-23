from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from websockets.exceptions import ConnectionClosed

from ..config import NodeConfig

# The item shape carried on raw_queue. The processor is the sole consumer and
# the sole parser; node_id is the 1-based node index, ts is a RAW
# time.monotonic_ns() value (the offset-from-start subtraction happens later, at
# the write edge), and raw is the unparsed JSON-RPC message exactly as received.
RawItem = tuple[int, int, "str | bytes"]


# After the gate opens, keep discarding buffered frames until none has arrived
# for this long, then start recording. Polymarket OrderFilled logs arrive in
# per-block bursts roughly two seconds apart, so a window well under that
# reliably clears the single pre-gate burst (if any) and then falls quiet long
# before the next real burst -- it does not eat live trades.
PREGATE_DRAIN_QUIET_S = 0.05


@dataclass
class ListenerExit:
    """Returned when a listener stops because its connection closed mid-run.

    The runner inspects this to drive disconnect logging and the optional
    stop-on-disconnect behaviour (wired in Stage 7). monotonic_ns is the raw
    clock value at the moment the close was observed.
    """

    node: NodeConfig
    monotonic_ns: int
    reason: str


async def _discard_pregate_backlog(websocket) -> None:
    """Drain and discard frames buffered before the recording gate opened.

    Reads until no frame arrives for PREGATE_DRAIN_QUIET_S, then returns.
    Cancelling a waiting recv() does not consume a message, so no live frame is
    lost by the timeout; a frame that was already buffered is returned (and
    discarded) before the timeout can fire. If the socket closes during the
    drain, we simply stop -- the main loop's next recv() will surface the close.
    """
    while True:
        try:
            await asyncio.wait_for(websocket.recv(), timeout=PREGATE_DRAIN_QUIET_S)
        except asyncio.TimeoutError:
            return
        except ConnectionClosed:
            return


async def run_listener(
    node: NodeConfig,
    websocket,
    raw_queue: asyncio.Queue,
    start_recording: asyncio.Event,
) -> ListenerExit | None:
    """Run one node's listener until the connection closes or it is cancelled.

    Blocks on ``start_recording`` so all listeners begin together, optionally
    drains the pre-gate backlog, then runs the lean capture loop. Returns a
    :class:`ListenerExit` if the connection closes mid-run; propagates
    CancelledError untouched when the runner cancels it during graceful
    shutdown (which is the normal stop path).
    """
    await start_recording.wait()
    await _discard_pregate_backlog(websocket)

    # Bind to locals once: no attribute lookups in the hot path between a
    # message surfacing and its timestamp being taken.
    node_id = node.index
    recv = websocket.recv
    monotonic_ns = time.monotonic_ns
    put = raw_queue.put_nowait

    try:
        while True:
            raw = await recv()
            ts = monotonic_ns()          # FIRST thing after recv -- never reorder
            put((node_id, ts, raw))      # unbounded queue: cannot block or raise
    except ConnectionClosed as exc:
        # Expected when a node drops. Capture when we noticed and hand it back;
        # Stage 7 turns this into a disconnect log line and, if configured, a
        # graceful stop of the whole run.
        return ListenerExit(
            node=node,
            monotonic_ns=time.monotonic_ns(),
            reason=str(exc) or type(exc).__name__,
        )