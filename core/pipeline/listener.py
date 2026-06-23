from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from websockets.exceptions import ConnectionClosed

from ..config import NodeConfig

# node_id (indexed starting from 1), timestamp, raw message received by listener
RawItem = tuple[int, int, "str | bytes"]

# The necessary amount of time a node needs to not send messages for (AFTER recording has started) before messages start to get passed to the raw queue. 
# OrderFilled logs arrive in a predictable pattern, so this delay ensures no node has a head start.
PREGATE_DRAIN_QUIET_S = 0.05 #rename


@dataclass
class ListenerExit:
    """Returned when a listener stops because its connection closed mid-run.

    ``node`` is the node that disconnected.
    ``monotonic_ns`` is the raw clock value at the moment the disconnect was observed.
    ``reason`` is the reason for the disconnection.
    """

    node: NodeConfig
    monotonic_ns: int
    reason: str


async def _discard_until_quiet(websocket) -> None:
    """Waits until no message is received for ``PREGATE_DRAIN_QUIET_S`` seconds, discarding any messages received during that time. 

    Returns when either the timeout is reached or the connection closes.
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
    await _discard_until_quiet(websocket)

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