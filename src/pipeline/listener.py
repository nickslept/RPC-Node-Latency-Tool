from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from websockets.exceptions import ConnectionClosed

from ..config import NodeConfig

# node_id (indexed starting from 1), timestamp, raw message received by listener
RawItem = tuple[int, int, "str | bytes"]

# The necessary amount of time a node needs to not send messages for (AFTER recording has started) before messages start to get passed to the raw queue. 
# eth_subscribe() messages arrive in a predictable pattern, so this delay ensures no node has a head start.
QUIET_INTERVAL_SECONDS = 0.05


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
    """Waits until no message is received for ``QUIET_INTERVAL_SECONDS`` seconds, discarding any messages received during that time. 

    Returns when either the timeout is reached or the connection closes.
    """
    while True:
        try:
            await asyncio.wait_for(websocket.recv(), timeout=QUIET_INTERVAL_SECONDS)
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
    """Runs one node's listener until the connection closes or is cancelled.

    Waits for the start recording event, then discards messages for a short period to ensure no node has a head start.
    Captures timestamps of incoming messages, and moves them to the raw queue.

    Returns a :class:`ListenerExit` if the connection unexpectedly closes mid-run; otherwise, returns None (even if the run is properly ended via Ctrl+C).
    """
    await start_recording.wait()
    await _discard_until_quiet(websocket)

    # optimizes for speed by binding certain attributes to local variables.
    node_id = node.index
    recv = websocket.recv
    monotonic_ns = time.monotonic_ns
    put = raw_queue.put_nowait

    try:
        while True:
            raw = await recv()
            ts = monotonic_ns()
            put((node_id, ts, raw))
    except ConnectionClosed as exc:
        return ListenerExit(
            node=node,
            monotonic_ns=time.monotonic_ns(),
            reason=str(exc) or type(exc).__name__,
        )