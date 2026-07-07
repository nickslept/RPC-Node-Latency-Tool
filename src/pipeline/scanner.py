from __future__ import annotations

import asyncio
import time

from .state import RunState


async def run_scanner(
    state: RunState,
    timeout_seconds: float,
    interval_seconds: float,
) -> None:
    """
    Checks ``entries`` every ``interval_seconds``, promoting entries whose time between the first reported timestamp and the current time is greater than ``timeout_seconds``.
    """
    timeout_ns = int(timeout_seconds * 1_000_000_000)
    entries = state.entries
    promote = state.write_queue.put_nowait

    while True:
        await asyncio.sleep(interval_seconds)
        for tx, slots in list(entries.items()):
            now = time.monotonic_ns()
            earliest = min((timestamp for timestamp in slots if timestamp is not None), default=None)
            if earliest is None:
                continue  # shouldn't ever occur but avoids errors (entries always have >=1 arrival timestamp)
            if now - earliest > timeout_ns:
                promote((tx, slots))
                del entries[tx]