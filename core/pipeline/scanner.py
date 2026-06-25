from __future__ import annotations

import asyncio
import time

from .state import RunState


async def run_scanner(
    state: RunState,
    timeout_seconds: float,
    interval_seconds: float,
) -> None:
    """Sweep ``entries`` every ``interval_seconds``, promoting aged-out entries."""
    timeout_ns = int(timeout_seconds * 1_000_000_000)
    entries = state.entries
    promote = state.write_queue.put_nowait

    while True:
        # The ONLY await. Everything below is synchronous and atomic w.r.t. the
        # processor, preserving the lock-free invariant over `entries`.
        await asyncio.sleep(interval_seconds)

        now = time.monotonic_ns()
        for tx, slots in list(entries.items()):   # snapshot -> safe inline delete
            earliest = min((s for s in slots if s is not None), default=None)
            if earliest is None:
                continue  # cannot occur in practice (entries always have >=1 fill)
            if now - earliest > timeout_ns:
                promote((tx, slots))   # written with its None slots intact
                del entries[tx]