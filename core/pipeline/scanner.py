"""The timeout scanner coroutine -- the completion backstop.

The processor promotes a transaction only when MIN_NODES_REQUIRED distinct nodes
have reported. Any transaction that never reaches that threshold -- because one
or more nodes were slow or never saw it -- would otherwise sit in the dict
forever. The scanner is the guarantee that every entry eventually leaves: on a
periodic sweep it promotes entries that have aged past TIMEOUT_SECONDS, with
their unfilled slots left as None.

Mechanics, matching the agreed design:

* Age is measured from each entry's EARLIEST recorded timestamp -- the first
  node that saw the transaction. That makes "this trade has been visible for
  TIMEOUT seconds and some nodes still haven't reported" the precise condition,
  and prevents a trickle of late nodes from resetting the clock and letting an
  entry evade timeout indefinitely.

* The comparison is raw-to-raw: ``now`` comes from time.monotonic_ns(), the same
  raw clock the slots hold (the offset-from-start subtraction happens later, in
  the writer). Comparing against an offset clock here would make everything look
  ages old and promote instantly.

* Timeout-promoted entries are written WITH their None slots intact. Unlike the
  shutdown path -- which discards incomplete entries because they are merely
  recent trades whose slow nodes haven't had time yet -- a timed-out entry has
  had a full, generous window, so a still-empty slot is a genuine "did not
  report" observation worth recording.

* The sweep iterates a ``list(entries.items())`` snapshot so entries can be
  deleted inline in a single pass. Critically, the sweep body contains NO
  ``await``: the only await is the sleep between sweeps. That is what lets the
  scanner and the processor share ``entries`` without a lock -- neither yields
  part-way through mutating it.

Stopped on shutdown by cancellation (the sleep is the cancellation point; a
sweep, being await-free, always runs to completion atomically).
"""

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