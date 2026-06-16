"""The processor coroutine -- the sole parser and the sole owner of the dict.

It drains raw_queue continuously and is the only coroutine that parses JSON and
the only one (besides the scanner) that mutates ``entries``. For each raw item
(node_id, raw_timestamp_ns, raw_message) it:

  1. Parses the message and extracts the transaction hash. Anything that is not
     a well-formed log notification (subscription chatter, heartbeats, malformed
     frames) is dropped silently -- the listener forwards everything verbatim,
     so filtering lives here.
  2. Looks up or creates the transaction's five-slot entry.
  3. First-write-wins: writes the timestamp ONLY into an empty slot. A single
     trade emits several OrderFilled logs per node, so the same (tx, node) pair
     arrives repeatedly; the first timestamp is the true first-seen arrival, and
     later duplicates for an already-filled slot are dropped without overwriting.
  4. Threshold completion is checked ONLY when a slot actually transitions from
     empty to filled. This keeps "have enough DISTINCT nodes reported" honest --
     it counts filled slots, immune to duplicate inflation -- and avoids
     redundant checks on dropped duplicates. When the count reaches
     min_nodes_required, the entry is promoted to write_queue and removed.

The timeout scanner (separate coroutine) is the backstop that promotes entries
which never reach the threshold. A late duplicate arriving after an entry was
promoted-and-deleted creates a fresh single-slot entry (an orphan); that is
expected and resolved later by the cleaning stage's group-by-min, not here.

This coroutine is stopped on shutdown by cancellation; raw_queue and any
remaining entries are discarded, per the agreed shutdown policy. It performs no
``await`` between reading and mutating an entry, which (with the scanner's
snapshot sweep) is what makes the shared dict safe without a lock.
"""

from __future__ import annotations

import json

from ..schema import NUM_NODES
from .state import RunState


def extract_tx_hash(raw: "str | bytes") -> str | None:
    """Return the transaction hash from a logs subscription notification.

    Returns None for anything that is not a proper ``eth_subscription`` log
    notification carrying a string ``transactionHash`` -- those are dropped.
    Pure and total (never raises), so it is safe to call on every raw frame.
    """
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(msg, dict) or msg.get("method") != "eth_subscription":
        return None
    params = msg.get("params")
    if not isinstance(params, dict):
        return None
    result = params.get("result")
    if not isinstance(result, dict):
        return None
    tx = result.get("transactionHash")
    return tx if isinstance(tx, str) else None


async def run_processor(state: RunState, min_nodes_required: int) -> None:
    """Drain raw_queue forever, filing timestamps and promoting completed txs.

    ``min_nodes_required`` is the threshold trigger (default 5 = wait for all);
    the timeout scanner handles entries that never reach it. Runs until
    cancelled by the runner during shutdown.
    """
    # Bind hot-path references to locals once.
    get = state.raw_queue.get
    promote = state.write_queue.put_nowait
    entries = state.entries
    reports = state.counters.per_node_reports

    while True:
        node_id, timestamp_ns, raw = await get()

        tx = extract_tx_hash(raw)
        if tx is None:
            continue  # not a log notification; drop

        # Defensive: the listener only ever emits 1..NUM_NODES, but never let a
        # bad index crash the sole dict-owner.
        if not 1 <= node_id <= NUM_NODES:
            continue
        slot = node_id - 1

        entry = entries.get(tx)
        if entry is None:
            entry = [None] * NUM_NODES
            entries[tx] = entry

        if entry[slot] is None:
            # First-write-wins: record the genuine first-seen arrival.
            entry[slot] = timestamp_ns
            reports[slot] += 1

            # Threshold check runs ONLY on this real empty->filled transition.
            filled = NUM_NODES - entry.count(None)
            if filled >= min_nodes_required:
                promote((tx, entry))   # entry list handed off; not touched again
                del entries[tx]
        # else: duplicate log for an already-filled slot -> drop silently.