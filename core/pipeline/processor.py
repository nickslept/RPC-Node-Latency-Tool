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