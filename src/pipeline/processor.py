from __future__ import annotations

import json

from ..schema import NUM_NODES
from .state import RunState


def extract_tx_hash(raw: "str | bytes") -> str | None:
    """Extracts the transaction hash from a ``eth_subscribe()`` message.

    Parses the message as JSON to do so.

    Returns the transaction hash as a String, or None if it isn't found/the message doesn't match the expected format.
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
    """Fetches and removes messages from the raw queue. Updates ``entries`` dict with the arrival time data for each transaction. Increments the ``reports`` dict for each node.
    
    ``RunState`` is the shared state of the pipeline.
    ``min_nodes_required`` is the minimum number of nodes that must report for a transaction before it is promoted to the write queue.
    """
    
    # optimizes for speed by binding certain attributes to local variables.
    get = state.raw_queue.get
    promote = state.write_queue.put_nowait
    entries = state.entries
    reports = state.counters.per_node_reports

    while True:
        node_id, timestamp_ns, raw = await get() # gets & removes a raw message from the queue

        tx = extract_tx_hash(raw)
        if tx is None:
            continue  # not an eth_subscribe() message

        entry = entries.get(tx)
        if entry is None:
            entry = [None] * NUM_NODES
            entries[tx] = entry
        
        slot = node_id - 1
        if entry[slot] is None: # records only the first arrival for each node
            entry[slot] = timestamp_ns
            reports[slot] += 1
            # Checks if the minimum number of nodes have reported for this transaction
            filled = NUM_NODES - entry.count(None)
            if filled >= min_nodes_required:
                promote((tx, entry))
                del entries[tx]