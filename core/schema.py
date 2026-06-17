from __future__ import annotations

import pyarrow as pa

# The number of RPC node providers being compared.
NUM_NODES = 5

# Self-explanatory column name.
TX_HASH_COLUMN = "tx_hash"


def arrival_column(node_index: int) -> str:
    """
    Returns the arrival time column name given a node index.
    Raises ValueError if the node index is out of bounds.
    """
    if not 1 <= node_index <= NUM_NODES:
        raise ValueError(
            f"node_index must be in 1<=node_index<={NUM_NODES}, got {node_index}"
        )
    return f"node_{node_index}_arrival_ns"


# Ordered list of ARRIVAL TIME column names
ARRIVAL_COLUMNS: list[str] = [arrival_column(i) for i in range(1, NUM_NODES + 1)]

# All columns in order (tx_hash, then the arrival time columns)
COLUMNS: list[str] = [TX_HASH_COLUMN, *ARRIVAL_COLUMNS]

# PyArrow schema for the data
SCHEMA: pa.Schema = pa.schema(
    [(TX_HASH_COLUMN, pa.string())]
    + [(name, pa.int64()) for name in ARRIVAL_COLUMNS]
)


# --- Metadata ---

META_START_REF_NS = b"start_ref_ns"        # raw time.monotonic_ns() at gate-open
META_RUN_STARTED_UTC = b"run_started_utc"  # ISO-8601 wall-clock run start

def build_run_metadata(start_ref_ns: int, run_started_utc: str) -> dict[bytes, bytes]:
    """
    Builds and returns a dictionary containing the file metadata for a run.
    """
    return {
        META_START_REF_NS: str(start_ref_ns).encode("utf-8"),
        META_RUN_STARTED_UTC: run_started_utc.encode("utf-8"),
    }


def read_run_metadata(schema: pa.Schema) -> dict[str, object]:
    """
    Returns the decoded run metadata as a dictionary.
    """
    md = schema.metadata or {}
    return {
        "start_ref_ns": int(md[META_START_REF_NS].decode("utf-8")),
        "run_started_utc": md[META_RUN_STARTED_UTC].decode("utf-8"),
    }