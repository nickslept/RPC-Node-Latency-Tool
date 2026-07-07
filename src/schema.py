from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq

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

META_START_REF_NS = b"start_ref_ns"      # raw time.monotonic_ns() once recording actually starts
META_END_REF_NS = b"end_ref_ns"          # raw time.monotonic_ns() when shutdown is triggered
META_RUN_START_UTC = b"run_start_utc"    # ISO-8601 wall-clock time when recording actually starts
META_NODE_PROVIDERS = b"node_providers"  # JSON mapping of column -> provider (e.g. {"node_1": "alchemy"})


def build_start_metadata(
    start_ref_ns: int, run_start_utc: str, node_names: tuple[str, ...]
) -> dict[bytes, bytes]:
    """
    Builds and returns the file metadata that can be determined at the start of a run.

    ``start_ref_ns`` is the raw time.monotonic_ns() when recording actually starts.
    ``run_start_utc`` is the ISO-8601 wall-clock time when recording actually starts.
    ``node_names`` is the ordered tuple of provider names from ``config.toml``.
    """
    providers = {f"node_{i}": name for i, name in enumerate(node_names, start=1)}
    return {
        META_START_REF_NS: str(start_ref_ns).encode("utf-8"),
        META_RUN_START_UTC: run_start_utc.encode("utf-8"),
        META_NODE_PROVIDERS: json.dumps(providers).encode("utf-8"),
    }


def build_end_metadata(end_ref_ns: int) -> dict[bytes, bytes]:
    """
    Builds and returns the file metadata that can ONLY be determined at the end of a run. 
    
    ``end_ref_ns`` is the raw time.monotonic_ns() when shutdown is triggered.
    """
    return {META_END_REF_NS: str(end_ref_ns).encode("utf-8")}


def read_file_metadata(path: str) -> dict[str, object]:
    """
    Reads a parquet file's metadata after a run is completed.

    Returns a dictionary with ``start_ref_ns`` (int), ``end_ref_ns`` (int | None),
    ``run_start_utc`` (str), and ``node_providers`` (dict[str, str]).
    """
    md = pq.ParquetFile(path).metadata.metadata or {}
    end_ref = md.get(META_END_REF_NS)
    return {
        "start_ref_ns": int(md[META_START_REF_NS].decode("utf-8")),
        "end_ref_ns": int(end_ref.decode("utf-8")) if end_ref is not None else None,
        "run_start_utc": md[META_RUN_START_UTC].decode("utf-8"),
        "node_providers": json.loads(md[META_NODE_PROVIDERS].decode("utf-8")),
    }