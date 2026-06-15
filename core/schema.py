"""Shared Arrow/Parquet schema for the RPC Node Latency Comparison Tool.

This module is the single source of truth for the on-disk data shape. It is
imported by every stage that reads or writes parquet -- the pipeline writer, the
cleaning stage, and (later) the analysis stage. Defining the schema in exactly
one place is what guarantees the writer and every reader can never drift out of
sync.

On-disk layout (fixed 6 columns):

    tx_hash             : string   transaction hash; the trade's identity
    node_1_arrival_ns   : int64?   offset-from-start nanoseconds, or null
    node_2_arrival_ns   : int64?
    node_3_arrival_ns   : int64?
    node_4_arrival_ns   : int64?
    node_5_arrival_ns   : int64?

A *null* in a node column means that node did not report the transaction. The
arrival columns are nullable int64; nulls must round-trip as true Arrow nulls
and never be coerced to 0 or NaN (the schema test verifies this explicitly,
since a silent coercion would turn "did not report" into "reported at t=0").

Both the raw parquet (written by the pipeline) and the processed parquet
(written by cleaning) use this exact schema. They differ only in an invariant
the schema cannot express: raw may contain duplicate tx_hash rows by design,
processed has one row per tx_hash. The cleaning stage enforces that separately.
"""

from __future__ import annotations

import pyarrow as pa

# Number of RPC node providers being compared. The schema width is fixed at this
# value, and the ordered [[nodes]] list in config.toml must contain exactly this
# many entries. It lives here, rather than in config, because the *columns* are
# the more fundamental constraint -- config validates itself against this.
NUM_NODES = 5

# Bumped only if the column layout itself ever changes. Stamped into each
# parquet file's metadata so a reader can detect an incompatible old file.
SCHEMA_VERSION = "1"

TX_HASH_COLUMN = "tx_hash"


def arrival_column(node_index: int) -> str:
    """Canonical column name for a 1-based node index.

    ``arrival_column(1)`` -> ``"node_1_arrival_ns"``. This is the *only* place
    arrival column names are constructed, so config (which maps providers to
    columns by position) and every other stage agree by construction.
    """
    if not 1 <= node_index <= NUM_NODES:
        raise ValueError(
            f"node_index must be in 1..{NUM_NODES}, got {node_index}"
        )
    return f"node_{node_index}_arrival_ns"


# Ordered arrival column names: node_1_arrival_ns ... node_5_arrival_ns.
ARRIVAL_COLUMNS: list[str] = [arrival_column(i) for i in range(1, NUM_NODES + 1)]

# All columns in canonical order: tx_hash first, then the five arrivals.
COLUMNS: list[str] = [TX_HASH_COLUMN, *ARRIVAL_COLUMNS]

# The shared Arrow schema. pa.int64() is nullable by default; a null is the
# encoding of "this node did not report this transaction".
SCHEMA: pa.Schema = pa.schema(
    [(TX_HASH_COLUMN, pa.string())]
    + [(name, pa.int64()) for name in ARRIVAL_COLUMNS]
)


def empty_table() -> pa.Table:
    """An empty table conforming to SCHEMA.

    Useful for edge cases (e.g. a run that produced zero rows) and for tests.
    """
    return SCHEMA.empty_table()


# --- Per-run parquet file metadata ----------------------------------------
#
# Provenance is stamped into the parquet footer at writer-open time. The values
# are run-specific (so they are supplied by the writer, not defined here), but
# the *keys* and the *encoding* are a shared contract and belong here, so the
# writer and any reader use identical strings and identical encode/decode logic.
#
# Arrow metadata is a bytes -> bytes mapping, hence the bytes keys.

META_START_REF_NS = b"start_ref_ns"        # raw time.monotonic_ns() at gate-open
META_RUN_STARTED_UTC = b"run_started_utc"  # ISO-8601 wall-clock run start
META_SCHEMA_VERSION = b"schema_version"    # value of SCHEMA_VERSION at write time


def build_run_metadata(start_ref_ns: int, run_started_utc: str) -> dict[bytes, bytes]:
    """Build the parquet file-metadata dict for a run.

    ``start_ref_ns`` is the raw monotonic clock value captured when the
    synchronized-start gate opened; storing it lets a reader reconstruct raw
    arrival times from the offset values in the columns if ever needed.
    ``run_started_utc`` is the human-readable wall-clock start (ISO-8601).
    """
    return {
        META_START_REF_NS: str(start_ref_ns).encode("utf-8"),
        META_RUN_STARTED_UTC: run_started_utc.encode("utf-8"),
        META_SCHEMA_VERSION: SCHEMA_VERSION.encode("utf-8"),
    }


def read_run_metadata(schema: pa.Schema) -> dict[str, object]:
    """Decode the run metadata from a parquet schema's footer metadata.

    Returns a dict with ``start_ref_ns`` (int), ``run_started_utc`` (str), and
    ``schema_version`` (str). Raises KeyError if the file was not written with
    run metadata. The inverse of :func:`build_run_metadata`.
    """
    md = schema.metadata or {}
    return {
        "start_ref_ns": int(md[META_START_REF_NS].decode("utf-8")),
        "run_started_utc": md[META_RUN_STARTED_UTC].decode("utf-8"),
        "schema_version": md[META_SCHEMA_VERSION].decode("utf-8"),
    }