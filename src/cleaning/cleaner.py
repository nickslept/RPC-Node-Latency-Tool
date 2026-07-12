from __future__ import annotations

import os
import shutil

import polars as pl

from .. import schema

# run metadata (to carry over into cleaned file)
_RUN_METADATA_KEYS = {
    key.decode("utf-8")
    for key in (
        schema.META_START_REF_NS,
        schema.META_END_REF_NS,
        schema.META_RUN_START_UTC,
        schema.META_NODE_PROVIDERS,
    )
}


def _generate_cleaned_path(input_path: str, processed_dir: str) -> str:
    """
    Returns the output path for a cleaned file: ``PROCESSED_DIR`` with ``cleaned_`` in front of the original filename.
    """
    return os.path.join(processed_dir, f"cleaned_{os.path.basename(input_path)}")


def _read_run_metadata(input_path: str) -> dict[str, str]:
    """
    Reads the run's metadata from the raw parquet file so it can be carried over to the cleaned file.

    Returns only the keys defined in ``schema`` (ignores other keys such as ARROW:schema).
    """
    metadata = pl.read_parquet_metadata(input_path)
    return {key: value for key, value in metadata.items() if key in _RUN_METADATA_KEYS}


def run_cleaning(input_path: str, processed_dir: str) -> int:
    """
    Cleans the parquet file passed in via ``input_path`` and saves the result in ``processed_dir``.

    If the file has no duplicate tx_hashes, it is moved & renamed as-is. Otherwise duplicates are
    removed by only keeping the minimum (earliest) non-None arrival time per node and a new file is written. 
    Files that have already been cleaned (checked via filename in case cleaning logic is updated in the future) are skipped.

    Returns ``0`` on success.
    """
    output_path = _generate_cleaned_path(input_path, processed_dir)
    if os.path.exists(output_path):
        print(f"[ERROR] Selected file has already been cleaned: {output_path}")
        return 0
    os.makedirs(processed_dir, exist_ok=True)

    df = pl.read_parquet(input_path)
    total_tx_hash = df.height
    total_unique_tx_hash = df[schema.TX_HASH_COLUMN].n_unique()
    duplicates = total_tx_hash - total_unique_tx_hash

    if duplicates == 0:
        shutil.move(input_path, output_path)
        print(f"[INFO] No duplicate tx_hashes found within {total_tx_hash:,} rows. File has been moved and renamed to: {output_path}")
        return 0

    arrival_time_cols = [col for col in df.columns if col != schema.TX_HASH_COLUMN]
    cleaned_data = df.group_by(schema.TX_HASH_COLUMN, maintain_order=True).agg(
        pl.col(arrival_time_cols).min()
    )
    cleaned_data.write_parquet(output_path, metadata=_read_run_metadata(input_path))
    print(
        f"[INFO] Removed {duplicates:,} duplicate tx_hashes. Cleaned file went from {total_tx_hash:,} to {cleaned_data.height:,} rows. "
        f"Saved to: {output_path}"
    )
    return 0
