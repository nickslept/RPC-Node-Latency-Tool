from __future__ import annotations

import polars as pl

from .. import schema

# Label for transactions that a node never reported on the rankings chart (DNR = "did not report").
DNR_LABEL = "DNR"


def get_provider_order(providers: dict[str, str]) -> list[str]:
    """
    Returns a list of Strings containing the provider names ordered by node number (node_1's provider 1st, node_2's provider 2nd, etc).

    ``providers`` is the metadata mapping of column prefix -> provider name (e.g. {"node_1": "alchemy"}).
    """
    return [providers[f"node_{i}"] for i in range(1, len(providers) + 1)]


def build_offset_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Takes in the cleaned parquet file's dataframe and creates a new dataframe with the following changes:
    ``tx_hash`` remains the same.
    ``min_arrival_ns`` column created for the earliest arrival time in ns for a specific transaction.
    All arrival time columns are converted into offsets from the fastest arrival time for a particular transaction.
    The fastest node has an offset of 0. Null = the node didn't report the transaction.

    Returns the dataframe described above, sorted by ``min_arrival_ns``.
    """
    arrival_cols = [col for col in df.columns if col != schema.TX_HASH_COLUMN]
    return (
        df.with_columns(pl.min_horizontal(arrival_cols).alias("min_arrival_ns"))
        .select(
            schema.TX_HASH_COLUMN,
            "min_arrival_ns",
            *[
                (pl.col(col) - pl.col("min_arrival_ns")).alias(col.replace("arrival", "offset"))
                for col in arrival_cols
            ],
        )
        .sort("min_arrival_ns")
    )


def build_long_offsets(offset_frame: pl.DataFrame, providers: dict[str, str]) -> pl.DataFrame:
    """
    Unpivots the offset frame to long form: one row per (transaction, provider) observation.

    The writer stores arrivals relative to start_ref_ns (see pipeline/writer.py), so the arrival
    values are already "ns since run start" and t=0 is the actual recording start.

    Returns columns: t_s (seconds since run start), provider, offset_ns, offset_ms.
    """
    offset_cols = [col for col in offset_frame.columns if col.endswith("_offset_ns")]
    return (
        offset_frame.with_columns((pl.col("min_arrival_ns") / 1e9).alias("t_s"))
        .unpivot(on=offset_cols, index="t_s", variable_name="provider", value_name="offset_ns")
        .drop_nulls("offset_ns")
        .with_columns(
            (pl.col("offset_ns") / 1e6).alias("offset_ms"),
            pl.col("provider").str.replace("_offset_ns", "").replace(providers),
        )
    )


def _with_time_bins(long: pl.DataFrame, bin_seconds: int) -> pl.DataFrame:
    """
    Adds a t_min column: each observation floored to its ``bin_seconds``-wide bin, expressed in minutes.
    """
    return long.with_columns((pl.col("t_s") // bin_seconds * bin_seconds / 60).alias("t_min"))


def bin_median(long: pl.DataFrame, bin_seconds: int) -> pl.DataFrame:
    """
    Returns the median offset_ms per (provider, time bin), sorted by bin. Columns: provider, t_min, median_ms.
    """
    return (
        _with_time_bins(long, bin_seconds)
        .group_by("provider", "t_min")
        .agg(pl.col("offset_ms").median().alias("median_ms"))
        .sort("t_min")
    )


def bin_percentiles(long: pl.DataFrame, bin_seconds: int) -> pl.DataFrame:
    """
    Returns offset_ms percentiles per (provider, time bin), sorted by bin.
    Columns: provider, t_min, p10, p25, p50, p75, p90.
    """
    return (
        _with_time_bins(long, bin_seconds)
        .group_by("provider", "t_min")
        .agg(
            pl.col("offset_ms").quantile(0.10).alias("p10"),
            pl.col("offset_ms").quantile(0.25).alias("p25"),
            pl.col("offset_ms").quantile(0.50).alias("p50"),
            pl.col("offset_ms").quantile(0.75).alias("p75"),
            pl.col("offset_ms").quantile(0.90).alias("p90"),
        )
        .sort("t_min")
    )


def build_place_share(df: pl.DataFrame, providers: dict[str, str]) -> pl.DataFrame:
    """
    For each transaction, ranks the nodes that reported it by arrival time (place 1 = fastest,
    ties share a place via rank "min"). A null arrival means the node did not report that
    transaction and is labeled ``DNR_LABEL``.

    Returns one row per provider with each place's share of ALL transactions (a provider's row
    sums to 1.0). Columns: provider, "1", ..., "N", DNR — every label present even when its
    share is 0, so chart code can rely on the full set.
    """
    arrival_cols = [col for col in df.columns if col != schema.TX_HASH_COLUMN]
    place_labels = [str(place) for place in range(1, len(arrival_cols) + 1)] + [DNR_LABEL]

    places = (
        df.unpivot(on=arrival_cols, index=schema.TX_HASH_COLUMN, variable_name="provider", value_name="arrival_ns")
        .with_columns(pl.col("provider").str.replace("_arrival_ns", "").replace(providers))
        .with_columns(
            pl.col("arrival_ns").rank("min").over(schema.TX_HASH_COLUMN).cast(pl.Int32).alias("place")
        )
    )

    share = (
        places.group_by("provider", "place")
        .len()
        .with_columns(
            pl.col("place").cast(pl.Utf8).fill_null(DNR_LABEL).alias("place_label"),
            (pl.col("len") / df.height).alias("share"),
        )
        .pivot(values="share", index="provider", on="place_label")
        .fill_null(0.0)
    )
    # pivot only creates columns for places that actually occurred; guarantee the full set
    missing = [label for label in place_labels if label not in share.columns]
    if missing:
        share = share.with_columns(pl.lit(0.0).alias(label) for label in missing)
    return share.select("provider", *place_labels)
