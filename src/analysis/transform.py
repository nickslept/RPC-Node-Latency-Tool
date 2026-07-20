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


def build_offset_dataframe_long(offset_frame: pl.DataFrame, providers: dict[str, str]) -> pl.DataFrame:
    """
    Takes in the offset dataframe & node_N to provider mapping and returns a new long form dataframe with the following column structure:
    ``run_time_sec`` a float representing the elapsed time since run start for when the FASTEST provider reported the transaction.
    ``provider`` a string representing the provider (e.g. "alchemy").
    ``offset_ns`` an int representing the offset in nanoseconds from the fastest node for the transaction.
    ``offset_ms`` an int representing the offset in milliseconds from the fastest node for the transaction.

    **EACH NODE PROVIDER'S REPORT OF A TRANSACTION = 1 UNIQUE ROW. NULLS (node didn't report the transaction) ARE DROPPED.**
    """
    offset_cols = [col for col in offset_frame.columns if col.endswith("_offset_ns")]
    return (
        offset_frame.with_columns((pl.col("min_arrival_ns") / 1e9).alias("run_time_sec"))
        .unpivot(on=offset_cols, index="run_time_sec", variable_name="provider", value_name="offset_ns") # 2 new cols: provider (currently node_N_offset_ns from the col names before) -> offset_ns
        .drop_nulls("offset_ns")
        .with_columns(
            (pl.col("offset_ns") / 1e6).alias("offset_ms"),
            pl.col("provider").str.replace("_offset_ns", "").replace(providers), # changes the strings in the provider col from node_N_offset_ns --> node_N and then swaps that with the actual name (e.g. alchemy)
        )
    )


def _add_time_bins(long: pl.DataFrame, bin_seconds: int) -> pl.DataFrame:
    """
    Adds a ``bin_start_min`` (float) column to the long form offset dataframe.
    
    This column contains the start time of the ``bin_seconds`` sized bin each row falls in,
    in minutes relative to the run's start. Rows in the same bin share the same value.
    """
    return long.with_columns((pl.col("run_time_sec") // bin_seconds * bin_seconds / 60).alias("bin_start_min"))


def bin_median(long: pl.DataFrame, bin_seconds: int) -> pl.DataFrame:
    """
    Takes in a long form offset dataframe and the bin time (in seconds) and returns a dataframe with the following column structure:

    - ``provider`` a string representing the provider (e.g. "alchemy").
    - ``bin_start_min`` a float containing the start time of the ``bin_seconds`` sized bin each row falls in, in minutes relative to the run's start.
    - ``median_ms`` a float representing the median offset in milliseconds per node provider within each bin.
    """
    return (
        _add_time_bins(long, bin_seconds)
        .group_by("provider", "bin_start_min")
        .agg(pl.col("offset_ms").median().alias("median_ms"))
        .sort("bin_start_min")
    )


def bin_percentiles(long: pl.DataFrame, bin_seconds: int) -> pl.DataFrame:
    """
    Takes in a long form offset dataframe and the bin time (in seconds) and returns a dataframe with the following column structure:

    - ``provider`` a string representing the provider (e.g. "alchemy").
    - ``bin_start_min`` a float containing the start time of the ``bin_seconds`` sized bin each row falls in, in minutes relative to the run's start.
    - ``p10``, ``p25``, ``p50``, ``p75``, ``p90`` are all floats representing the corresponding percentile offset in milliseconds per node provider within each bin.
        - e.g. p10 means that 10% of the data within the time bin for the node provider lies below that float.
    """
    return (
        _add_time_bins(long, bin_seconds)
        .group_by("provider", "bin_start_min")
        .agg(
            pl.col("offset_ms").quantile(0.10).alias("p10"),
            pl.col("offset_ms").quantile(0.25).alias("p25"),
            pl.col("offset_ms").quantile(0.50).alias("p50"),
            pl.col("offset_ms").quantile(0.75).alias("p75"),
            pl.col("offset_ms").quantile(0.90).alias("p90"),
        )
        .sort("bin_start_min")
    )


def build_place_share_dataframe(df: pl.DataFrame, providers: dict[str, str]) -> pl.DataFrame:
    """
    Takes in the cleaned parquet file's dataframe and creates a new dataframe with the following column layout:

    - ``provider``: A string containing the provider's name
    
    - ``1`` ... ``N`` : A float representing the fraction of ALL transactions in the run where the provider's report 
    arrived in that place (``1`` = fastest node to report the transaction). 
    
    - ``DNR_LABEL``: A float representing the fraction of ALL transactions in the run where the provider did not report
    the transaction. This column takes on the name of whatever ``DNR_LABEL`` is actually set to. Default = "DNR".
    """
    arrival_cols = [col for col in df.columns if col != schema.TX_HASH_COLUMN]
    place_labels = [str(place) for place in range(1, len(arrival_cols) + 1)] + [DNR_LABEL]

    places = (
        df.unpivot(on=arrival_cols, index=schema.TX_HASH_COLUMN, variable_name="provider", value_name="arrival_ns") # 3 cols remaining: tx_hash, provider, arrival_ns
        .with_columns(pl.col("provider").str.replace("_arrival_ns", "").replace(providers)) # replaces the strings in the provider col from node_N_offset_ns --> node_N and then swaps that with the actual name (e.g. alchemy)
        .with_columns(
            pl.col("arrival_ns").rank("min").over(schema.TX_HASH_COLUMN).cast(pl.Int32).alias("place") # adds a place column (int32) which assigns a speed-based rank to each node provider within each tx_hash. ties are dealt w/ automatically.
        )
    )

    share = (
        places.group_by("provider", "place")
        .len() # adds a len column which just counts how many times a certain place showed up in the data
        .with_columns(
            pl.col("place").cast(pl.Utf8).fill_null(DNR_LABEL).alias("place_label"), #adds a place_label column which is just the place column but everything is converted to a string, and nulls take on DNR_LABEL
            (pl.col("len") / df.height).alias("share"), #adds a fraction of ALL transactions where a provider finished in a specific place column (float)
        ) 
        .pivot(values="share", index="provider", on="place_label") #converts the table into a provider column, 1-max place, and DNR_LABEL column in which all the values are the cumulative share of place among all transactions
        .fill_null(0.0) #changes nulls (e.g. a node always reported and therefore has NO share of DNR_LABEL) to 0.0
    )
    
    # ensures that missing columns are handled (e.g. none of the nodes were ever in 4th place)
    missing = [label for label in place_labels if label not in share.columns]
    if missing:
        share = share.with_columns(pl.lit(0.0).alias(label) for label in missing) #adds the missing column and assigns the value 0.0 for all the node providers
    return share.select("provider", *place_labels) #returned in proper order