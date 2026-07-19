from __future__ import annotations

import os

import polars as pl

from .. import schema
from . import charts, prep


def _generate_analysis_dir(input_path: str, results_dir: str) -> str:
    """
    Returns the output dir containing the folder name (for a run's analysis) as a String: ``results_dir`` with the filename's ``cleaned_``
    prefix replaced by ``analysis_of_`` and .parquet dropped (e.g. cleaned_run_X.parquet -> results_dir/analysis_of_run_X).
    """
    stem = os.path.splitext(os.path.basename(input_path))[0]
    stem = stem.removeprefix("cleaned_")
    return os.path.join(results_dir, f"analysis_of_{stem}")


def _prompt_bin_size() -> int | None:
    """
    Asks the user for the bin size (in seconds) used by the time-binned charts.

    Returns the width as an int, or None if the user cancels.
    """
    while True:
        try:
            choice = input("Enter a bin size, in seconds, for the time-binned charts ('stop' to cancel): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return None
        if choice.lower() in ("", "stop"):
            return None
        if choice.isdigit() and int(choice) > 0:
            print(f"[INFO] Bin size: {choice}s")
            return int(choice)
        print("[ERROR] Invalid bin size, please enter a positive whole number of seconds.")


def _ensure_safe_filename(name: str) -> str:
    """
    Makes a RPC node provider's name (found in file metadata) safe to use in a filename. 

    Replaces illegal characters (e.g. /) with "_".

    Returns the filename as a String.
    """
    return "".join(char if char.isalnum() or char in ("-", "_") else "_" for char in name)


def run_analysis(input_path: str, results_dir: str) -> int:
    """
    Analyzes the cleaned parquet file at ``input_path`` and saves the charts to a per-run folder
    under ``results_dir``. Binned chart filenames carry the chosen bin width, so re-running with a
    different width adds new pictures next to the old ones instead of overwriting them.

    Returns ``0`` on success, ``1`` if the user cancels or the file is unusable.
    """
    bin_seconds = _prompt_bin_size()
    if bin_seconds is None:
        print("[ERROR] No bin size selected.")
        return 1

    try:
        metadata = schema.read_file_metadata(input_path)
    except KeyError:
        print(f"[ERROR] Selected file is missing run metadata (node providers / start time): {input_path}")
        return 1
    providers: dict[str, str] = metadata["node_providers"]

    df = pl.read_parquet(input_path)
    if df.height == 0:
        print(f"[ERROR] Selected file has no rows: {input_path}")
        return 1

    output_dir = _generate_analysis_dir(input_path, results_dir)
    os.makedirs(output_dir, exist_ok=True)

    ordered_providers = prep.get_provider_order(providers)
    try:
        provider_colors = charts.build_provider_color_map(ordered_providers)
    except ValueError as exc:
        return 1 #already prints within build_provider_color_map
    offset_frame = prep.build_offset_dataframe(df)
    long = prep.build_offset_dataframe_long(offset_frame, providers)

    saved: list[str] = []

    path = os.path.join(output_dir, "latency_boxplot.png")
    charts.save_latency_boxplot(long, provider_colors, path)
    saved.append(path)

    path = os.path.join(output_dir, f"median_latency_over_run_{bin_seconds}s.png")
    charts.save_median_over_run(prep.bin_median(long, bin_seconds), bin_seconds, provider_colors, path)
    saved.append(path)

    band = prep.bin_percentiles(long, bin_seconds)
    for provider in ordered_providers:
        path = os.path.join(output_dir, f"percentiles_{_ensure_safe_filename(provider)}_{bin_seconds}s.png")
        charts.save_percentile_bands(band.filter(pl.col("provider") == provider), provider, bin_seconds, path)
        saved.append(path)

    path = os.path.join(output_dir, "finishing_places.png")
    charts.save_finishing_places(prep.build_place_share_dataframe(df, providers), provider_colors, path)
    saved.append(path)

    print(f"[INFO] Analyzed {df.height:,} transactions across {len(ordered_providers)} providers.")
    for chart_path in saved:
        print(f"[INFO] Saved: {chart_path}")
    return 0