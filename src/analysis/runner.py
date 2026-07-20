from __future__ import annotations

import os

import polars as pl

from .. import schema
from . import charts, transform


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
    Analyzes the cleaned parquet file in ``input_path`` and saves the charts to a per-run analysis folder
    under ``results_dir``. Binned chart filenames carry the chosen bin size, so re-running with a
    different bin size adds new pictures to the folder.

    Returns ``0`` on success and ``1`` if the user cancels or there is an error.
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

    ordered_providers = transform.get_provider_order(providers)
    try:
        provider_colors = charts.build_provider_color_map(ordered_providers)
    except ValueError as exc:
        print(f"[ERROR]: {exc}")
        return 1
    offset_dataframe = transform.build_offset_dataframe(df)
    offset_dataframe_long = transform.build_offset_dataframe_long(offset_dataframe, providers)

    saved: list[str] = []

    path = os.path.join(output_dir, "delay_boxplot.png")
    charts.generate_and_save_delay_boxplot(offset_dataframe_long, provider_colors, path)
    saved.append(path)

    path = os.path.join(output_dir, f"median_delay_lineplot_all_nodes_binned_{bin_seconds}s.png")
    charts.generate_and_save_median_delay_lineplot_all_nodes(transform.bin_median(offset_dataframe_long, bin_seconds), bin_seconds, provider_colors, path)
    saved.append(path)

    binned_percentiles_dataframe = transform.bin_percentiles(offset_dataframe_long, bin_seconds)
    for provider in ordered_providers:
        path = os.path.join(output_dir, f"delay_fan_chart_{_ensure_safe_filename(provider)}_binned_{bin_seconds}s.png")
        charts.generate_and_save_delay_fan_chart(binned_percentiles_dataframe.filter(pl.col("provider") == provider), provider, bin_seconds, path)
        saved.append(path)

    path = os.path.join(output_dir, "speed_ranking_stacked_bar_chart_all_transactions.png")
    charts.generate_and_save_speed_ranking_stacked_bar_chart_all_transactions(transform.build_place_share_dataframe(df, providers), provider_colors, path)
    saved.append(path)

    print(f"[SUMMARY] Analyzed {df.height:,} transactions across {len(ordered_providers)} providers.")
    for chart_path in saved:
        print(f"[SUMMARY] Saved: {chart_path}")
    return 0