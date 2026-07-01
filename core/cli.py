"""
Avaliable commands:
    python -m core help        # list the available commands
    python -m core ingest      # starts a new data collection run
    python -m core clean       # pick a parquet file to clean from data/raw/. Saves to data/processed/.  
    python -m core analyze     # pick a file from data/raw/ or data/processed/ to analyze
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from .config import ConfigError, load_config

# Various paths and directories
CONFIG_PATH = "config.toml"
ENV_PATH = ".env"
RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")


def _generate_new_raw_path() -> str:
    """
    Returns a unique path for a new run's parquet file based on the current time.
    """
    filename = datetime.now(timezone.utc).strftime("run_%m-%d-%Y_%H-%M-%S_UTC")
    return os.path.join(RAW_DIR, f"{filename}.parquet")


def _select_parquet_file(directories: list[str], *, action: str) -> str | None:
    """
    Lists the parquet files under ``directories`` and asks the user to pick a file.

    ``directories`` is a list of the folders to scan (example item: raw/). 
    ``action`` is the specific command (e.g. "clean").

    Returns the chosen path as a String if possible. Returns None if there are no files to pick from OR if the user manually cancels.
    """
    files: list[str] = []
    for directory in directories:
        if not os.path.isdir(directory):
            continue
        for name in os.listdir(directory):
            if name.endswith(".parquet"):
                files.append(os.path.join(directory, name))
    files.sort(key=os.path.getmtime, reverse=False)

    if not files:
        print(f"[ERROR] No parquet files found in: {', '.join(directories)}")
        return None

    print(f"Select a file to {action}:")
    for number, path in enumerate(files, start=1):
        print(f"[{number}] {path}")

    while True:
        try:
            choice = input(f"Enter a number from 1-{len(files)} ('stop' to cancel): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return None
        if choice.lower() in ("", "stop"):
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return files[int(choice) - 1]
        print("[ERROR] Invalid selection, please try again.")


def _cmd_ingest(args: argparse.Namespace) -> int:
    try:
        config = load_config(CONFIG_PATH, env_path=ENV_PATH)
    except ConfigError as exc:
        print(f"[ERROR] Config error: {exc}")
        return 1

    output_path = _generate_new_raw_path()
    print(f"[INFO] Output path: {output_path}")

    from .pipeline.runner import run as run_ingestion
    return run_ingestion(config, output_path)


def _cmd_clean(args: argparse.Namespace) -> int:
    input_path = _select_parquet_file([RAW_DIR], action="clean")
    if input_path is None:
        print("[ERROR] No file selected or nothing to clean.")
        return 1

    print(f"selected: {input_path}")
    print("`clean` is not implemented yet.")
    print(
        "Planned: read the selected raw parquet, collapse duplicate tx_hash "
        "rows by taking each node column's earliest (min) value, and write the "
        "deduplicated result to data/processed/."
    )
    return 2


def _cmd_analyze(args: argparse.Namespace) -> int:
    input_path = _select_parquet_file([RAW_DIR, PROCESSED_DIR], action="analyze")
    if input_path is None:
        print("[ERROR] No file selected or nothing to analyze.")
        return 1

    print(f"selected: {input_path}")
    print("`analyze` is not implemented yet.")
    print(
        "Planned: read the selected parquet, compute the lag metric, and write "
        "charts and statistics to data/results/."
    )
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rpc-latency",
        description="RPC node latency comparison tool (ingest -> clean -> analyze).",
    )
    # Not required: running with no command falls back to printing help (see main).
    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser(
        "ingest", help="collect live per-node trade arrival timestamps to a parquet"
    )
    p_ingest.set_defaults(func=_cmd_ingest)

    p_clean = sub.add_parser(
        "clean", help="(not implemented) pick a raw parquet to dedupe"
    )
    p_clean.set_defaults(func=_cmd_clean)

    p_analyze = sub.add_parser(
        "analyze", help="(not implemented) pick a raw/processed parquet to analyze"
    )
    p_analyze.set_defaults(func=_cmd_analyze)

    p_help = sub.add_parser("help", help="list the available commands")
    p_help.set_defaults(func=lambda args: (parser.print_help() or 0))

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    # With no subcommand there's no func to dispatch to -- show the command list.
    if not getattr(args, "func", None):
        parser.print_help()
        return 0
    return args.func(args)