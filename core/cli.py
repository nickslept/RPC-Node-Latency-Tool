"""
Available commands:
    python -m core              ->       list the available commands
    python -m core -h OR --help ->       list the available commands
    python -m core ingest       ->       starts a new data collection run (optional: --duration HH:MM:SS to automatically stop the run after that much time)
    python -m core clean        ->       pick a parquet file to clean from RAW_DIR. Saves to PROCESSED_DIR.  
    python -m core analyze      ->       pick a file from RAW_DIR OR PROCESSED_DIR to analyze
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


def _parse_duration(value: str) -> int:
    """
    Converts a run duration string formatted as HH:MM:SS (e.g. 101:23:10) into total seconds.

    Hours can be any non-negative number. Minutes and seconds must be between 0 and 59.

    Returns the duration as a total number of seconds.
    """
    parts = value.split(":")
    if len(parts) != 3 or not all(part.isdigit() for part in parts): # checks for non-integers AND negative numbers
        raise argparse.ArgumentTypeError(f"[ERROR] Invalid duration format: '{value}'. Expected HH:MM:SS (e.g. 101:23:10)")
    hours, minutes, seconds = (int(part) for part in parts)
    if minutes > 59 or seconds > 59:
        raise argparse.ArgumentTypeError(f"[ERROR] Invalid duration '{value}', minutes and seconds must be between 0 and 59")
    total_seconds = hours * 3600 + minutes * 60 + seconds
    if total_seconds == 0:
        raise argparse.ArgumentTypeError(f"[ERROR] Invalid duration '{value}', duration must be greater than 00:00:00")
    return total_seconds


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
            print(f"[INFO] Selected: {files[int(choice) - 1]}")
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
    return run_ingestion(config, output_path, duration_seconds=args.duration)


def _cmd_clean(args: argparse.Namespace) -> int:
    input_path = _select_parquet_file([RAW_DIR], action="clean")
    if input_path is None:
        print("[ERROR] No file selected or no files to clean.")
        return 1

    print("Not implemented yet")
    return 2


def _cmd_analyze(args: argparse.Namespace) -> int:
    input_path = _select_parquet_file([RAW_DIR, PROCESSED_DIR], action="analyze")
    if input_path is None:
        print("[ERROR] No file selected or no files to analyze.")
        return 1

    print("Not implemented yet")
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rpc-node-latency-tool",
        description="A program that collects, cleans, and analyzes blockchain data from multiple RPC nodes for latency comparison.",
    )
    sub = parser.add_subparsers(dest="command")

    p_ingest = sub.add_parser(
        "ingest", help="starts a new data collection run. saves to RAW_DIR. optional: --duration HH:MM:SS to automatically stop the run after that much time."
    )
    p_ingest.add_argument(
        "--duration",
        type=_parse_duration,
        default=None,
        metavar="HH:MM:SS",
        help="automatically stops the run after that much time (format: HH:MM:SS; e.g. 101:23:10).",
    )
    p_ingest.set_defaults(func=_cmd_ingest)

    p_clean = sub.add_parser(
        "clean", help="pick a parquet file to clean from RAW_DIR. Saves to PROCESSED_DIR."
    )
    p_clean.set_defaults(func=_cmd_clean)

    p_analyze = sub.add_parser(
        "analyze", help="pick a parquet file from RAW_DIR or PROCESSED_DIR to analyze."
    )
    p_analyze.set_defaults(func=_cmd_analyze)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None): #prints help if no function specified
        parser.print_help()
        return 0
    return args.func(args)