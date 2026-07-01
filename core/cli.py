"""Command-line interface: ``ingest`` / ``clean`` / ``analyze`` / ``help``.

Only ``ingest`` is implemented -- it runs the live data-collection pipeline.
``clean`` and ``analyze`` are stubs to be filled in later. Rather than taking a
file path on the command line, each one interactively prompts you to pick the
parquet file it should operate on (``clean`` from data/raw/, ``analyze`` from
data/raw/ or data/processed/) so the three-stage flow (raw -> processed ->
results) is visible.

None of the commands take flags. ``ingest`` always reads ``config.toml`` and
``.env`` from the repo root and writes a timestamped file under data/raw/.

Run it from the repo root, e.g.:

    python -m core ingest      # collect a run to data/raw/run_<timestamp>.parquet
    python -m core clean       # pick a file from data/raw/ to clean
    python -m core analyze     # pick a file from data/raw/ or data/processed/
    python -m core help        # list the available commands
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from .config import ConfigError, load_config

# Fixed input locations, relative to the repo root.
CONFIG_PATH = "config.toml"
ENV_PATH = ".env"

# The three-stage data directories, relative to the repo root.
RAW_DIR = os.path.join("data", "raw")
PROCESSED_DIR = os.path.join("data", "processed")


def _default_raw_path() -> str:
    """A timestamped parquet path under data/raw/ (sortable, filesystem-safe)."""
    stamp = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    return os.path.join(RAW_DIR, f"{stamp}.parquet")


def _prompt_for_parquet(directories: list[str], *, action: str) -> str | None:
    """Lists the parquet files under ``directories`` and asks the user to pick one.

    ``directories`` are the folders to scan, in priority order; missing folders
    are skipped. ``action`` is the verb shown in the prompt (e.g. "clean").

    Returns the chosen path, or None if there is nothing to pick or the user
    cancels (by entering 'q', a blank line, or EOF/Ctrl-C).
    """
    # Gather every .parquet file across the given directories, newest first.
    candidates: list[str] = []
    for directory in directories:
        if not os.path.isdir(directory):
            continue
        for name in os.listdir(directory):
            if name.endswith(".parquet"):
                candidates.append(os.path.join(directory, name))
    candidates.sort(key=os.path.getmtime, reverse=True)

    if not candidates:
        print(f"No parquet files found in: {', '.join(directories)}")
        return None

    print(f"Select a file to {action} (newest first):")
    for number, path in enumerate(candidates, start=1):
        print(f"  {number}) {path}")

    while True:
        try:
            choice = input(f"Enter a number 1-{len(candidates)} ('q' to cancel): ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return None
        if choice.lower() in ("", "q", "quit"):
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(candidates):
            return candidates[int(choice) - 1]
        print("Invalid selection, try again.")


def _cmd_ingest(args: argparse.Namespace) -> int:
    try:
        config = load_config(CONFIG_PATH, env_path=ENV_PATH)
    except ConfigError as exc:
        print(f"config error: {exc}")
        return 1

    output_path = _default_raw_path()
    print(f"output: {output_path}")

    # Imported lazily so `clean`/`analyze` don't pull in websockets/pyarrow.
    from .pipeline.runner import run as run_ingestion

    return run_ingestion(config, output_path)


def _cmd_clean(args: argparse.Namespace) -> int:
    input_path = _prompt_for_parquet([RAW_DIR], action="clean")
    if input_path is None:
        print("No file selected; nothing to clean.")
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
    input_path = _prompt_for_parquet([RAW_DIR, PROCESSED_DIR], action="analyze")
    if input_path is None:
        print("No file selected; nothing to analyze.")
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