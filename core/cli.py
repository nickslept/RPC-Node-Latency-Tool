"""Command-line interface: ``ingest`` / ``clean`` / ``analyze``.

Only ``ingest`` is implemented -- it runs the live data-collection pipeline.
``clean`` and ``analyze`` are stubs to be filled in later; their argument shapes
are sketched so the three-stage flow (raw -> processed -> results) is visible.

Run it from the repo root, e.g.:

    python -m core ingest                          # writes data/raw/run_<ts>.parquet
    python -m core ingest --config config.toml -o my_run.parquet
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from .config import ConfigError, load_config


def _default_raw_path() -> str:
    """A timestamped parquet path under data/raw/ (sortable, filesystem-safe)."""
    stamp = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    return os.path.join("data", "raw", f"{stamp}.parquet")


def _cmd_ingest(args: argparse.Namespace) -> int:
    try:
        config = load_config(args.config, env_path=args.env)
    except ConfigError as exc:
        print(f"config error: {exc}")
        return 1

    output_path = args.output or _default_raw_path()
    print(f"output: {output_path}")

    # Imported lazily so `clean`/`analyze` don't pull in websockets/pyarrow.
    from .pipeline.runner import run as run_ingestion

    return run_ingestion(config, output_path)


def _cmd_clean(args: argparse.Namespace) -> int:
    print("`clean` is not implemented yet.")
    print(
        "Planned: read a raw parquet from data/raw/, collapse duplicate tx_hash "
        "rows by taking each node column's earliest (min) value, and write the "
        "deduplicated result to data/processed/."
    )
    return 2


def _cmd_analyze(args: argparse.Namespace) -> int:
    print("`analyze` is not implemented yet.")
    print(
        "Planned: read a processed parquet, compute the lag metric, and write "
        "charts and statistics to data/results/."
    )
    return 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rpc-latency",
        description="RPC node latency comparison tool (ingest -> clean -> analyze).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser(
        "ingest", help="collect live per-node trade arrival timestamps to a parquet"
    )
    p_ingest.add_argument("--config", default="config.toml", help="path to config.toml")
    p_ingest.add_argument("--env", default=".env", help="path to the .env secrets file")
    p_ingest.add_argument(
        "-o",
        "--output",
        default=None,
        help="output parquet path (default: data/raw/run_<timestamp>.parquet)",
    )
    p_ingest.set_defaults(func=_cmd_ingest)

    p_clean = sub.add_parser("clean", help="(not implemented) dedupe a raw parquet")
    p_clean.add_argument("input", nargs="?", help="raw parquet path")
    p_clean.add_argument("-o", "--output", default=None, help="processed parquet path")
    p_clean.set_defaults(func=_cmd_clean)

    p_analyze = sub.add_parser(
        "analyze", help="(not implemented) produce charts and statistics"
    )
    p_analyze.add_argument("input", nargs="?", help="processed parquet path")
    p_analyze.add_argument("-o", "--output-dir", default=None, help="results directory")
    p_analyze.set_defaults(func=_cmd_analyze)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)