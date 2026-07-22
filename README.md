# polymarket-rpc-latency-bench

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**Collects, cleans, and analyzes latency data across Polygon RPC node providers for Polymarket trades.**

## Overview

Polymarket trades settle on the Polygon blockchain, where each completed trade’s exchange contract emits an `OrderFilled` event. This tool subscribes, at the same time, to multiple RPC node providers who stream these events, timestamps each provider’s delivery of every trade with nanosecond precision, and visualizes the latency data. This is useful if you’re choosing between multiple providers for Polymarket tooling where reading real-time trade data as quickly as possible is crucial.

The project runs in three stages, each with its own command:

| Stage | Command | Output |
|-------|---------|--------|
| **Collect** | `python -m src collect` | `data/raw/run_<timestamp>_UTC.parquet` |
| **Clean** | `python -m src clean` | `data/processed/cleaned_run_*.parquet` |
| **Analyze** | `python -m src analyze` | charts in `data/results/analysis_of_run_*/` |

## How it works

### 1. Data collection pipeline
- 

### 2. Data cleaning
- Removes
- 

### 3. Data analysis
- talk about binning the data
- talk about charts being generated

## Requirements

- Python 3.11+
- A websocket endpoint (with API key) from each provider you want to benchmark. The default config compares five: [Chainstack](https://chainstack.com), [Infura](https://infura.io), [dRPC](https://drpc.org), [QuickNode](https://quicknode.com), and [Alchemy](https://alchemy.com).

## Installation

```bash
# Windows
git clone https://github.com/nickslept/polymarket-rpc-latency-bench.git
cd polymarket-rpc-latency-bench

python -m venv .venv
.venv\Scripts\activate

pip install -e .
```

## Configuration

### API keys

**1. Copy the template**

```bash
cp env.example .env
```

**2. Fill in your keys in the new ``.env`` file (note that QuickNode also needs its unique subdomain):**
```ini
QUICKNODE_SUBDOMAIN=
QUICKNODE_KEY=
CHAINSTACK_KEY=
DRPC_KEY=
ALCHEMY_KEY=
INFURA_KEY=
```

### Run settings

All modifiable run settings can be found in `config.toml`. Inline comments extensively detail each variable. 

## Usage

Run `python -m src` to list the available commands.

### 1. Collect

```bash
python -m src collect                        # runs until user presses Ctrl+C
python -m src collect --duration 100:00:00   # stops automatically after HH:MM:SS (in this example 100 hours)
```

Connects to every node, waits for all subscription acks, then starts recording the data. Progress lines print as each batch of data is written to disk. The run file is saved in `data/raw/`.

### 2. Clean

```bash
python -m src clean
```

Pick a raw file from the list. Duplicate `tx_hash` rows are merged by keeping each node's earliest arrival time. Files that are already free of duplicate `tx_hash` rows are simply moved and renamed. Run metadata is carried over regardless. The cleaned file is saved in `data/processed/`.

### 3. Analyze

```bash
python -m src analyze
```

Pick a cleaned file from the list. Next, pick a bin size (in seconds) for the time-binned charts. Re-running with a different bin size adds new charts alongside the existing ones (binned filenames include the bin size). All charts are saved to `data/results/analysis_of_run_*/`.

## Sample data


## Project structure

```
src/
├── cli.py                    # argparse CLI: collect / clean / analyze
├── config.py                 # config.toml & .env loading and validation
├── schema.py                 # handles parquet schema and file metadata
├── pipeline/
│   ├── runner.py             # orchestrates a data collection run
│   ├── connections.py        # opens + closes WebSocket connections
│   ├── listener.py           # syncs nodes before data collection begins, per-node message receive loop & timestamping data arrival
│   ├── processor.py          # message removal from raw queue, tx hash parsing, handles promotion of filled rows (based on min in config)
│   ├── scanner.py            # handles timeout-based promotion of partially filled rows
│   ├── writer.py             # writes Parquet file
│   ├── state.py              # shared state (queues, counters, events)
│   └── disconnect_logger.py  # logs when nodes disconnect
├── cleaning/
│   └── cleaner.py            # removes duplicate tx_hash/prepares raw run file for analysis
└── analysis/
    ├── runner.py             # orchestrates data analysis
    ├── transform.py          # manages dataframes 
    └── charts.py             # manages charts
```

## License

MIT — see [LICENSE](LICENSE).