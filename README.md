<div align="center">

# polymarket-rpc-latency-bench

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**Collects, cleans, and analyzes latency data across Polygon RPC node providers for Polymarket trades.**
</div>


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
- Concurrently opens a WebSocket connection to every provider and sends each one an identical `eth_subscribe` request for `logs` emitted by the Polymarket exchange contracts with the `OrderFilled` event topic.
- Recording only begins once **every** node has acknowledged its subscription (within `ack_timeout_seconds`). Each listener then discards messages until its node has been quiet for a moment so no provider has a head start.
- Each listener timestamps every incoming message with a monotonic clock (`time.monotonic_ns()`) the moment it arrives, then hands it off to the raw queue. JSON parsing happens in a separate task so nothing slows down the message receive loops.
- A processor drains the raw queue, extracts each message’s `tx_hash`, and keeps only the **first** arrival time per node per transaction.
- A transaction is promoted to the write queue once `min_nodes_required` nodes have reported it. A background scanner also promotes transactions whose earliest report is older than `timeout_seconds`, so a transaction still gets written even if a node misses it entirely. The same mechanism protects slow nodes, since a transaction is promoted by the scanner rather than waiting forever for a report from `min_nodes_required` nodes.
- A writer batches rows in the write queue, and writes them to a Parquet file: one row per trade with its `tx_hash` and one arrival-time column per node, stored as nanoseconds since recording started. Run metadata (reference start/end times, UTC start time, and the node number to provider name mapping) is embedded in the file.
- Mid-run disconnects are logged to a `.disconnects.txt` file. `stop_on_disconnect` in the config controls whether the run ends or keeps going when a node disconnects.

### 2. Data cleaning
- Removes duplicate `tx_hash` rows from a raw run file. Duplicates can occur when a transaction is promoted (e.g. by timeout) and a straggler node reports it afterwards, creating a second partial row for the same trade.
- Duplicate rows are merged by keeping each node’s earliest non-null arrival time, leaving exactly one row per transaction. Files already free of duplicates are simply moved and renamed; run metadata is carried over either way.

### 3. Data analysis
- Converts each node’s arrival time into a delay (in ms) behind the fastest node for that transaction. Therefore, the fastest provider has a delay of 0 ms, and a null means the provider never reported the trade.
- For the time-series charts, transactions are binned (grouped) into fixed time intervals chosen by the user. 
- The following charts are generated:
  - **Delay boxplot**: each provider’s distribution of delay behind the fastest node, across all transactions.
  - **Median delay line plot (all providers)**: a time-binned line plot depicting every provider’s median delay behind the fastest node for each transaction over time.
  - **Fan charts (one per provider)**: a time-binned fan chart depicting the provider’s delay behind the fastest node for each transaction over time, with shaded p10–p90 and p25–p75 bands.
  - **Speed-ranking stacked bar chart**: the share of transactions each provider reported 1st, 2nd, …, or not at all ("DNR" = did not report).
  - The boxplot and speed-ranking chart are also generated a second time using only transactions that **every** node reported.

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

Pick a raw file from the list. Duplicate `tx_hash` rows are merged by keeping each node’s earliest arrival time. Files that are already free of duplicate `tx_hash` rows are simply moved and renamed. Run metadata is carried over regardless. The cleaned file is saved in `data/processed/`.

### 3. Analyze

```bash
python -m src analyze
```

Pick a cleaned file from the list. Next, pick a bin size (in seconds) for the time-binned charts. Re-running with a different bin size adds new charts alongside the existing ones (time-binned charts include the bin size in the filename). All charts are saved to `data/results/analysis_of_run_*/`.

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
