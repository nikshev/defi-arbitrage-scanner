# DeFi Arbitrage Scanner

A real-time blockchain data pipeline that detects triangular arbitrage
opportunities across Uniswap V2, Uniswap V3, and SushiSwap on Ethereum.
Prices are modelled as edges in a JanusGraph property graph; Gremlin
cycle-detection traversals surface profitable 3-hop paths; PySpark
aggregates historical snapshots; and a Streamlit dashboard visualises
everything live.

---

## Architecture

```
                     Ethereum Mainnet (Infura RPC)
                              |
                    +---------+---------+
                    |   DEXFetcher      |  (fetcher/dex_fetcher.py)
                    |  Uniswap V2/V3    |
                    |  SushiSwap        |
                    +---------+---------+
                              |
               +---------------------------+
               |  Parquet Snapshots        |  (data/snapshots/)
               +---------------------------+
                     |              |
          +----------+    +---------+---------+
          |               |   JanusGraph      |
          |               |  Token vertices   |
          |               |  Pool edges       |
          |               +---------+---------+
          |                         |
  +-------+-------+       +---------+---------+
  |  PySpark       |       |  ArbitrageFinder  |
  |  Historical    |       |  Gremlin cycles   |
  |  Analysis      |       +---------+---------+
  +-------+-------+                 |
          |                         |
          +------------+------------+
                       |
              +--------+---------+
              |  Streamlit       |
              |  Dashboard       |
              |  :8501           |
              +------------------+
```

---

## Project Structure

```
blockchain-data-agent/
├── docker-compose.yml          # Orchestrates JanusGraph + app + dashboard
├── Dockerfile                  # Root app image (Python 3.11 + JDK)
├── requirements.txt            # Full Python dependencies
├── .env.example                # Environment variable template
├── config/
│   └── settings.yaml           # All runtime configuration
├── fetcher/
│   ├── dex_fetcher.py          # Web3 price fetcher + Parquet snapshots
│   └── abi/
│       ├── uniswap_v2_pair.json
│       ├── uniswap_v3_pool.json
│       └── erc20.json
├── graph/
│   ├── schema.py               # JanusGraph schema (vertices + edges + indexes)
│   ├── graph_loader.py         # Upsert logic for tokens and pool edges
│   └── arbitrage_finder.py     # Gremlin cycle-detection + profit calculation
├── spark/
│   └── historical_analysis.py  # PySpark aggregations over snapshots
├── dashboard/
│   ├── app.py                  # Streamlit UI
│   ├── Dockerfile              # Lightweight dashboard image
│   └── requirements_dashboard.txt
├── data/
│   └── snapshots/              # Parquet files written by DEXFetcher
└── tests/
    ├── test_fetcher.py         # pytest suite for DEXFetcher
    └── test_arbitrage.py       # pytest suite for ArbitrageFinder
```

---

## Quick Start

### With Docker (recommended)

```bash
# 1. Copy and fill in your environment variables
cp .env.example .env
# Edit .env — set INFURA_RPC_URL to your Infura project endpoint

# 2. Build and start all services
docker compose up --build

# 3. Open the dashboard
open http://localhost:8501
```

Services started:
| Service      | Port  | Description                            |
|--------------|-------|----------------------------------------|
| janusgraph   | 8182  | JanusGraph / Gremlin Server            |
| app          | -     | Price fetcher (runs continuously)      |
| dashboard    | 8501  | Streamlit UI                           |

---

### Without Docker (local development)

#### Prerequisites

- Python 3.11+
- Java 11+ (required by PySpark and JanusGraph)
- Optional: a running JanusGraph instance

#### Installation

```bash
cd blockchain-data-agent

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env — fill in INFURA_RPC_URL
```

#### Run the fetcher

```bash
# Fetches prices and saves Parquet snapshots every 30 seconds
python -m fetcher.dex_fetcher
```

The fetcher automatically falls back to realistic mock data when no RPC URL
is configured, so you can run it without an Infura key for demos.

#### Run the dashboard

```bash
streamlit run dashboard/app.py
```

Navigate to `http://localhost:8501`.

#### Run the Spark historical analysis

```python
import yaml
from spark.historical_analysis import HistoricalAnalyzer

with open("config/settings.yaml") as f:
    config = yaml.safe_load(f)

analyzer = HistoricalAnalyzer(config)
results = analyzer.analyze_opportunities()
print(results["by_pair"])
print(results["by_hour"])
```

---

## Configuration

All settings live in `config/settings.yaml`.  Key sections:

| Section          | Key                        | Default      | Description                          |
|------------------|----------------------------|--------------|--------------------------------------|
| `ethereum`       | `rpc_url`                  | env var      | Infura / Alchemy RPC endpoint        |
| `arbitrage`      | `min_profit_ratio`         | `0.005`      | 0.5% minimum profit filter           |
| `arbitrage`      | `update_interval_seconds`  | `30`         | Fetch cadence                        |
| `janusgraph`     | `host`                     | `localhost`  | JanusGraph hostname                  |
| `janusgraph`     | `port`                     | `8182`       | Gremlin WebSocket port               |
| `spark`          | `snapshots_path`           | `data/snapshots` | Parquet snapshot directory       |
| `spark`          | `report_path`              | `data/reports`   | Analysis report output           |

---

## Running Tests

```bash
# Run the full test suite
pytest tests/ -v

# Run only fetcher tests
pytest tests/test_fetcher.py -v

# Run only arbitrage tests
pytest tests/test_arbitrage.py -v

# With coverage report
pip install pytest-cov
pytest tests/ --cov=fetcher --cov=graph --cov-report=term-missing
```

Tests use mock mode by default — no live RPC or JanusGraph connection needed.

---

## Mock / Demo Mode

Both the fetcher and arbitrage finder degrade gracefully when external
services are unavailable:

- **No RPC URL**: `DEXFetcher` generates realistic mock prices with small
  random spreads between DEXes.
- **No JanusGraph**: `ArbitrageFinder` returns pre-defined mock opportunities
  with randomised profit noise.
- **No Parquet snapshots**: `HistoricalAnalyzer` synthesises 48-hour demo
  data covering all tracked pairs.
- **Dashboard**: fully functional using the mock backends above.

---

## Dashboard Features

- **Live Arbitrage Opportunities** — filterable table with path, profit %,
  DEXes used, and timestamp.
- **Price Deviation Heatmap** — per-pair spread across DEXes, coloured
  green (positive) to red (negative).
- **Arbitrage Cycle Graph** — interactive Plotly network graph showing
  which tokens and DEXes form profitable cycles.
- **Historical Analysis Charts** — average spread by token pair and by
  hour of day (UTC).
- **Auto-refresh** — configurable 10/30/60/120-second auto-refresh with a
  sidebar toggle.

---

## Demo Screenshots

_Screenshots will be added after first deployment._

---

## License

See [LICENSE](LICENSE).
