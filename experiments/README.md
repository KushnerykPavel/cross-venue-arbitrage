# Python experiments

This directory contains exploratory research against exported canonical
captures. Rust capture, validation, replay, and production logic remain under
`backend/`; notebooks are for research and visualisation only.

## Layout

```text
experiments/
  data/                         # local Parquet data; ignored by Git
    parquet/
      btc-capture-001/
  notebooks/                    # Jupyter notebooks
  requirements.txt              # Python research dependencies
```

Copy the exported dataset from the VPS into the project directory:

```bash
mkdir -p experiments/data/parquet
rsync -avh --info=progress2 --partial \
  root@vps-5a3ce21c:/srv/cross-venue-arb-data/parquet/btc-capture-001/ \
  experiments/data/parquet/btc-capture-001/
```

Create the local environment and start Jupyter:

```bash
python3 -m venv experiments/.venv
source experiments/.venv/bin/activate
pip install -r experiments/requirements.txt
jupyter lab experiments/notebooks
```

Start with `01_bbo_overview.ipynb`. It uses lazy Polars scans and does not
load the 90-million-row `order_book_levels` table unless explicitly needed.

## Offline simulator

The research simulator is under `experiments/simulator/`. It derives bounded
top-N books from the L2 Parquet table and separates:

```text
market-data delivery → TraderSimulator → strategy → order intent
                                          ↓
                                  ExchangeSimulator
                                          ↓
                                  fills and PnL
```

It models independent market-data and order latency, market and limit orders,
partial fills, cancellation/expiration states, fees, inventory, and separated
realized/unrealized/net PnL accounting.

The arbitrage strategy triggers on executable `net_edge_bps`, calculated from
the requested quantity across L2 levels after both venue fees and configured
slippage. `minimum_net_edge_bps` is the primary threshold; the old
`minimum_edge_bps` field is retained only for compatibility.

The signal is latency-aware: market-data latency delays the state visible to
the strategy, order latency delays activation at the exchange, quotes must be
within `max_quote_skew_ns`, and `latency_buffer_bps` is added to the required
net edge. Orders may also receive a configured `default_order_ttl_ns`.

```bash
source experiments/.venv/bin/activate
python -m experiments.simulator.run \
  --dataset experiments/data/parquet/btc-capture-001 \
  --max-depth 10 \
  --output experiments/data/results/cross-venue-arbitrage
```

The output directory contains `orders.parquet`, `fills.parquet`,
`equity.parquet`, `inventory.json`, and `summary.json`.

Research notebooks:

1. `01_data_validation.ipynb`
2. `02_derived_order_books.ipynb`
3. `03_cross_venue_arbitrage.ipynb`
4. `04_execution_and_pnl_analysis.ipynb`

This is a research simulator, not a live execution path. Its float conversion
occurs only at the Python analysis boundary; canonical Rust data remains exact.

## Research boundary

Notebook results are exploratory. Any strategy semantics, fill model, fees,
latency assumptions, or PnL logic that becomes part of the system must later
be specified and implemented in the shared deterministic replay path.
