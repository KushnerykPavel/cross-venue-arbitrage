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

Default taker fees match the account tiers in use: Aster base tier at 4 bps
and Lighter Standard at 0 bps. `taker_speed_bump_ns` adds Lighter Standard's
300 ms venue delay to market orders.

A cancel takes effect after the venue's order latency plus
`cancel_speed_bump_ns` (Lighter Standard: 300 ms). Until then, the target
order can still fill; a cancel that arrives too late is recorded as rejected.
Every fill is still charged the taker fee; a maker fill model is not
implemented yet.

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
  --split train \
  --max-depth 10 \
  --output experiments/data/results/cross-venue-arbitrage-train
```

The output directory contains `orders.parquet`, `fills.parquet`,
`equity.parquet`, `inventory.json`, and `summary.json`.

For one capture, run again with `--split validation` and `--split test`, using
separate output directories. The split points are 60% and 80% of elapsed local
receive time. Each interval is a contiguous Capture Sequence range; the test
interval is held out until the strategy parameters have been chosen using train
and validation. The existing `CrossVenueArbitrageStrategy` is the baseline.
Its fees, latency, and slippage are simulator assumptions, and its `net_pnl`
is a simulated outcome rather than an observed executable return. Each split
starts with empty book and portfolio state, so its first snapshots warm it up.

Research notebooks:

1. `01_data_validation.ipynb`
2. `02_derived_order_books.ipynb`
3. `03_cross_venue_arbitrage.ipynb`
4. `04_execution_and_pnl_analysis.ipynb`
5. `05_cont_kukanov_stoikov_ofi.ipynb` — Level I order flow imbalance on
   Binance, with train/validation price-impact and next-second signal checks;
   the held-out test set is not loaded.
6. `07_lead_lag.ipynb` — Binance → Aster/Lighter lead/lag: response to Binance
   moves and taker round-trip edge on the lag venue (`experiments/analysis/`);
   train and validation only.

This is a research simulator, not a live execution path. Its float conversion
occurs only at the Python analysis boundary; canonical Rust data remains exact.

## Research boundary

Notebook results are exploratory. Any strategy semantics, fill model, fees,
latency assumptions, or PnL logic that becomes part of the system must later
be specified and implemented in the shared deterministic replay path.
