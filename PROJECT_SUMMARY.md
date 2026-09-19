# Project Summary

## Goal

Build a compact Rust research/execution platform for learning market
microstructure and demonstrating HFT/low-latency engineering skills.

The initial strategy is **cross-venue lead/lag arbitrage**, not because
market making is discarded, but because lead/lag gives a strong first
project for:

-   multi-venue market-data handling;
-   local L2 order-book reconstruction;
-   timestamping and latency measurement;
-   deterministic event replay;
-   signal research;
-   execution modelling;
-   low-latency architecture.

Market making remains a second strategy. Later it can use an external
venue as a fair-value signal for quoting on Lighter.

## Initial venues and instrument

Initial venues:

1.  Aster
2.  Hyperliquid
3.  Lighter

Initial instrument: **BTC perpetual**.

BTC is deliberately chosen for the first iteration because it provides
dense market data, strong liquidity and a demanding environment for
validating the infrastructure. Lower-cap instruments can be added later
to investigate larger but less liquid dislocations.

## Strategy roadmap

The intended order is:

1.  Cross-venue lead/lag research.
2.  Shadow execution.
3.  Small controlled live execution.
4.  Market making using cross-venue fair value.
5.  Optional additional research:
    -   order-flow imbalance / microprice;
    -   liquidation/event-driven signals;
    -   funding/basis opportunities.

## Required market data

For every venue, collect where available:

-   L2 order-book updates;
-   snapshots required for reconstruction/recovery;
-   best bid/ask derived from every accepted L2 state rather than recorded as
    a separate MVP feed;
-   public trades and aggressor side when available;
-   exchange timestamps;
-   local receive timestamps;
-   internal processing timestamps;
-   sequence/update identifiers.

For live execution later, additionally record:

-   order send time;
-   exchange acknowledgement;
-   partial/full fills;
-   cancels/rejects;
-   fees/rebates;
-   position and risk state.

## Storage

The trading hot path must not write synchronously to a database.

The initial storage pipeline is:

`venue feed -> adapter -> normalized event -> bounded handoff -> recorder thread -> append-only binary log`

The binary event log is the replay source of truth. Offline jobs can
convert it to **Parquet**. **DuckDB** and Python/Polars are used for
research and analytics.

The first storage MVP is a thirty-minute BTC perpetual Capture Run across
Aster, Hyperliquid, and Lighter. It records full accepted L2 snapshots, public
Market Trades, and availability transitions into uncompressed, versioned
Postcard segments. Capture Sequence defines deterministic replay order; a
bounded recorder handoff fails the entire run rather than dropping events.
Detailed schemas, durability behavior, validation, Parquet layout, and success
criteria are specified in
[`docs/design/reproducible-market-data-pipeline.md`](docs/design/reproducible-market-data-pipeline.md).

ClickHouse, Kafka and Redis are intentionally excluded from the first
version unless a demonstrated requirement appears.

## Infrastructure

An existing European VPS is sufficient for the research/data-collection
phase.

Before optimizing geography, measure:

-   feed latency;
-   RTT where meaningful;
-   receive-to-process latency;
-   p50/p95/p99/p99.9;
-   jitter;
-   disconnect/recovery behaviour;
-   cross-venue event timing.

Only after identifying a real network bottleneck should identical
collectors be temporarily benchmarked from other regions/providers.

## Technology

Core implementation: **Rust**.

Tokio is appropriate at asynchronous network boundaries. The
latency-sensitive event-processing path should remain simple and
explicit, with dedicated processing where justified rather than
spreading async abstractions through the entire engine.

Python is for research/analysis, not the production hot path.

## Development philosophy with Codex

Codex is an implementation assistant, not the architecture owner.

The human owns:

-   architecture;
-   domain semantics;
-   trading assumptions;
-   invariants;
-   risk decisions;
-   performance trade-offs.

Codex receives small, reviewable tasks. It must not perform broad
unsolicited refactors or invent new architecture. If a task conflicts
with an invariant or requires an architectural decision, it stops and
reports the decision point.

The project should remain small enough that the human can explain every
important component.
