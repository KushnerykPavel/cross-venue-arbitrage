# Architecture

## Status

This document defines the physical module layout, dependency direction, and
the accepted high-level live, capture, and replay flows. Detailed semantics
live in the linked ADRs and design documents. Undocumented strategy,
execution, risk, concurrency, storage, or hot-path decisions still require
explicit design work before implementation.

## Repository layout

The repository is a monorepo:

- the repository root coordinates the backend, frontend, containers, and
  shared documentation;
- `backend/` is the virtual Cargo workspace for all Rust code;
- `backend/crates/` contains reusable backend modules;
- `backend/apps/` contains executable composition roots;
- `frontend/` contains the React application;
- `backend/config/` is reserved for versioned, non-secret backend
  configuration;
- `backend/benches/` and `backend/tests/` are reserved for workspace-level
  performance and integration verification.

Each Rust crate exposes its intended interface through `src/lib.rs`.
Internal file layout is not part of the crate interface.

## Module responsibilities

### Core modules

- `domain`: exact domain values and identifiers such as prices,
  quantities, symbols, orders, and venues.
- `market-data`: normalized market events, order-book state, and
  normalization logic.
- `venue`: the shared live-session runtime, transport seam, observation clock,
  lifecycle events, and protocol-neutral adapter contract.
- `strategy`: pure opportunity detection and profitability decisions.
- `execution`: execution coordination, order state, and hedging logic.
- `risk`: risk policy and decisions.
- `reconciliation`: comparison and repair of internal and venue state.
- `recorder`: append-only event recording and replay input support.
- `metrics`: operational and latency measurements.
- `engine`: orchestration of the backend modules.

### Venue adapters

- `venue-hyperliquid`
- `venue-lighter`
- `venue-aster`
- `venue-binance`

Each venue crate exposes whole-market adapters. The Binance adapter is split
into paired depth and trade sessions because USDⓈ-M Futures routes those
streams through separate WebSocket endpoints. Together, the adapters own
routing, normalization, recovery commands, an independent Order Book, and
bounded trade identity deduplication for every configured Market Coin. Venue
wire representations must not leak into the shared runtime or strategy logic.

### Applications

- `trader`: composition root for the live application.
- `replay`: composition root for deterministic replay.
- `export-parquet`: offline conversion from validated canonical captures to
  immutable analytical datasets.

Applications assemble modules. Business and market semantics belong in
the reusable crates rather than in application entry points.

## Dependency direction

Applications act as composition roots. Venue-specific crates depend on
`venue`, while `venue` depends only on stable domain and normalized market-data
concepts; it never depends on a venue-specific crate. Venue-specific
representations do not enter strategy logic, and stable domain concepts do not
depend on adapters or applications. Cycles between crates are not allowed.

## Shared live and replay core

Live and replay inputs must converge on the same normalized processing
path. The `trader` and `replay` applications may differ in their input
and output adapters, but strategy and state-transition logic must remain
shared.

## Reproducible market-data pipeline

The canonical research input is a versioned append-only normalized event log.
Live capture assigns a Capture Sequence before a bounded, non-blocking handoff
to a dedicated recorder thread; an accepted event then enters the synchronous
engine. Replay validates the complete capture before delivering those same
events to the same engine entry point in Capture Sequence order.

The MVP records full accepted L2 snapshots, public Market Trades, and Order
Book and Trade Stream availability for BTC perpetual on Binance, Aster,
Hyperliquid, and Lighter. Parquet is an immutable offline derivative for DuckDB and Polars, not
the replay source of truth. The exact contract, ownership, failure behavior,
filesystem format, and acceptance criteria are specified in
[the reproducible market-data pipeline design](docs/design/reproducible-market-data-pipeline.md).

## Open architecture decisions

Implementation must stop for explicit direction if it requires any of
the following before they are documented:

- canonical price and quantity scales;
- strategy-facing event semantics beyond the captured market-data variants;
- order-book reconstruction and recovery rules;
- execution timestamp representations;
- concurrency beyond the approved live runtime and recorder handoff;
- strategy signal and profitability semantics;
- execution, hedging, reconciliation, and risk policies;
- frontend-to-backend transport and authentication.

The initial Hyperliquid snapshot contract is resolved in
[ADR 0001](docs/adr/0001-hyperliquid-order-book-snapshots.md). The original
development runner in [ADR 0002](docs/adr/0002-hyperliquid-live-console-runner.md)
is superseded by the multi-market live-session boundary in
[ADR 0003](docs/adr/0003-hyperliquid-multi-market-live-session.md). These
decisions do not define the production transport. Lighter metadata resolution,
incremental reconstruction, sequence recovery, and live-session ownership are
resolved in [ADR 0004](docs/adr/0004-lighter-order-book-session.md).
Aster Futures market resolution, partial-depth snapshot semantics, and session
ownership are resolved in
[ADR 0005](docs/adr/0005-aster-partial-depth-session.md).
Binance Futures market resolution, top-20 partial-depth semantics, and session
ownership are resolved in
[ADR 0009](docs/adr/0009-binance-futures-partial-depth-session.md).
The single shared Configured Market Set for every live venue is defined in
[ADR 0006](docs/adr/0006-shared-market-configuration.md).
The common live runtime, deterministic whole-market adapter boundary, shared
clock, lifecycle vocabulary, and synchronous action execution are defined in
[ADR 0007](docs/adr/0007-shared-live-market-data-runtime.md).
The canonical capture, ordering, recorder handoff, storage, replay, and Parquet
contracts are defined in
[ADR 0008](docs/adr/0008-reproducible-market-data-capture.md).

## Verification

Tests should exercise each module through its public interface. Workspace
integration tests belong in `backend/tests/`; benchmarks with documented
methodology belong in `backend/benches/`.
