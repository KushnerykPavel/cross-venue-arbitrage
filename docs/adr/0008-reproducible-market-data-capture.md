# ADR 0008: Reproducible market-data capture

## Status

Accepted on 2026-09-18.

## Context

Cross-venue spread, lead/lag, order-flow, and later market-making research need
the exact normalized event order observed by the live process. The repository
has normalized Order Books and a shared live runtime, but its recorder and
replay applications are empty. Local receive times are comparable only within
one run, venue timestamps do not define a cross-venue order, and the three
venues have different Order Book and public-trade semantics.

The first dataset must remain small enough to inspect and explain. It targets a
thirty-minute BTC perpetual capture from Binance, Aster, Hyperliquid, and Lighter on the
existing VPS. Raw WebSocket frames, compression, automatic retention, funding,
and private execution data are excluded from this MVP.

## Decision

- A `Capture Run` is identified by a UUID and starts a gap-free
  `Capture Sequence` at one. A restart always creates a new Capture Run; event
  identity is `(capture_id, capture_sequence)`.
- The canonical replay truth is an append-only normalized event log. It stores
  full accepted `OrderBookSnapshot` values, `OrderBookUnavailable`, public
  `MarketTrade`, `TradeStreamUnavailable`, and `TradeStreamResumed` events.
- Live and replay send the same owned normalized event model through the same
  engine entry point. Exchange time never determines replay order.
- Each venue uses one WebSocket for its Order Book and trade subscriptions.
- Aster aggregate trades remain aggregates. Lighter ordinary, liquidation,
  deleverage, and market-settlement reports are retained. Venue-native trade
  identities are preserved and used for adapter-level deduplication when the
  venue provides a reliable identity.
- Trade deduplication is bounded independently for every `(venue, market)`.
  `TRADE_DEDUP_CAPACITY` defaults to 1,000,000 identities. Identities remain
  resident until the Capture Run ends and are never evicted; exhausting the
  capacity stops the run fail-closed rather than admitting a possible
  duplicate.
- The live path assigns Capture Sequence, creates an owned event, and performs
  a non-blocking send to a bounded queue of initially 4,096 events. Only a
  successfully accepted event enters the engine. Queue saturation stops the
  whole Capture Run as incomplete; events are never silently dropped.
- A dedicated recorder thread owns Postcard serialization, batching, CRC32C,
  segment rotation, filesystem writes, periodic `fdatasync`, recovery, SHA-256
  segment hashes, and manifest publication.
- Storage DTOs are immutable and versioned independently of domain structs.
  Each segment contains exactly one format version. Unknown versions fail
  explicitly; migrations create a new dataset and never rewrite their source.
- Segments rotate after five minutes or one GiB, whichever occurs first. The
  active `.open` file is not replayable. Finalized `.log` files are published
  through flush, `fdatasync`, SHA-256, atomic rename, and atomic manifest
  replacement.
- A process restart repairs the previous `.open` tail to the last valid framed
  record, finalizes that run as `incomplete_process_crash`, and begins a new
  Capture Run.
- The canonical log is Rust-owned. Python, Polars, and DuckDB consume an
  immutable Parquet derivative, not the binary segments.
- Recorder completeness and venue data quality are separate. A complete
  Capture Run may contain documented disconnects or trade-stream gaps.
- The MVP stores data under a configurable absolute `DATA_DIR` outside the
  repository, performs no automatic deletion, and records for thirty minutes.
  A result of forty GiB or more fails the MVP acceptance criterion.

## Consequences

The same log and replay configuration can reproduce the observed event order
and strategy inputs. The normalized-only MVP cannot re-run historical frames
through a corrected decoder; extending capture to raw frames requires a later
decision. Full Order Book snapshots and owned queue values favor correctness
and simplicity over storage and copy efficiency. Compression, delta encoding,
queue sizing, and retention remain measurement-driven follow-ups after the MVP
report.

The recorder queue is the first approved application queue in the live path.
It does not weaken ADR 0007's synchronous per-frame processing: enqueue and
engine processing complete before the next frame is consumed, and overload
fails the capture instead of hiding backpressure or losing data.
