# ADR 0001: Hyperliquid order-book snapshot contract

## Status

Accepted on 2026-09-17.

## Context

Hyperliquid's `l2Book` feed publishes complete book snapshots rather than
sequenced deltas. The public schema provides no sequence number, gap marker,
or checksum, and does not normatively define the meaning of `WsBook.time`.

The first implementation is limited to the BTC perpetual and deliberately
excludes live WebSocket ownership, reconnect scheduling, and channel sizing.

## Decision

- Subscribe to the BTC perpetual `l2Book` feed with `fast: false` for twenty
  levels per side.
- Represent prices and quantities exactly as a normalized positive `i128`
  coefficient plus an explicit decimal scale. Binary floating point is not
  used.
- Preserve the exchange timestamp as an opaque raw integer. Preserve caller-
  supplied receive and processing clock values separately and without
  assuming they share a clock domain.
- Treat every valid message as a complete replacement of previous book state.
- Sort bids descending and asks ascending before publication.
- Reject malformed values, non-positive values, zero order counts, duplicate
  prices, empty sides, and locked or crossed snapshots.
- Mark the book unhealthy after invalid input or disconnection. Do not expose
  the last snapshot as current until a fresh valid snapshot is applied.
- The Hyperliquid adapter may depend on `domain` and `market-data`.
- Implement decoding, normalization, replacement, and deterministic tests
  before introducing live transport.

## Consequences

The adapter does not claim that `WsBook.time` is a sequence or an exchange
latency timestamp. Snapshot replacement makes recovery independent of missing
intermediate publications, but consumers must treat the book as unavailable
between disconnection or rejection and the next valid snapshot.

The exact-decimal parser is intentionally bounded to eighteen fractional
digits and to values that can be safely compared after checked scaling in an
`i128`. Unsupported values fail explicitly.
