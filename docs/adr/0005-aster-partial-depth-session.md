# ADR 0005: Aster partial-depth session

## Status

Accepted on 2026-09-18.

## Context

Aster Futures V3 offers both diff-depth streams, which require a REST snapshot
and temporary event buffering, and partial-depth streams containing the current
top 5, 10, or 20 levels. The existing live path intentionally has no queue and
delivers complete normalized snapshots synchronously.

## Decision

- The shared Configured Market Set is supplied by the configuration contract in
  ADR 0006.
- Startup resolves each Market Coin through Futures V3 `exchangeInfo`. A market
  must match `baseAsset` exactly, use `USDT` as quote asset, have
  `contractType=PERPETUAL`, and have `status=TRADING`. The venue symbol is not
  hardcoded.
- One Aster Live Market Data Session owns one combined WebSocket connection and
  an independent Order Book for every configured market.
- The session uses `<symbol>@depth10@100ms`. Each partial-depth event replaces
  the complete normalized top-10 book for its market. It is not merged as a
  delta.
- Aster's final update ID `u` is retained as `source_sequence`; event time `E`
  is retained as the exchange timestamp. Aster does not provide per-level order
  counts, so `BookLevel.order_count` is `None`.
- Invalid identified input makes only its Market Coin unavailable. Input that
  cannot be routed does not invalidate another market.
- A socket disconnection makes every Aster book unavailable. Each becomes
  available independently after its next valid partial-depth snapshot.
- `trader` owns presentation and runs Aster alongside Hyperliquid and Lighter.
  Delivery is synchronous in socket receive order, with no application queue or
  shared mutable state.

## Consequences

The Aster view is intentionally limited to top 10 rather than reconstructing
the full depth. This avoids adding the buffering and recovery state required by
the diff-depth protocol and matches the current snapshot-oriented live path.
Changing to full-depth reconstruction requires a separate decision covering a
bounded bootstrap buffer and its overflow behavior.

The adapter reuses the existing Tokio, Tungstenite, Futures, Reqwest, Serde,
JSON, and Rustls dependencies; no new third-party crate is introduced.
