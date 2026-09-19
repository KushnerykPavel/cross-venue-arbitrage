# ADR 0004: Lighter incremental order-book session

## Status

Accepted on 2026-09-17.

## Context

Lighter addresses order-book subscriptions by numeric Venue Market ID rather
than Market Coin. A subscription begins with a `subscribed/order_book` snapshot
and continues with `update/order_book` price-level changes. Updates contain
`begin_nonce` and `nonce`; zero quantity removes a price level.

Unlike Hyperliquid, Lighter does not publish an order count for each aggregated
level. Treating every update as a complete snapshot or inventing an order count
would corrupt normalized state.

## Decision

- The shared Configured Market Set is supplied by the configuration contract in
  ADR 0006.
- At startup, the Lighter adapter resolves each configured Market Coin through
  the public `orderBooks` metadata endpoint. Unknown, inactive, and
  non-perpetual markets are rejected. Venue Market IDs are not hardcoded.
- One Lighter Live Market Data Session owns one WebSocket connection and an
  independent reconstructed Order Book for each configured Market Coin.
- A `subscribed/order_book` frame replaces all reconstructed levels and installs
  its `nonce`. An `update/order_book` frame is accepted only when its
  `begin_nonce` equals the installed `nonce`.
- Positive-size updates replace a price level; an exact decimal zero removes it.
  Prices and quantities retain the canonical exact-decimal representation.
- A nonce gap or malformed identified book frame makes only that Market Coin
  unavailable. The session unsubscribes and resubscribes that market and waits
  for a fresh snapshot before accepting more deltas.
- A socket disconnection makes every Lighter Order Book unavailable until each
  receives a fresh valid subscription snapshot.
- Every accepted state is published as a normalized full-depth
  `OrderBookSnapshot`. Lighter's `nonce` is retained as `source_sequence`.
- Normalized `BookLevel.order_count` is optional: Hyperliquid provides it and
  Lighter does not.
- Lighter server `ping` messages receive application-level `pong` responses.
- `trader` runs the Hyperliquid and Lighter sessions concurrently in one Tokio
  task and owns presentation. Each session synchronously delivers updates in
  its socket receive order, with no queue or shared mutable state.

## Consequences

Sequence gaps cannot silently contaminate a Lighter book, and recovery remains
isolated to the affected Market Coin. Rebuilding a full normalized snapshot
after every delta is intentionally correctness-first and may be optimized only
after measurement without changing the observable contract.

The startup metadata request adds `reqwest` with Rustls. Lighter WebSocket
ownership uses the same Tokio, Tungstenite, Futures, and Rustls dependencies as
the Hyperliquid live session.
