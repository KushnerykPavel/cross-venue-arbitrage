# ADR 0003: Hyperliquid multi-market live session

## Status

Accepted on 2026-09-17. Supersedes ADR 0002.

## Context

The first console runner combined WebSocket lifecycle, subscription,
normalization, book state, and presentation inside `trader`. Supporting more
than one Hyperliquid perpetual would duplicate protocol decisions in the
application and leave reconnect recovery difficult to test independently.

Hyperliquid supports multiple `l2Book` subscriptions on one WebSocket. Each
message identifies its Market Coin and carries a complete snapshot.

## Decision

- The shared Configured Market Set is supplied by the configuration contract in
  ADR 0006.
- Entries are trimmed. Empty entries, remaining whitespace or control
  characters, and exact duplicates are rejected at startup. Case and
  punctuation are preserved; no narrower identifier grammar is invented.
- `backend/.env` supplies local values and is ignored by version control.
  `backend/.env.example` is committed. Values already present in the process
  environment take precedence over the file.
- The Hyperliquid Live Market Data Session owns one WebSocket, all `l2Book`
  subscriptions, heartbeat, reconnect timing, frame routing, and an independent
  Order Book for each configured Market Coin.
- The session and its books remain in one Tokio task. Normalized snapshots are
  delivered synchronously in receive order before the next frame is consumed.
  There is no queue or shared mutable state.
- An invalid snapshot makes only its identified Market Coin unavailable. Input
  that cannot be associated with a configured Market Coin does not invalidate
  another book.
- A socket disconnection makes every configured Order Book unavailable. Each
  book becomes available independently only after its own fresh valid snapshot.
- `trader` owns presentation and the Ctrl-C shutdown signal. It receives session
  lifecycle and normalized snapshot events through a synchronous callback.
- Market existence is not checked at startup. A later metadata capability may
  add exchange validation without narrowing the identifier syntax locally.

## Ownership and failure behavior

- **Owner:** one Hyperliquid Live Market Data Session owns the socket and all
  configured Hyperliquid books.
- **Producer:** Hyperliquid's WebSocket server.
- **Consumer:** the synchronous callback supplied by `trader`.
- **Ordering:** callback delivery follows WebSocket receive order.
- **Capacity:** no application queue exists.
- **Backpressure:** routing, normalization, state replacement, and presentation
  complete before the next frame is consumed.
- **Shutdown:** the caller-provided signal closes an active socket or cancels a
  reconnect wait.
- **Failure:** malformed identified snapshots are isolated per coin;
  disconnection invalidates all books and enters bounded reconnect.

## Consequences

Venue protocol and recovery behavior now live behind one reusable session
boundary, while `trader` is limited to configuration and presentation. Console
output still participates in backpressure and therefore remains unsuitable as
latency evidence. Supporting many markets may later require a separately
approved handoff design, but this ADR intentionally introduces no queue.
