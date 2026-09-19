# ADR 0002: Hyperliquid live console runner

## Status

Superseded on 2026-09-17 by
[ADR 0003](0003-hyperliquid-multi-market-live-session.md).

## Context

The Hyperliquid snapshot adapter is deterministic but needs a small live
composition root so a developer can observe BTC perpetual order-book data.
This runner is for connectivity and correctness checks, not latency
benchmarking or production trading.

## Decision

- The `trader` application owns one WebSocket connection and one
  `HyperliquidOrderBookAdapter` in a single Tokio task.
- Messages are decoded and printed before the next message is consumed. There
  is no internal queue, shared mutable state, or additional worker task.
- The runner subscribes only to the BTC perpetual twenty-level `l2Book` feed.
- It sends Hyperliquid's application-level ping every fifty seconds.
- A disconnect marks the book unhealthy. The runner reconnects with bounded
  exponential delays of 2, 4, 8, 16, and 30 seconds, then stays at 30 seconds.
- A successful connection resets the reconnect delay after the first valid
  snapshot.
- Invalid snapshots are reported and withheld; the same connection remains
  open so the next complete snapshot can restore health.
- Ctrl-C stops the application. Connection failures are reported to stderr.
- Each accepted snapshot prints its best bid, best ask, quantities, depth,
  and three distinct raw timestamp values to stdout.

## Ownership and failure behavior

- **Owner:** the `trader` task owns the socket and adapter.
- **Producer:** Hyperliquid's WebSocket server.
- **Consumer:** the same `trader` task.
- **Ordering:** processing follows WebSocket receive order.
- **Capacity:** no application queue exists.
- **Backpressure:** decoding and console output complete before the next read.
- **Shutdown:** Ctrl-C exits the receive or reconnect wait.
- **Failure:** disconnects invalidate the book and enter bounded reconnect;
  malformed snapshots invalidate the book and are logged.

## Dependencies

- `tokio` supplies the async runtime, timers, and Ctrl-C handling.
- `tokio-tungstenite` supplies WebSocket framing and Rustls TLS.
- `rustls` selects the `ring` process-level crypto provider explicitly.
- `futures-util` supplies stream and sink extension traits.

These dependencies exist only in the application composition root. They do
not enter canonical domain arithmetic or deterministic order-book logic.

## Consequences

Console I/O may block message consumption, so output from this runner must not
be used as latency evidence. A production collector will require a separately
approved bounded handoff and observability design.
