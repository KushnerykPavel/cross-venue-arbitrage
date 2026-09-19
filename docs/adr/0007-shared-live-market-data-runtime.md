# ADR 0007: Shared live market data runtime

## Status

Accepted on 2026-09-18. Supersedes the runtime ownership and public
live-session interface clauses in ADRs 0003, 0004, and 0005. Their
venue-specific protocol, normalization, and recovery rules remain in force.

## Context

The first three venue connectors each combined WebSocket lifecycle code with
venue protocol handling. This duplicated reconnect, shutdown, heartbeat,
transport, timing, and lifecycle-event behavior. Their public session and
per-market adapter types also exposed implementation structure to the
composition root.

The live pipeline still requires ADR 0002's synchronous receive ordering and
backpressure: all work caused by one frame must finish before another frame is
read. A shared runtime must preserve that property without moving protocol
decisions into a generic abstraction.

## Decision

- The `venue` crate owns one reusable `LiveMarketDataSession` runtime and the
  production Tungstenite transport.
- There is one runtime instance and one whole-market adapter instance per
  venue. Each adapter owns the independent Order Book for every member of the
  Configured Market Set.
- A venue adapter is a deterministic state machine. It receives connection,
  heartbeat, text-frame, and disconnection inputs and returns ordered,
  protocol-neutral actions. It performs no transport I/O.
- The runtime executes every returned action in order before polling the next
  frame. No application queue or shared mutable state is introduced.
- The runtime owns connection, reconnect delay, transport ping/pong, shutdown,
  and Local Receive Time sampling. Adapters own subscription messages,
  application heartbeats, routing, venue recovery commands, normalization, and
  Order Book state.
- `trader` creates one monotonic clock origin and injects clones into all three
  runtimes. Local Receive Time is sampled when a frame becomes available.
  Processing Completion Time is sampled after the identified market update is
  decoded, validated, and applied, immediately before publication.
- The runtime publishes one common event vocabulary: `Connecting`, `Connected`,
  `MarketSubscribed`, `OrderBookUpdated`, `OrderBookUnavailable`, and
  `Disconnected`.
- Unroutable input does not invalidate an Order Book. Invalid input associated
  with a configured Market Coin invalidates only that market. A fatal protocol
  action reconnects the venue.
- On disconnect, the adapter returns one `OrderBookUnavailable` action for each
  configured Market Coin in configuration order. The runtime publishes all of
  them before `Disconnected`. Each Order Book recovers only from its own fresh,
  valid snapshot.
- Venue metadata discovery remains a venue-local bootstrap concern outside the
  shared runtime.
- The transport boundary is injected internally. Production uses Tungstenite;
  runtime tests use a scripted in-memory transport.
- `venue-hyperliquid`, `venue-lighter`, and `venue-aster` depend on `venue`.
  `venue` does not depend on venue-specific crates. Applications remain the
  composition roots.
- The former public venue-specific live sessions, per-market adapters, outcome
  types, lifecycle events, shutdown aliases, and endpoint constants are
  removed without compatibility exports.

## Consequences

Transport lifecycle semantics now have one implementation and one deterministic
test seam. Venue crates retain their protocol differences, while `trader` owns
only configuration, composition, shutdown, and presentation. Slow presentation
continues to delay the next receive, so console timing is still not latency
evidence. Any future queue, parallel market processing, or clock-origin change
requires a new decision because it would alter ordering or timestamp meaning.
