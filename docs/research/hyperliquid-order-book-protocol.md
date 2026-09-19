# Hyperliquid `l2Book` WebSocket protocol research

Research date: 2026-09-17. This note uses only Hyperliquid's official documentation and repositories. It distinguishes explicit protocol guarantees from inferences that an adapter must not silently depend on.

## Endpoint and subscription lifecycle

Hyperliquid documents these public WebSocket endpoints:

- Mainnet: `wss://api.hyperliquid.xyz/ws`
- Testnet: `wss://api.hyperliquid-testnet.xyz/ws`

Subscribe with a JSON text message:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l2Book",
    "coin": "BTC"
  }
}
```

The `l2Book` subscription also accepts optional `nSigFigs`, `mantissa`, and `fast` fields. `fast: true` selects five levels and `fast: false` selects twenty levels according to the current subscription documentation. The documentation does not explicitly state the value used when `fast` is omitted.

A successful request first receives an acknowledgement with `channel: "subscriptionResponse"`; its `data` echoes the subscription request. Book data then arrives on `channel: "l2Book"`. An unsubscribe request uses `method: "unsubscribe"` and a `subscription` object matching the original subscription.

Source: [WebSocket endpoints and connection lifecycle](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket), [subscription request, acknowledgement, options, and unsubscribe](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions).

### Instrument identifiers

The same API covers perpetuals and spot:

- A perpetual `coin` is the name returned by the perpetual `meta` response.
- Spot uses `PURR/USDC` for PURR and `@{index}` for other spot pairs, where the index comes from `spotMeta.universe`.
- Display names can differ from HyperCore names; the docs give UI `BTC/USDC` versus HyperCore `UBTC/USDC` as an example.

Source: [Info API: perpetuals versus spot](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#perpetuals-vs-spot).

## Book message schema

The wire envelope and payload are:

```typescript
type Message = {
  channel: "l2Book";
  data: {
    coin: string;
    levels: [WsLevel[], WsLevel[]];
    time: number;
  };
};

type WsLevel = {
  px: string;
  sz: string;
  n: number;
};
```

`px` is price, `sz` is aggregate size, and `n` is the number of orders at the level. Hyperliquid's notation defines size as units of the coin/base currency.

The official Python SDK demonstrates that `levels[0]` is bids and `levels[1]` is asks: its `side_to_uint` maps bid (`"B"`) to `0` and ask (`"A"`) to `1`, then indexes `book_data["levels"]` with that value. The REST example also shows the first array with decreasing bid prices and the second with an ask price. However, the API documentation does **not** explicitly make within-side sorting a normative guarantee. A defensive adapter should parse all levels and sort bids descending and asks ascending, while rejecting crossed or malformed snapshots according to its chosen validation policy.

Sources: [official WebSocket type definitions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#data-type-definitions), [official Python SDK book-side mapping](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/examples/basic_adding.py), [L2 snapshot response example](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#l2-book-snapshot), [API notation](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/notation).

## Snapshot semantics, ordering, and gaps

`l2Book` is explicitly documented as a **snapshot feed**. Every published `l2Book` payload is a replacement view of the requested depth, not a delta to apply to prior state. The type comment says it is pushed on a block when at least `0.5` has elapsed since the last push; the comment does not name the unit, so code should not treat that text as a precise latency SLA.

The schema has no sequence number, block height, checksum, previous-sequence link, or explicit gap indicator. Consequently:

- there is no documented way to prove that every publication was received;
- there is no delta chain to repair, because each received payload is independently usable as a snapshot;
- `time` may be used as a monotonicity/staleness guard only after its semantics are confirmed; it must not be represented as an exchange sequence number;
- reconnect recovery consists of reconnecting and resubscribing, then replacing local state with a newly received snapshot.

Hyperliquid warns that server-side disconnects can occur periodically and without announcement, requires automated clients to reconnect gracefully, and says missed data is present in the snapshot acknowledgement on reconnect or may be queried via the corresponding info request. For `l2Book`, the channel's recurring snapshot semantics are the relevant recovery mechanism; the acknowledgement itself merely echoes the subscription in the documented envelope.

Source: [snapshot-feed declaration and schema](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#data-type-definitions), [disconnect and reconnect guidance](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket).

## Timestamp

`WsBook.time` is a JSON number. The official example uses `1754450974231`, which has the shape of Unix epoch milliseconds, but the `WsBook` definition does not document the unit or whether it is block time, snapshot construction time, or API-server emission time. Other API fields explicitly say “millis,” while this field does not.

A correctness-preserving adapter should therefore retain the raw integer, separately record local receive time, and avoid claiming precise exchange-latency or block-order semantics until Hyperliquid provides a normative definition or controlled API observations establish an operational contract.

Source: [WebSocket `WsBook` definition](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions#data-type-definitions), [official L2 response example](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#l2-book-snapshot).

## Price, size, aggregation, and depth

Prices and sizes are decimal **strings** on the wire. They should be parsed with exact decimal or fixed-point arithmetic rather than binary floating point.

Hyperliquid documents these precision rules for valid trading values:

- Prices allow at most five significant figures and at most `MAX_DECIMALS - szDecimals` fractional decimal places; `MAX_DECIMALS` is six for perpetuals and eight for spot. Integer prices are always allowed.
- Sizes are rounded to the asset's `szDecimals`, obtained from the relevant metadata response.
- The docs recommend removing trailing zeroes for signing, but do not promise a canonical string representation for market-data output. Consumers should therefore accept ordinary JSON decimal spellings allowed by their exact-decimal parser rather than compare raw strings.

The default/native book can be aggregated by passing `nSigFigs` values `2`, `3`, `4`, or `5`; `null` means full precision. `mantissa` is permitted only with `nSigFigs: 5` and accepts `1`, `2`, or `5`. The REST L2 snapshot returns at most twenty levels per side. Current WebSocket docs define `fast` depth as five and slow depth as twenty.

Sources: [tick and lot size](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size), [L2 aggregation and REST depth](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint#l2-book-snapshot), [WebSocket depth option](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions).

## Heartbeats and reconnect behavior

The server closes a connection if it has not sent any message on that connection for 60 seconds. For quiet subscriptions, the client can send:

```json
{ "method": "ping" }
```

The server responds with:

```json
{ "channel": "pong" }
```

The official Python SDK sends this application-level ping every 50 seconds. That interval is an SDK implementation choice, not a stated protocol requirement. The public documentation prescribes graceful reconnect but does not prescribe backoff, jitter, a resume token, or a maximum retry frequency beyond the connection rate limit. An adapter should reconnect with bounded exponential backoff and jitter, respect the connection-rate ceiling, resubscribe only after the socket opens, and withhold market data from downstream consumers until a fresh valid book snapshot is received.

Sources: [timeouts and application heartbeat](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats), [official Python SDK heartbeat implementation](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/websocket_manager.py), [disconnect guidance](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket).

## Documented rate limits

The WebSocket limits below apply per IP address:

- 10 simultaneous WebSocket connections;
- 30 new WebSocket connections per minute;
- 1,000 WebSocket subscriptions;
- 10 unique users across user-specific WebSocket subscriptions;
- 2,000 messages **sent to Hyperliquid** per minute across all WebSocket connections;
- 100 simultaneous in-flight post messages across all WebSocket connections.

The outbound-message limit includes application messages such as subscriptions and heartbeats; it is not documented as a cap on market-data messages received from Hyperliquid. If REST is used for recovery or comparison, the aggregate REST allowance is 1,200 weight per minute and an `l2Book` info request has weight 2.

Source: [rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits).

## Exact unresolved assumptions for adapter implementation

These points are not normatively resolved by the official public documentation and must be made explicit in the adapter design:

1. **Timestamp contract:** whether `WsBook.time` is Unix epoch milliseconds, which component assigns it, and whether it is guaranteed nondecreasing for one `(connection, coin, aggregation)` stream.
2. **Within-side ordering:** whether levels are guaranteed best-to-worst. The official example and SDK imply `[bids, asks]` with the best level first, but the docs do not state a sorting guarantee. The adapter can remove this dependency by sorting parsed levels itself.
3. **Omitted `fast` default:** the docs specify five levels for fast and twenty for slow, but do not explicitly say whether omission means slow. Send `fast: false` when twenty-level depth is required and test server acceptance.
4. **Publication cadence wording:** the snapshot type comment says “each block that is at least 0.5 since last push” without naming a unit or formalizing behavior. Do not build timeout or freshness guarantees from it.
5. **Duplicate and out-of-order delivery:** no guarantee is stated. The adapter needs a policy based on raw `time` plus receive time, while avoiding false claims that `time` is a sequence.
6. **Numeric lexical grammar:** `px` and `sz` are strings, but output canonicalization (trailing zeroes, exponent notation, maximum magnitude) is not specified. Use a bounded exact-decimal parser and reject unsupported representations explicitly.
7. **Malformed/crossed snapshot policy:** the protocol does not say how consumers should handle duplicate price levels, zero/negative values, an empty side, or best bid greater than/equal to best ask. The adapter must define whether to reject the snapshot, merge duplicates, or publish degraded state.
8. **Subscription failure envelope:** the reviewed pages document successful acknowledgements but not a complete error schema for invalid coins/options or rate limiting. Unknown/error messages must be surfaced without terminating the read loop, and integration tests should capture actual server responses.
9. **Reconnect readiness:** the docs say reconnect and recover via snapshots but do not define a resumable cursor. Treat the book as unavailable from disconnect until the first valid post-resubscription `l2Book` payload; do not replay the previous snapshot as current.
10. **Spot metadata mapping lifecycle:** spot identifiers and UI remappings require metadata. The docs do not specify an atomic change notification tied to a book subscription, so metadata refresh/versioning must be designed separately if spot is in scope.

These uncertainties do not prevent a safe snapshot adapter. They do prevent treating `l2Book` as a sequenced event log or using its timestamp as a proven exchange sequence/latency measurement.
