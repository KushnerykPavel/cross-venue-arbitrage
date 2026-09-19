# Lighter order-book protocol research

Research date: 2026-09-17.

## Sources

- [Official Lighter Python SDK WebSocket client](https://github.com/elliottech/lighter-python/blob/main/lighter/ws_client.py)
- [Official Lighter Python SDK](https://github.com/elliottech/lighter-python)
- Public mainnet metadata: `GET https://mainnet.zklighter.elliot.ai/api/v1/orderBooks`
- Public mainnet WebSocket: `wss://mainnet.zklighter.elliot.ai/stream`

## Confirmed contract

- Public order-book data requires no credentials.
- The client subscribes with
  `{"type":"subscribe","channel":"order_book/{market_id}"}`.
- The server channel is returned as `order_book:{market_id}`.
- `subscribed/order_book` contains a full `order_book` with `bids`, `asks`,
  `nonce`, `begin_nonce`, `offset`, and `last_updated_at`.
- `update/order_book` contains changed price levels. A size such as `0.00000`
  deletes that price.
- The observed incremental continuity relation is the update's `begin_nonce`
  equal to the previously accepted book `nonce`. `offset` is retained by the
  wire protocol but is not used as the continuity counter.
- Levels contain decimal strings named `price` and `size`; no order-count field
  is present.
- Server messages with `type: "ping"` require an application-level
  `{"type":"pong"}` response.
- On 2026-09-17 the public metadata identified active perpetuals ETH as market
  0, BTC as market 1, and SOL as market 2. These observations are not embedded
  in code; the adapter resolves the current mapping at startup.

The adapter preserves `last_updated_at` as the opaque exchange timestamp and
does not infer a clock unit in canonical state.
