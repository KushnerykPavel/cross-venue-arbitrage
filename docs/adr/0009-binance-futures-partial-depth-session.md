# ADR 0009: Binance Futures market-data sessions

## Status

Accepted on 2026-09-23.

## Context

Binance USDⓈ-M Futures provides both diff-depth and partial-depth market
streams. The current shared runtime delivers complete normalized snapshots
synchronously and has no approved bounded bootstrap buffer for reconstructing a
diff-depth book.

## Decision

- Add `venue-binance` as an independent whole-market adapter.
- Resolve configured market coins through Binance Futures `exchangeInfo`.
- Accept only active USDT perpetual markets and retain Binance's resolved
  symbol in capture metadata.
- Use one combined WebSocket session for `<symbol>@depth10@100ms` on
  `wss://fstream.binance.com/public/stream?streams=...` and one combined
  WebSocket session for `<symbol>@aggTrade` on
  `wss://fstream.binance.com/market/stream?streams=...` for every configured
  market. Binance's USDⓈ-M Futures routing does not deliver market-category
  aggregate trades through the public endpoint ([stream routing reference](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice)).
- Treat each partial-depth message as a complete top-10 snapshot, not as a
  delta. Retain the final update ID as `source_sequence` and Binance event time
  as the exchange timestamp.
- Preserve Binance aggregate-trade identity (`a`, `f`, `l`) for deduplication,
  recording, replay, and Parquet export.

## Consequences

Binance provides a higher-frequency top-10 L2 view and aggregate trades while
preserving the existing synchronous runtime and normalized event contract.
The two sessions have independent reconnect lifecycles; both feed the shared
capture sequence and engine path. Levels beyond the top 10 are not captured.
Switching this adapter to full diff-depth reconstruction requires a separate
decision covering REST snapshot bootstrap, buffered updates, sequence-gap
recovery, and bounded overflow behavior.
