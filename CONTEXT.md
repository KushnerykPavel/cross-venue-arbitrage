# Cross-Venue Arbitrage

This context describes the market data language used by the arbitrage engine when observing perpetual markets across trading venues.

## Language

**Market Coin**:
A venue-defined identifier for a perpetual market, such as `BTC`, `ETH`, `SOL`, or a qualified HIP-3 market name. Its case and punctuation are significant and are preserved exactly.
_Avoid_: Token, asset, symbol

**Configured Market Set**:
The distinct Market Coins observed together during one trader run.
_Avoid_: Token list, symbol list

**Order Book**:
The latest accepted full depth state for one Market Coin at one venue, together with whether that state is currently available for decisions. It may come from complete venue snapshots or from validated incremental reconstruction.
_Avoid_: Book cache, price book

**Venue Market ID**:
An exchange-assigned identifier used to address a Market Coin on a specific venue. It is metadata rather than the canonical name of the Market Coin.
_Avoid_: Symbol, coin ID

**Live Market Data Session**:
A continuous observation period for the complete Configured Market Set at one venue. Connection loss makes every Order Book in the session unavailable; each recovers independently from fresh valid market data.
_Avoid_: WebSocket client, connection manager

**Local Receive Time**:
The monotonic instant when a venue frame becomes available to its Live Market Data Session. Every venue in one trader run shares the same clock origin.
_Avoid_: Arrival timestamp, receive wall time

**Processing Completion Time**:
The monotonic instant after a venue frame has been decoded, validated, and applied to market state, measured from the same origin as Local Receive Time.
_Avoid_: Processing timestamp, processed raw time

**Exchange Time Observation**:
A venue-provided raw time value together with its documented semantic kind and unit, if known. It is preserved for analysis but never defines cross-venue replay order.
_Avoid_: Canonical timestamp, Capture Sequence

**Market Trade**:
A venue-reported public execution or explicitly identified execution aggregate, including aggressor side when it can be determined reliably. An aggregate is preserved as one observation and is never expanded into invented constituent trades.
_Avoid_: Fill, our trade

**Aggressor Side**:
The side of the Market Trade that removed resting liquidity. `Buy` means buyer-initiated, `Sell` means seller-initiated, and `Unknown` means the venue did not provide a reliable classification.
_Avoid_: Position side, our order side

**Execution Fill**:
An execution of an order owned by this trading system, including partial executions. It is private execution state, not a public Market Trade.
_Avoid_: Market trade, trade event

**Normalized Market Event**:
A venue-neutral market fact accepted after venue-specific decoding and validation. Live processing and deterministic replay consume the same event meaning.
_Avoid_: Raw frame, WebSocket message

**Capture Run**:
One bounded recording period with a unique identity and one shared monotonic clock origin. A process restart begins a new Capture Run rather than continuing the previous one.
_Avoid_: Recording file, recorder process

**Capture Sequence**:
A gap-free event ordinal assigned within one Capture Run at the common recorder handoff. The pair of Capture Run identity and Capture Sequence uniquely identifies a recorded event and defines replay order.
_Avoid_: Exchange sequence, global event ID, timestamp order

**Trade Stream**:
The ordered delivery of public Market Trade reports for one Market Coin at one venue. After an interruption it may resume, but it is not considered gap-free unless the venue provides a verified recovery contract.
_Avoid_: Order Book stream, execution fills
