# Reproducible Market-Data Pipeline

## Purpose

This document specifies the first reproducible data pipeline for quantitative
strategy research. It turns live BTC perpetual observations from Aster,
Hyperliquid, and Lighter into a validated canonical log, deterministic replay,
and an offline Parquet dataset.

The MVP is a thirty-minute measurement run. It establishes correctness and
real data-volume evidence before compression, delta encoding, capacity tuning,
or retention policy are considered.

## Scope

The MVP records:

- every accepted full normalized L2 Order Book state;
- public venue-reported trades and execution aggregates;
- Order Book unavailability;
- Trade Stream unavailability and resumption;
- all venue-provided exchange time observations and source identities;
- Local Receive Time and Processing Completion Time;
- data-quality and recorder metrics.

It does not record raw WebSocket frames, BBO, derived features, funding,
private orders or fills, account state, or risk state. BBO, spread, imbalance,
and microprice are derived offline from the recorded L2 state.

## Data flow

```text
Aster / Hyperliquid / Lighter
        │ one WebSocket per venue
        ▼
venue-specific whole-market adapter
        │ owned NormalizedMarketEvent
        ▼
Capture Sequence assignment
        │
        ├── try_send clone ──> bounded recorder queue ──> recorder thread
        │                                                    │
        │                                                    ▼
        │                                           canonical segments
        ▼
synchronous engine processing

canonical segments ──> validated replay ──> same engine entry point
                  └──> offline exporter ──> Parquet ──> DuckDB / Polars
```

An event is processed by the live engine only after the recorder queue accepts
its clone. If enqueue fails, that event does not enter the engine and the
Capture Run stops as incomplete.

## Ownership

- `market-data` owns the owned `NormalizedMarketEvent`, `MarketTrade`, Order
  Book payload, availability events, and shared time vocabulary.
- `venue-hyperliquid`, `venue-lighter`, and `venue-aster` own wire decoding,
  normalization, trade identity interpretation, deduplication, and recovery
  decisions.
- `venue` owns connection lifecycle and synchronous delivery of normalized
  events. Each venue runtime uses one WebSocket for both L2 and trades.
- `engine` owns the common live/replay processing entry point and Capture
  Sequence semantics.
- `recorder` owns `StoredEventV1`, domain-to-storage conversion, the bounded
  handoff, segment writer and reader, recovery, checksums, hashes, and manifest.
- `trader` composes live sessions, engine, Capture Run, recorder, shutdown, and
  presentation.
- `replay` validates a Capture Run and sends its stored events to the same
  engine entry point.
- `export-parquet` performs offline conversion into an immutable analytical
  dataset.

No venue-specific wire representation enters the strategy interface.

## Normalized events

The normalized event variants are:

```text
OrderBookSnapshot
BestBidOfferUpdated
OrderBookUnavailable
MarketTrade
TradeStreamUnavailable
TradeStreamResumed
```

Every accepted L2 update produces a complete `OrderBookSnapshot`. Hyperliquid
and Aster inputs are naturally complete snapshots. Lighter remains
venue-incremental internally but publishes the complete reconstructed state
after every accepted update.

Hyperliquid also publishes `BestBidOfferUpdated` from its independent `bbo`
subscription. BBO is a separate state stream: it does not replace or mutate
`OrderBookSnapshot`, and its bid/ask sides may be null. The engine marks BBO
`Available` only when both sides exactly match the most recent L2 top level and
their Local Receive Times are within 500 ms. Otherwise it preserves the event
but reports `Unknown`; the full L2 book is not invalidated.

`OrderBookUnavailable` has a stable category and diagnostic detail:

```text
Disconnected
InvalidMarketData
SequenceGap
RecorderShutdown
Other
```

Strategies consume only the availability transition. The category is for data
quality; diagnostic text never affects deterministic strategy decisions.
Normal Capture Run completion emits no synthetic unavailability. The
`RecorderShutdown` category is reserved for abnormal recorder-initiated stops.

Trade feeds have independent availability. `TradeStreamResumed` means delivery
resumed; it does not claim that missed trades were recovered. Strategies must
reset rolling trade-flow state across a trade-stream interruption.

## Market Trade semantics

Each venue-reported array element becomes one `MarketTrade`, preserving array
order. All elements decoded from the same frame share Local Receive Time and
receive consecutive Capture Sequence values; Processing Completion Time is
sampled for each completed normalized event.

```text
reporting_kind:
  Individual
  TakerOrderAggregate

trade_kind:
  Regular
  Liquidation
  Deleverage
  MarketSettlement
  Other

aggressor_side:
  Buy
  Sell
  Unknown

classification:
  VenueProvided
  DerivedFromMakerSide
  Unknown
```

`Buy` means buyer-initiated and `Sell` means seller-initiated. A mapping not
confirmed by fixture tests produces `Unknown` rather than an inferred value.

Venue rules:

| Venue | Reporting | Identity and deduplication | Additional rules |
|---|---|---|---|
| Aster | `TakerOrderAggregate` | `(market, aggregate_trade_id)`; retain first and last underlying trade IDs | Never invent constituent trades |
| Hyperliquid | `Individual` elements from a batch | `(market, block_time, trade_id)`; retain transaction hash | No documented gap-free resume |
| Lighter | `Individual` elements from ordinary and liquidation arrays | `(market_id, trade_id_string)`; retain optional message nonce | Preserve regular, liquidation, deleverage, and market-settlement kinds |

When a reliable identity exists, the adapter removes duplicates before they
become Normalized Market Events and increments a quality counter. Without such
an identity, price/time/quantity similarity is never treated as proof of a
duplicate.

Each `(venue, market)` owns an independent bounded identity set. Its configured
capacity is `TRADE_DEDUP_CAPACITY`, defaulting to 1,000,000 identities. The set
survives socket reconnects and is reset only when the process starts a new
Capture Run. It never evicts an identity: saturation stops the whole Capture
Run fail-closed. This preserves the guarantee that a known duplicate never
becomes a Normalized Market Event.

## Ordering and time

`Capture Sequence` is a gap-free `u64` ordinal scoped to one Capture Run. It is
assigned at the common handoff, begins at one, and is the only replay order. A
Capture Run restart creates a new UUID and resets the sequence; there is no
global mutable counter to recover.

`StoredEventV1` contains:

```yaml
capture_sequence: u64
venue: Venue
market_coin: MarketCoin
local_receive_time: monotonic nanoseconds since Capture Run origin
processing_completion_time: monotonic nanoseconds since the same origin
exchange_times: [ExchangeTimeObservation]
source_identity: optional typed venue identity
payload:
  OrderBookSnapshot | OrderBookUnavailable | MarketTrade |
  TradeStreamUnavailable | TradeStreamResumed
```

Each `ExchangeTimeObservation` contains:

```yaml
kind: EventTime | TradeTime | BlockTime | TransactionTime | Other
raw_value: u64
declared_unit: Milliseconds | Microseconds | Nanoseconds | Unknown
```

Unknown units remain unknown. Exchange time never orders events across venues.
Canonical Price and Quantity values retain `(coefficient: i128, scale: u8)`;
they are never serialized as `f64`.

## Recorder handoff

The recorder queue initially holds 4,096 owned events and is configurable. The
producer uses `try_send`; it never waits for capacity and never drops an event.
Queue full stops every venue session, drains events already accepted, and
finalizes the Capture Run as `incomplete_queue_full`.

The recorder thread owns Postcard serialization, record framing, CRC32C,
batching, file writes, `fdatasync`, SHA-256, rotation, and manifest updates. A
writer failure is reported through a status channel checked before accepting
the next live event. A filesystem error stops the entire Capture Run rather
than continuing in a different file.

The MVP records queue occupancy high-water mark and approximate owned-event
bytes. Queue capacity changes only after measurement.

## Filesystem layout

`DATA_DIR` is an absolute configurable path outside the Git repository. Docker
uses an explicit bind mount to this host path.

```text
DATA_DIR/
  captures/
    <capture_id>/
      manifest.json
      segment-000000.log
      segment-000001.log
  parquet/
    version=1/
      ...
```

The MVP does not delete or overwrite captures automatically.

## Segment format V1

Domain structs are not serialized directly. Explicit immutable `StoredEventV1`
DTOs convert to Postcard bytes. A later incompatible schema introduces a new
DTO and format version; it does not mutate V1.

Each segment has a fixed header containing:

- magic bytes;
- format version;
- fixed header length;
- Capture Run UUID;
- zero-based segment index;
- segment creation time as UTC Unix milliseconds.

Each record is independently framed:

```text
payload_length: u32 little-endian
postcard_payload: payload_length bytes
crc32c: u32 little-endian
```

The MVP canonical log is uncompressed. A reader rejects malformed lengths,
truncated payloads, invalid CRC32C, mismatched Capture Run identity, and unknown
format versions. Format changes require golden-byte and round-trip tests.

## Rotation, durability, and atomic publication

The active segment uses `.open` and rotates after five minutes or one GiB,
whichever occurs first. Every second, and also at rotation and clean shutdown,
the writer flushes and calls `fdatasync`.

Finalization is:

1. flush and `fdatasync` the `.open` file;
2. calculate its SHA-256;
3. atomically rename it to `.log` on the same filesystem;
4. write the updated manifest to a temporary file;
5. atomically replace `manifest.json`.

The implementation synchronizes the containing directory where required so a
successful rename is durable across a host crash.

Ordinary replay reads only `.log` segments listed in the manifest. It never
reads `.open` files or unlisted `.log` files.

On startup, recovery scans a previous `.open` segment to the last complete,
CRC-valid record, truncates the invalid tail, synchronizes and hashes it,
publishes the repaired `.log`, and marks that Capture Run
`incomplete_process_crash`. The new process then creates a new Capture Run with
sequence one. Approximately one second of acknowledged-but-not-durable tail
loss is accepted for this research MVP.

An `.open` segment with no valid records is synchronized and removed rather
than published as an empty `.log`; it contributes no segment entry. A CRC,
Postcard, framing, or Capture Sequence failure invalidates that record and the
entire tail after it.

Recovery also reconciles a valid next-index `.log` that was renamed before a
crash but not yet added to the manifest. Any other unlisted `.log` remains an
integrity error rather than being adopted implicitly.

## Shutdown

Normal timeout or user shutdown:

1. stop all venue sessions;
2. prohibit new events;
3. drain accepted queue events;
4. synchronize and finalize the active segment;
5. publish `capture_status=complete`.

Queue saturation follows the same drain path but publishes
`incomplete_queue_full`. On filesystem failure, capture stops immediately, the
active file remains recoverable, and an `incomplete_io_error` manifest is
written only if it can be written safely.

Capture statuses are:

```text
complete
incomplete_queue_full
incomplete_process_crash
incomplete_io_error
```

## Manifest

The capture manifest contains:

- Capture Run UUID and status;
- sanitized resolved configuration and its SHA-256;
- Git commit and dirty-build flag;
- package version and Rust target;
- hostname or collector label;
- UTC start and end;
- Configured Market Set;
- resolved venue symbols and Venue Market IDs;
- queue capacity and observed high-water mark;
- rotation limits;
- data-quality counters;
- an ordered segment list.

Each segment entry contains index, filename, first and last Capture Sequence,
record count, byte size, SHA-256, UTC start/end, and completion status.

`capture_status=complete` means the recorder retained every event it accepted.
It does not claim uninterrupted venue delivery. Data-quality counters include
disconnects, Order Book unavailable intervals, trade-stream gaps, invalid
messages, and deduplicated trades.

## Validated replay

Replay defaults to complete Capture Runs. Reading an incomplete run requires
an explicit `--allow-incomplete` option.

Before emitting any event, MVP replay validates:

- manifest status;
- contiguous ordered segment indices;
- SHA-256 of every segment;
- header identity and format version;
- CRC32C of every record;
- contiguous Capture Sequence;
- manifest first/last sequence and record counts;
- absence of unlisted `.log` files in the capture directory.

Only after full validation does replay send events to the shared engine. A
configuration override is explicit and recorded in replay output. Two runs of
the same capture and configuration must produce the same event/output digest
and final Order Book state.

## Parquet derivative

`export-parquet` reads only complete captures by default. Incomplete input
requires `--allow-incomplete`, and its status and quality flags propagate into
the derivative.

The immutable tables are:

- `captures` for metadata and quality counters;
- `order_book_events` with one row per snapshot;
- `best_bid_offers` with one row per BBO event and nullable exact bid/ask
  columns;
- `order_book_levels` with one row per bid or ask level;
- `market_trades` with one row per Market Trade;
- `availability_events` for Order Book and Trade Stream transitions;
- `exchange_times` for every Exchange Time Observation.

Rows relate through `(capture_id, capture_sequence)`. Analytical Price and
Quantity columns use exact `DECIMAL(38,18)`. Conversion pads scale with zeros
without rounding; overflow or any inexact value fails the export. No canonical
or analytical monetary column uses `DOUBLE`.

New datasets use schema version 2 and the following partition layout:

```text
parquet/
  version=2/
    date=YYYY-MM-DD/
      event_type=<type>/
        venue=<venue>/
          market=BTC/
            part-....parquet
```

Every row retains `capture_id`; Capture Run is not a partition key. Dataset
metadata records source Capture IDs, source manifest SHA-256 values, converter
schema version, converter Git commit, and UTC conversion time. Conversion
always creates a new dataset and never modifies canonical captures. Existing
version 1 datasets remain immutable and readable; only newly exported datasets
use version 2 and include `best_bid_offers`.

## MVP limits and acceptance

The first run captures BTC perpetual from all three venues for thirty minutes.
It succeeds only when:

- L2, Market Trades, and availability events are present for all venues;
- the Capture Run is `complete` and the queue never filled;
- all segment, checksum, sequence, and manifest validations pass;
- two replays produce the same digest and final Order Books;
- final replay Order Books match the last recorded snapshots;
- Parquet row counts reconcile with canonical event counts;
- every decimal conversion is exact;
- total captured bytes remain below forty GiB;
- a report includes events/second, bytes/event, bytes by venue/channel, queue
  high-water, serialization/write/`fdatasync` latency, deduplicated trades,
  gaps, and unavailable durations.

Only after this report may compression, delta encoding, queue capacity, or
retention be changed.

## Implementation stages

1. **Implemented.** Add the owned normalized event model, Market Trade types, and fixture-based
   trade decoders for all three venues.
2. **Implemented.** Implement `StoredEventV1`, segment writer/reader, checksums, rotation,
   atomic publication, and crash recovery.
3. **Implemented.** Add the Capture coordinator, bounded recorder handoff, and `trader`
   composition.
4. **Implemented.** Implement validated deterministic replay through `engine`.
5. **Exporter implemented; VPS measurement pending.** The offline Parquet
   exporter is implemented and verified with a live end-to-end smoke capture.
   The thirty-minute BTC measurement remains an operational run on the target
   VPS.

Run replay against a capture directory only after the Capture Run has closed:

```text
cargo run -p replay -- /absolute/DATA_DIR/captures/<capture_id>
```

Incomplete input and configuration changes are rejected unless they are
explicit:

```text
cargo run -p replay -- /absolute/DATA_DIR/captures/<capture_id> \
  --allow-incomplete \
  --config-override KEY=VALUE
```

Create a new immutable analytical dataset:

```text
cargo run -p export-parquet -- \
  /absolute/DATA_DIR/captures/<capture_id> \
  /absolute/DATA_DIR/parquet/<new-dataset-id>
```

The output directory must not already exist. Incomplete captures additionally
require `--allow-incomplete`. Order Book level rows are written in bounded
4,096-row batches so Lighter's full snapshots do not create a second
unbounded in-memory representation during conversion.

Each stage is independently reviewed and tested. The live measurement does not
start before recorder round-trip, corruption, rotation, and recovery tests pass.
