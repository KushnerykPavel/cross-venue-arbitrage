# Binance book quality: `depth10@100ms` is coarse in time, not wrong

## Question

The lead/lag report (`2026-10-01-lead-lag.md`) found that the recorded Binance
best quote changes 0.2–0.7 times per second while trade prices change 7–19
times per second, and 67–75% of trades print outside the recorded book. Every
lead/lag number depends on when a Binance move is detected. Is the recorded
book wrong, or coarse, and does a faster signal change the result?

No Rust code or ADR was changed. Scripts and raw output are in
`2026-10-01-binance-book-quality/`.

## Data

1. **Live raw capture, 10 minutes, 2026-10-01 ~09:18 CEST, from a Mac (not the
   VPS).** Five sources on BTCUSDT side by side, each frame stamped with local
   wall time:
   - `depth10@100ms` on `/public` (what ADR 0009 uses);
   - `depth10@100ms` on the legacy `/stream` endpoint;
   - `bookTicker` on `/public` (real-time best bid/ask, ~1,100 msg/s);
   - `aggTrade` on `/market` (what ADR 0009 uses);
   - REST `/fapi/v1/depth?limit=10` once per second.
   Script `binance_capture.py`, analysis `analyze_capture.py` and
   `extra_checks.py`, output `cap10m_results.txt`. The raw JSONL (143 MB) is not committed.
2. **Recorded Sept 27 capture**, dataset `6b556515-…`, train and validation
   splits only (test not loaded). Binance trade time `T`, trade event time `E`
   and book event time `E` are in the `exchange_times` table. Scripts
   `recorded_check.py` and `matched_edge.py`.

Cross-stream comparisons in the live capture are valid. Absolute
`receive − exchange time` values include the Mac's network path and clock
offset and must not be compared with collector numbers.

## Findings

### 1. The recorded book is correct (top of book and full top 10)

| Check (live capture) | Result |
| --- | --- |
| depth10 best bid/ask ≠ bookTicker at the same update id `u` | 0 of 5,851 |
| `/public` depth10 ≠ legacy `/stream` depth10 at the same `u`, all 10 levels | 0 of 5,851 |
| REST best bid/ask ≠ bookTicker as of the same `T` | 7 of 599 |

The top of each depth10 frame equals the real-time best bid/ask at that update
id. Levels 2–10 were compared only against the legacy endpoint (identical),
not against an independent full-depth source. The `/public` endpoint is not
degraded.

### 2. "Trades outside the book" is a join artifact

The earlier figure compared each trade with the last book received at or
before it. That book can already show the result of the same match. Using
Binance exchange time and the aggressor side, against the book strictly
*before* the trade (recorded data):

| Split | Inside previous book | Aggressor-side sweep | Wrong direction |
| --- | ---: | ---: | ---: |
| Train | 31.3% | 66.4% | 2.3% |
| Validation | 22.7% | 74.8% | 2.5% |

A sweep is a buy above the previous ask or a sell below the previous bid: the
order walked into deeper levels. That is normal matching. The 2.3–2.5%
wrong-direction trades are consistent with the previous book being up to
100 ms old. The live capture against `bookTicker` gives 0.4% outside both
classes.

### 3. Trade-price changes and mid changes measure different things

- 70–73% of trade-price changes happen *inside one match*: one taker order
  fills several levels at the same `T`, and each level is its own aggregate
  trade. Another ~20% is bid/ask bounce (live capture).
- Distinct match times are 3.4/s (train) and 7.1/s (validation), not 7–19/s.
- The top-10 book behind the touch is thin (often 0.001–0.004 BTC per level),
  so when the touch is consumed the mid jumps many ticks. That explains the
  35-tick median mid jump.

So 0.2–0.7 mid changes per second vs 7–19 trade-price changes per second is
not evidence of a coarse book.

### 4. The real cost of depth10 is sampling delay, not wrong data

From the live capture:

| | Per second |
| --- | ---: |
| bookTicker mid changes | 14.2 |
| depth10 mid changes | 1.6 |
| trade price changes | 46.9 |

- depth10 does not miss moves; it collapses bursts. Of the 982 depth10
  intervals in which the bookTicker mid changed at all, the next depth10 frame
  showed a different mid in 977. Inside those intervals the bookTicker mid
  changed a median of 6 times (p90 18): a sweep or requote walks the touch in
  steps, and depth10 sees only the end state. Only 3.0% of bookTicker mid
  changes revert to the previous mid within 100 ms, so this is not flicker.
- After a bookTicker mid change, the next depth10 frame arrives a median of
  37 ms later on local receive (p90 81 ms). The figure is the same for 1-tick,
  2–4-tick and ≥ 5-tick jumps, so large moves are not delayed more. The
  maximum is 362 ms (a late depth10 frame).
- For move events (≥ X bps over 200 ms), depth10 detects later than
  bookTicker. The median is about 40 ms at X = 1–3 bps (n = 9–106), and the
  p90 is about 80–87 ms. At X = 5 there was one event, so no estimate.

**This bounds the understatement in the lead/lag report.** Book-based event
times are late by about 40 ms on average and at most about 100 ms. The lead
is not hidden by a broken feed.

### 5. Trades are not a faster path than bookTicker

The aggTrade stream on `/market` holds some frames back on Binance's side.
`E − T` uses only the exchange clock:

| Stream | E − T p50 | p90 | p99 | max |
| --- | ---: | ---: | ---: | ---: |
| bookTicker | 0 ms | 1 | 1 | 3 |
| depth10 | 1 ms | 2 | 3 | 5 |
| aggTrade (live) | 1 ms | 83 | 150 | 158 |
| aggTrade (recorded train) | 1 ms | 150 | 151 | 167 |

- Share of aggregate trades with `E − T` ≥ 100 ms: 8.6% live, 25.5% on
  recorded train and 15.8% on validation. In the live capture it is 33.3% of
  trades that are the only aggregate in their match, and 0.6% of trades that
  are part of a multi-level sweep. So large sweeps, the ones that trigger
  events, are rarely held back.
- For the same match, aggTrade arrives a median of 4.5 ms *before*
  bookTicker. p10 is −42 ms, but p90 is +91 ms.
- Move detection from trade prices vs from bookTicker: about 5–30 ms earlier
  at the median, with a tail of up to +100–300 ms late.

The answer is mixed. Trades often arrive a few ms before bookTicker, but
they are not reliable. bookTicker is the clean fast path for the touch.

### 6. Trade-triggered events on the recorded data (check two)

Events from a per-match VWAP of Binance trades, keyed on the first local
receive of the match, at the same X, 200 ms window and 2 s cooldown as the
book events.

- **Timing.** At X = 2 bps, 188 of 240 book events (train) and 147 of 188
  (validation) have a same-direction trade event within 500 ms. The book
  event comes a median of 50 ms (train) and 44 ms (validation) later, with
  p10 −9 to −24 ms and p90 +106 to +110 ms.
- **Edge on the same events.** On matched X = 2 events, t0 is moved to
  whichever detection came first. Gross edge, mean, in bps (h = 1 s / 2 s):

| Split | Venue, L | Book t0 | Earliest t0 | Gain |
| --- | --- | ---: | ---: | ---: |
| Train | Aster, 50 ms | 2.24 / 2.29 | 2.66 / 2.72 | +0.42 |
| Train | Lighter, 350 ms | 0.38 / 0.40 | 0.57 / 0.60 | +0.19 |
| Validation | Aster, 50 ms | 2.10 / 2.19 | 2.49 / 2.58 | +0.39 |
| Validation | Lighter, 350 ms | 0.18 / 0.25 | 0.44 / 0.49 | +0.25 |

- At X = 5 there are only 5 matched events per split, so no estimate.
- **Trade trigger alone.** Used as its own event set, the trade trigger
  barely changes mean net edge at X = 2. For Lighter at fee 0 and L = 350 ms
  it goes from +0.36 to +0.49 (train) and from +0.15 to +0.28 (validation).
  At X = 5 it finds fewer events (8 and 5 vs 11 and 16), and the edge is
  noisy.

The matched-event gain is optimistic:
- It keeps only trade events that a book event confirmed within ±500 ms,
  which uses information up to half a second ahead and drops trade-only
  false alarms.
- Taking the earlier of two detectors adds its own early bias.
- It uses trade timing as a stand-in for bookTicker timing, which the
  recorded data does not have.

The unconditional figure is the trade-trigger-alone result: about +0.13 bps
for Lighter at L = 350 ms. So earlier detection is worth about +0.1
(unconditional) to +0.4 (matched, optimistic) bps gross at X = 2. That does
not change any conclusion of the lead/lag report:
- Aster taker/taker still loses about 5.5 bps at 4 bps per leg.
- Lighter taker/taker stays thin at about +0.5 bps.

## Conclusion

1. **The Binance book is coarse in time, not wrong, and the `/public`
   stream is not degraded.** depth10 is an exact 10 Hz sample of the
   real-time best bid/ask. The "trades outside the book" and "mid changes vs
   trade changes" figures were measurement artifacts: a same-match join, and
   multi-level sweeps.
2. **The coarseness costs time, not hidden moves.** Book-based event times
   are late by about 40 ms on average and at most about 100 ms. The lead/lag
   report's edge is slightly understated, by about +0.1 to +0.4 bps gross at
   X = 2 (trade timing as a proxy), not by enough to change its conclusions.
3. **A trade-based trigger is not a clean fix.** `/market` aggTrade frames
   are sometimes held back by about 150 ms on Binance's side. bookTicker
   carries the touch in real time with `E − T` ≤ 3 ms.
4. **The book is coarse, but only by its 10 Hz sampling.** There is no
   large hidden lead. Lighter's 300 ms taker delay still dominates its edge.

## Decision needed (human-owned: ADR 0009 / market-data semantics)

Pick one; I have not changed code or the ADR.

- **A. Keep ADR 0009 as is.** Accept about 40 ms (≤ 100 ms) of sampling
  delay on Binance event detection. Document the bound in the lead/lag
  report. No engineering cost.
- **B. Amend ADR 0009 to add `<symbol>@bookTicker` on `/public`** as a
  separate normalized top-of-book event, keeping depth10 for L2.
  - Gives the real-time touch with no bootstrap or gap-recovery problem.
    Each frame is a complete best bid/ask, and `u` gives ordering against
    depth10.
  - Needs decisions on: a new event type, or a top-of-book snapshot in the
    existing model; recorder volume (~1,100 msg/s in this window, about
    100× depth10); and how the engine picks between two books for one venue.
  - Expected gain from this evidence: about 40 ms earlier detection, worth
    about +0.1 to +0.4 bps gross at X = 2.
- **C. Full diff-depth (`@depth@0ms`) reconstruction.** Already flagged in
  ADR 0009 as needing REST bootstrap, buffering and gap recovery. Not
  justified by this evidence alone.

Using trades as the primary trigger is not recommended because of the
`/market` hold-back. Trade direction could still be a secondary feature
later.

## Caveats

- The live capture is one 10-minute window on a Mac, in a more active regime
  than Sept 27: the depth10 mid changed 1.6/s live vs 0.2–0.7/s recorded.
  Stream-to-stream timing differences should carry over. Absolute rates will
  not.
- The aggTrade hold-back appears both live (p90 83 ms) and on the VPS
  recording (p90 150 ms), so it is not a Mac artifact.
- Matched-event edge uses the earlier of two detections. It measures what
  earlier detection is worth on the same events. It is not a strategy
  result.
- RPI liquidity (Binance's retail price improvement orders) is excluded from
  depth10 and bookTicker. It was not analysed: the `@rpiDepth@500ms` stream
  is a diff format, and the 2.3–2.5% wrong-direction trades leave little
  room for it here.
