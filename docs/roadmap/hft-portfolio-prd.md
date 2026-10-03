# PRD: HFT portfolio roadmap (Keyrock-first)

Status: ready-for-agent
Date: 2026-10-02
Source: `docs/research/kraken-keyrock-hft-requirements-gap.md` and the
prioritisation interview held on 2026-10-02.

## Problem Statement

The project owner wants this repository to serve as credible evidence for
HFT / low-latency Rust engineering roles, primarily at Keyrock (crypto market
maker) and secondarily at Kraken (exchange), within 1–3 months.

Today the repository demonstrates market-data correctness well: venue
adapters with recovery, exact decimal prices and quantities, separate
monotonic Local Receive Time and Processing Completion Time, a fail-closed
bounded recorder, an append-only checksummed capture, and deterministic
replay. The lead/lag research is honest about fees.

It does not demonstrate what the target postings ask for next:

- "low latency" is a claim, not a measured distribution — no
  receive→processing percentiles and no benchmark exist;
- there is no CI, so no reviewer can check that the code builds, lints and
  passes tests;
- the Rust `strategy`, `execution` and `risk` crates are empty stubs; the only
  execution simulation is a float-based Python research tool, which
  AGENTS.md §5.2 does not allow to be the shared core;
- there are no property or fuzz tests on the decoders and Order Book;
- there is no observability.

Without execution simulation and explicit risk controls the project cannot
show Keyrock's "execution, market data and operational risk" requirements,
and it cannot pass the AGENTS.md §12 gate (replay → execution simulation →
shadow → risk controls) that precedes any future live path.

## Solution

Close the gaps in a fixed order, each step leaving reviewable evidence:

1. **CI** on GitHub Actions: format, clippy with warnings denied, Rust tests,
   Python tests, and `cargo deny`.
2. **Latency evidence**: an offline report of receive→processing latency
   percentiles per venue from existing Capture Runs, and a microbenchmark of
   decode → Order Book update on fixed fixtures with tail percentiles and
   allocation counts.
3. **Rust strategy**: one lead/lag signal (Binance leads, Aster/Lighter lag)
   as a pure function of Normalized Market Events and a read-only market
   view, driving two swappable execution policies — taker first, maker
   second.
4. **Rust execution simulator**: a deterministic simulated venue producing
   Execution Fills from replayed Order Books and Market Trades, plus a ledger
   computing positions, fees and PnL in exact arithmetic, with a parity test
   against the Python simulator.
5. **Risk gate**: six pre-trade checks including a type-level ban on Binance
   orders and a latched kill switch.
6. **Property and fuzz tests** for `ExactDecimal`, Order Book invariants and
   adapter text decoding (may run in parallel with 3–5).
7. **Observability**: metrics collected off the hot path through a bounded
   handoff; exporter choice deferred.

Each architecture decision is recorded in an ADR before the code that depends
on it: ADR 0011 (latency measurement, benchmark and CI dependencies),
ADR 0012 (strategy event semantics), ADR 0013 (execution simulation),
ADR 0014 (risk policy and kill switch).

## User Stories

### CI

1. As the project owner, I want every push to run `cargo fmt --check`, so that formatting drift never reaches main.
2. As the project owner, I want every push to run clippy with warnings denied, so that lint regressions are caught automatically.
3. As the project owner, I want every push to run the full Rust test suite, so that a broken Order Book or replay is caught before merge.
4. As the project owner, I want every push to run the Python experiment tests, so that research tooling stays correct.
5. As the project owner, I want `cargo deny` to check licences and security advisories, so that dependency risk is visible.
6. As a hiring reviewer, I want a green CI badge and visible workflow, so that I can trust the repository's claims without cloning it.
7. As the project owner, I want benchmarks excluded from CI, so that noisy shared runners never produce misleading latency numbers.

### Latency evidence

8. As the project owner, I want receive→processing latency (Processing Completion Time minus Local Receive Time) computed per venue from a Capture Run, so that "low latency" is a measured distribution.
9. As the project owner, I want p50, p95, p99, p99.9 and maximum reported, so that tail behaviour is visible, not just the average.
10. As the project owner, I want outliers listed with their Capture Sequence, so that I can investigate individual spikes.
11. As the project owner, I want the report to state the ADR 0007 caveat that console/presentation work can delay receives, so that numbers are not over-claimed.
12. As the project owner, I want the report to record which Capture Run, host and build produced it, so that results are reproducible.
13. As the project owner, I want a microbenchmark of decode → Order Book update per venue on fixed fixtures, so that code cost is measured without network noise.
14. As the project owner, I want the benchmark to report tail percentiles and allocations per message, so that hot-path cost is visible beyond the mean.
15. As the project owner, I want the benchmark methodology documented, so that future optimisation claims are compared against a fixed baseline (AGENTS.md §14).
16. As a hiring reviewer, I want a committed latency report and benchmark baseline, so that I can see the candidate measures before optimising.

### Market view and strategy

17. As a strategy author, I want a read-only market view after each engine update, so that I can read every venue's best bid/ask, depth and Order Book availability without owning books.
18. As a strategy author, I want the market view to expose each Order Book's Local Receive Time, so that I can judge staleness.
19. As a strategy author, I want the triggering Normalized Market Event passed alongside the view, so that I know what changed.
20. As a strategy author, I want the strategy to be unaware of whether events come from live or replay, so that one implementation serves both (AGENTS.md §5.2).
21. As the project owner, I want the lead/lag signal to be a pure function of events and view, so that it is unit-testable and deterministic.
22. As the project owner, I want the signal to ignore unavailable Order Books, so that no decision is made on a book known to be invalid.
23. As the project owner, I want the signal separated from execution policy, so that the observed relationship, signal and execution assumption stay distinct (AGENTS.md §11).
24. As the project owner, I want a taker execution policy that crosses on the lag venue when the signal fires, so that the Rust stack reproduces the Python lead/lag result.
25. As the project owner, I want a maker execution policy that quotes on the lag venue and leans or cancels quotes when Binance moves, so that the project demonstrates market making with an external fair value.
26. As the project owner, I want execution policies to flatten a position after a configured maximum holding time or on signal reversal, so that directional exposure is bounded.
27. As the project owner, I want the strategy to emit intents (place, cancel, flatten) rather than talk to venues, so that strategy never owns clients, persistence or credentials.
28. As the project owner, I want strategy decisions to be identical across repeated replays of the same Capture Run and config, so that research results are reproducible.

### Execution simulation

29. As the project owner, I want simulated time to come from the replay's monotonic Local Receive Time, so that simulation shares the capture's single clock origin.
30. As the project owner, I want the order decision time to be the Processing Completion Time of the triggering event, so that decisions are not placed before the data that caused them was processed.
31. As the project owner, I want fixed per-venue send/ack/fill latencies in config, so that the first simulator is deterministic and easy to reason about.
32. As the project owner, I want later to sample latencies from the measured distribution with a fixed seed, so that realism improves while replay stays deterministic.
33. As the project owner, I want exchange timestamps never used to drive fills, so that venue clocks and the local clock are not mixed (AGENTS.md §8).
34. As the project owner, I want taker orders filled by walking the replayed Order Book at simulated arrival time, so that available depth limits fills honestly.
35. As the project owner, I want taker fills to have no lasting market impact, so that the replayed book stays authoritative — and I want this limitation documented.
36. As the project owner, I want maker quotes filled only when a Market Trade prints strictly through the quote price, so that the first maker model is conservative.
37. As the project owner, I want a second maker model that estimates queue position from displayed size and trades at the level, so that I can compare optimistic and conservative fill assumptions.
38. As the project owner, I want both maker PnLs reported side by side, so that fill-model risk is visible rather than hidden.
39. As the project owner, I want partial fills supported, so that Execution Fills reflect realistic order lifecycles.
40. As the project owner, I want cancel requests to take simulated latency, so that a quote can still fill while its cancel is in flight.
41. As the project owner, I want orders arriving when the lag venue's Order Book is unavailable to be rejected by the simulated venue, so that simulation never fills against an invalid book.
42. As the project owner, I want Execution Fills kept distinct from public Market Trades, so that private and public executions are never confused (CONTEXT.md).

### Ledger and PnL

43. As the project owner, I want positions, notional and PnL represented with `ExactDecimal`, so that no canonical monetary state uses floats (AGENTS.md §5.5).
44. As the project owner, I want fees configured as integer tenths of a basis point per venue and side, so that Aster maker 0 / taker 40 and Lighter 0 / 0 are exact.
45. As the project owner, I want realised and unrealised PnL reported separately, so that open exposure is not counted as profit.
46. As the project owner, I want PnL broken down into gross edge, fees and the effect of slippage/latency, so that theoretical spread is never reported as executable profit (AGENTS.md §11).
47. As the project owner, I want floats only at the report boundary, so that conversions are explicit.

### Risk gate

48. As the project owner, I want it impossible by construction to send an order to Binance, so that the signal-only rule of ADR 0010 cannot be violated.
49. As the project owner, I want a maximum position per venue, so that exposure on one lag venue is capped.
50. As the project owner, I want a maximum notional limit, so that total exposure is capped.
51. As the project owner, I want a maximum open orders limit, so that runaway quoting is impossible.
52. As the project owner, I want orders rejected when the target Order Book is unavailable or stale beyond a configured age, so that no order is based on invalid state.
53. As the project owner, I want a latched kill switch that trips on Order Book errors, a loss limit or a manual command, so that trading halts fail-closed.
54. As the project owner, I want the kill switch to stay tripped until an explicit reset event, so that it never re-arms on its own.
55. As the project owner, I want the reset event to be a recorded or configured event in replay, so that replay stays deterministic.
56. As the project owner, I want every rejection to carry a reason, so that the report shows why intents were blocked.
57. As the project owner, I want the risk gate between strategy and simulated venue, so that the same gate will guard a future live path.

### Simulation runner and parity

58. As the project owner, I want a replay mode that runs Capture Run → engine → strategy → risk gate → simulated venue → ledger, so that the whole decision path runs on recorded data.
59. As the project owner, I want the runner to output fills, rejections, PnL and a decision digest, so that runs can be compared exactly.
60. As the project owner, I want two runs on the same capture and config to produce the same digest, so that determinism is tested, not assumed.
61. As the project owner, I want a parity test showing the Rust simulator and Python simulator agree on fills and PnL within a stated tolerance on the same capture, so that the port is verified.
62. As the project owner, I want the Python simulator kept as a research tool, so that exploratory analysis stays fast while Rust is the source of truth.

### Property and fuzz tests

63. As the project owner, I want property tests for `ExactDecimal` parsing and arithmetic, so that exactness holds across generated inputs.
64. As the project owner, I want property tests for Order Book apply/delete invariants (sorted levels, no crossed book after valid updates, deletes remove levels), so that reconstruction is correct beyond hand-picked cases.
65. As the project owner, I want each adapter's text decoding entry point fuzzed, so that arbitrary input never panics and never corrupts an Order Book.
66. As the project owner, I want fuzzing in a separate crate outside the workspace build, so that the nightly toolchain is never needed for normal builds.

### Observability

67. As the project owner, I want counters and latency histograms collected through a bounded handoff off the hot path, so that measurement does not block decoding.
68. As the project owner, I want the backpressure behaviour of the metrics handoff explicit, so that metrics never silently drop market-data events.
69. As the project owner, I want the exporter choice deferred to its own decision, so that no dependency is added without justification.

### Documentation

70. As a hiring reviewer, I want ADRs 0011–0014 explaining each decision and its trade-offs, so that I can judge architectural reasoning.
71. As the project owner, I want ARCHITECTURE.md's open decisions updated as ADRs settle them, so that the documentation stays truthful.

## Implementation Decisions

### Order of work

CI → latency evidence → market view + strategy → execution simulator +
ledger → risk gate → observability. Property and fuzz tests may proceed in
parallel at any point because they touch only decoders, `ExactDecimal` and
Order Book code.

### ADRs (written before dependent code)

- **ADR 0011 — Latency measurement, benchmark and CI dependencies.** Offline
  receive→processing report from Capture Runs; microbenchmark of decode →
  Order Book update; dev-only `criterion` and `hdrhistogram`; GitHub Actions
  with fmt, clippy (`-D warnings`), cargo test, pytest and `cargo deny`; no
  benchmark job in CI. In-process latency histograms deferred to the
  observability step.
- **ADR 0012 — Strategy event semantics.** After each engine update the
  strategy receives the triggering Normalized Market Event and a read-only
  market view. No timer-sampled snapshots. Strategy emits intents only.
- **ADR 0013 — Execution simulation.** Simulated clock from Local Receive
  Time; decision time = Processing Completion Time of the triggering event;
  fixed per-venue latencies (seeded distributions later); taker book walk
  without lasting impact; maker trade-through (v1) then queue estimate;
  flatten on max holding time or signal reversal; no cross-venue hedge;
  `ExactDecimal` money and integer tenths-of-bps fees; Python parity
  tolerance.
- **ADR 0014 — Risk policy.** Six checks; type-level Binance ban; latched
  kill switch with explicit reset event.

### Modules

- **Market view (engine change).** The engine exposes a read-only view after
  each processed event: per venue and Market Coin — best bid/ask, depth
  access, Order Book availability, last Local Receive Time. It borrows engine
  state; it does not copy books.
- **Lead/lag signal (strategy crate).** Pure, deterministic, owns only its
  own small state (e.g. last Binance mid). Interface shape:
  `on_event(event, view) -> SignalState`. Ignores unavailable Order Books.
- **Execution policies (strategy crate).** `Taker` and `Maker` policies
  consume signal, view and current position and emit intents (place, cancel,
  flatten). They own max holding time and signal-reversal exit logic.
- **Risk gate (risk crate).** `check(intent, positions, view) -> Accept |
  Reject(reason)`, plus trip and reset events. The venue type used in orders
  cannot represent Binance, making the ban a compile-time property. Kill
  switch latched.
- **Simulated venue (execution crate).** Accepts orders with a decision time,
  advances on each replayed event, applies configured latency, maintains
  order state (pending, resting, partially filled, filled, cancelled,
  rejected) and emits Execution Fills using the configured fill model. The
  existing `order_state` stub is filled; `hedging` remains a stub.
- **Ledger (execution crate).** Applies Execution Fills, tracks positions per
  venue, fees, realised and unrealised PnL using `ExactDecimal`; marks to the
  market view.
- **Simulation runner (replay app mode).** Composes the modules over a
  Capture Run and config; emits a report (fills, rejections, PnL breakdown)
  and a decision digest. Same core path live trading would use.
- **Latency report (experiments analysis).** Reads Parquet export; computes
  per-venue percentiles of Processing Completion Time minus Local Receive
  Time; writes a dated report under experiments reports.
- **Benchmarks (backend benches).** Per-venue decode → Order Book update on
  fixed fixtures.
- **Fuzz crate and property tests.** Fuzz crate outside the workspace
  members; `proptest` as dev-dependency.
- **Metrics (metrics crate, last).** Counters and histograms over a bounded
  handoff with explicit backpressure; exporter decided separately.
- **CI workflow.** GitHub Actions on the existing GitHub remote.

### Dependencies (AGENTS.md §15)

All new dependencies are dev-only or tooling and never touch the hot path:
`criterion`, `hdrhistogram`, `proptest`, `cargo-fuzz` (separate crate),
`cargo-deny` (CI tool). Each ADR states requirement, why std/current
dependencies are insufficient, and hot-path impact.

## Testing Decisions

A good test exercises a module only through its public interface and
asserts observable behaviour (intents emitted, fills produced, rejections
with reasons, PnL values, digests), never internal fields. Fixtures are
small and deterministic (AGENTS.md §13).

Tested modules:

- **Market view** — engine test that the view reflects the latest accepted
  Order Book and availability after each event.
- **Lead/lag signal** — fixtures of Binance/lag events with expected signal
  states, including unavailable-book cases.
- **Execution policies** — given signal sequences and positions, expected
  intents; holding-time and reversal exits.
- **Risk gate** — each of the six checks accepts and rejects at the boundary;
  kill switch latches and only resets on the reset event.
- **Simulated venue** — taker book walk with partial depth; maker
  trade-through vs no fill on touch; cancel-in-flight fill; rejection on
  unavailable Order Book; latency ordering.
- **Ledger** — exact fee and PnL arithmetic for known fill sequences;
  realised vs unrealised split.
- **Simulation runner** — determinism digest across two runs (prior art: the
  replay app's `replay_is_deterministic_and_ends_on_the_last_recorded_books`
  test) and Rust↔Python parity within tolerance.
- **Latency report** — pytest on a small Parquet fixture with known
  percentiles (prior art: existing `experiments/tests`).
- **ExactDecimal and Order Book** — property tests; adapters — fuzz targets.

Prior art: recorder module tests, venue adapter scripted-transport tests,
the replay determinism test, and Python simulator tests.

Benchmarks, metrics and CI are not tested themselves; CI runs the tests
above.

## Out of Scope

- Live trading, shadow mode, credentials and any real order submission.
- Cross-venue hedging (Aster ↔ Lighter).
- Kafka, gRPC, PostgreSQL (conflict with AGENTS.md §9).
- Aeron, multicast, FIX, DPDK, FPGA (infrastructure unavailable on a VPS or
  no active venue offers it).
- A standalone matching engine (outside the mission, AGENTS.md §1).
- Fixed-scale `i64` price/quantity migration (needs benchmark evidence
  first, AGENTS.md §5.6).
- In-process latency histograms on the hot path before the observability
  step; the metrics exporter choice.
- Benchmark regression tracking in CI.
- Reconciliation against real venues.
- Person-level requirements (years of experience, production trading, C++,
  leadership, AWS) — see the research file §5.

## Further Notes

- Keyrock is the primary target; R1 (Senior Rust Engineer HFT) is the
  closest posting. Kraken had no dedicated HFT IC role on 2026-10-02.
- The research file's requirement IDs (K1–K5, R1–R2, KB1–KB3) trace each
  item back to a posting or engineering blog.
- Postings change; re-check before applying.
- Lighter Standard has a 300 ms taker delay, which likely erases the lead
  window for taker execution there; the maker policy is the expected source
  of positive results. The taker policy is kept as a reproduction of the
  Python finding, not as a profit claim.
