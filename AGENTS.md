# AGENTS.md

## 1. Project Mission

This repository implements a low-latency, event-driven market-data,
research, replay and eventually execution engine in Rust.

Initial scope:

-   instrument: BTC perpetual;
-   venues: Binance, Hyperliquid, Lighter;
-   first research strategy: cross-venue lead/lag;
-   later strategy: market making using external fair value.

This is also an HFT engineering learning project. Code quality, explicit
reasoning, observability, determinism and understanding are more
important than producing a large amount of code quickly.

## 2. Authority

The human is the architecture owner.

Agents implement explicitly scoped tasks. Agents MUST NOT independently
redefine:

-   system architecture;
-   event semantics;
-   order-book semantics;
-   timestamp semantics;
-   concurrency model;
-   risk policy;
-   execution assumptions;
-   storage format;
-   hot-path boundaries.

If implementation requires one of these decisions and it is not already
documented, STOP and describe the decision that is required.

Do not silently choose an architecture.

## 3. Read Before Editing

Before making changes:

1.  Read this file.
2.  Read `ARCHITECTURE.md`.
3.  Read any ADR or design document relevant to the affected component.
4.  Inspect the existing implementation and tests.

Do not assume a generic HFT architecture overrides repository
documentation.

## 4. Task Discipline

Work only on the requested task.

Prefer changes that are:

-   small;
-   local;
-   reviewable;
-   testable;
-   reversible.

Do NOT:

-   implement the next milestone automatically;
-   perform unrelated refactors;
-   rename unrelated APIs;
-   introduce frameworks "for future use";
-   add abstractions without a current requirement;
-   add dependencies without explaining why;
-   replace explicit code with clever generic machinery merely to reduce
    line count.

If the requested change would require a large refactor, report that
before performing it.

## 5. Architecture Invariants

The following are repository-level invariants unless explicitly changed
by an ADR.

### 5.1 One normalized event model

Venue-specific messages are decoded at the boundary and converted into
explicit internal event types.

Venue-specific wire representations must not leak into strategy logic.

Preserve information required for correctness, reconstruction, replay
and latency analysis.

### 5.2 Live and replay share the same core

Live market data and recorded market data must enter the same normalized
processing path.

Do not create a separate "backtest strategy implementation".

A strategy should not need to know whether an event came from a live
adapter or replay source.

### 5.3 Deterministic replay

Given the same event log and configuration, replay should produce the
same ordered state transitions and strategy decisions, except for
explicitly documented nondeterministic diagnostics.

Avoid wall-clock reads, random state, hidden background mutation or
uncontrolled concurrency in deterministic logic.

### 5.4 Explicit ownership

Prefer clear ownership and message passing over broadly shared mutable
state.

Do not introduce `Arc<Mutex<_>>` or `Arc<RwLock<_>>` into
latency-sensitive components merely because it is convenient.

Shared synchronization requires justification.

### 5.5 Prices and quantities are not `f64`

Do not use floating-point values for canonical price, quantity, fee or
monetary state.

Use integer/fixed-point representations with explicit scales or another
documented exact representation.

Conversions at external/reporting boundaries must be explicit.

### 5.6 Correctness before optimization

Do not perform speculative micro-optimization.

First establish:

1.  correct semantics;
2.  tests;
3.  measurement;
4.  benchmark/profile evidence;
5.  optimization.

Performance-sensitive changes must preserve observable behaviour unless
the task explicitly changes semantics.

## 6. Hot Path

The hot path broadly covers:

`network receive -> decode -> normalize -> book/state update -> signal -> risk decision -> execution intent`

The exact boundary may be refined in `ARCHITECTURE.md` or ADRs.

Inside the hot path, avoid unless measured and justified:

-   blocking I/O;
-   filesystem access;
-   database calls;
-   logging that can block;
-   unnecessary heap allocation;
-   unbounded queues;
-   unnecessary cloning;
-   lock contention;
-   sleep/timers used as synchronization;
-   serialization intended only for analytics.

Async networking may exist at system boundaries. Do not make the entire
core async by default.

## 7. Market Data Correctness

Order-book correctness has priority over strategy output.

Adapters must explicitly handle venue-specific rules such as:

-   snapshots;
-   incremental updates;
-   sequence numbers;
-   duplicate messages;
-   gaps;
-   reconnects;
-   stale state;
-   out-of-order behaviour where applicable.

Never silently continue using a book known to be invalid.

When correctness cannot be established, transition the venue/book to an
explicit unhealthy state and require recovery according to the adapter
contract.

## 8. Time Model

Do not collapse different notions of time into one timestamp.

Where available, preserve at least:

-   exchange/event timestamp;
-   local receive timestamp;
-   internal processing timestamp.

Execution components may later add:

-   order decision timestamp;
-   send timestamp;
-   acknowledgement timestamp;
-   fill timestamp.

Use a monotonic clock for local latency measurement where appropriate.

Wall-clock time and monotonic elapsed time are different concepts and
must not be silently substituted for one another.

## 9. Recorder and Storage

The recorder must not perform synchronous filesystem/database work on
the trading hot path.

Canonical raw/replay storage is an append-only event log.

Initial intended flow:

`normalized events -> bounded handoff -> dedicated recorder -> batched append-only writes`

Parquet is an analytical derivative, not the primary replay truth.

DuckDB/Polars/Python may consume analytical data offline.

Do not add Kafka, Redis, ClickHouse or another infrastructure dependency
without a demonstrated requirement and explicit approval.

Backpressure behaviour must be explicit. Never silently drop market-data
events.

## 10. Concurrency

Concurrency must have a reason.

For every new thread/task/queue, be able to explain:

-   owner;
-   producer;
-   consumer;
-   ordering guarantee;
-   capacity;
-   backpressure behaviour;
-   shutdown behaviour;
-   failure behaviour.

Prefer bounded channels/queues.

Do not introduce lock-free data structures simply to make the project
appear low latency. Use them only when measurements and ownership
requirements justify them.

## 11. Strategy Separation

Strategy logic consumes normalized market state/events and emits
intents/signals.

It must not own:

-   exchange WebSocket clients;
-   persistence;
-   credential handling;
-   venue-specific JSON decoding.

Initial lead/lag research must clearly separate:

-   observed market relationship;
-   signal;
-   simulated execution assumption;
-   fees;
-   slippage;
-   latency;
-   resulting PnL.

Do not report theoretical spread as executable profit.

## 12. Risk and Live Trading

Live trading is NOT part of the initial milestone.

When execution is introduced, it must be preceded by:

1.  deterministic replay;
2.  execution simulation;
3.  shadow mode;
4.  explicit risk controls.

Any live execution path must eventually include explicit limits and a
kill switch before meaningful capital is used.

Never weaken a risk control to make a test or demo pass.

## 13. Testing

Changes must include tests appropriate to their semantics.

Important areas include:

-   decoder correctness;
-   normalization;
-   order-book reconstruction;
-   gap/recovery behaviour;
-   event ordering;
-   fixed-point arithmetic;
-   replay determinism;
-   recorder round trips;
-   strategy invariants.

Prefer small deterministic fixtures over large opaque fixtures.

Bug fixes should include a regression test when practical.

## 14. Performance Work

Do not claim an optimization without measurement.

For hot-path changes, provide an appropriate benchmark or profile
evidence when the change is performance-motivated.

Report relevant metrics rather than only averages. Depending on the
task, these may include:

-   throughput;
-   allocations;
-   p50;
-   p95;
-   p99;
-   p99.9;
-   maximum/outliers.

Do not alter benchmark methodology merely to make results look better.

## 15. Dependencies

Keep the dependency surface small.

Before adding a crate, state:

-   what requirement it solves;
-   why the standard library/current dependencies are insufficient;
-   whether it touches the hot path;
-   major performance or correctness implications.

Do not add a dependency solely to avoid implementing a small,
domain-critical component that this project intentionally exists to
learn.

## 16. Documentation and ADRs

Architecture-changing decisions belong in an ADR or an explicit update
to `ARCHITECTURE.md`.

Examples:

-   changing the canonical event representation;
-   changing price representation;
-   introducing a shared-state concurrency model;
-   changing recorder format;
-   changing ordering semantics;
-   adding a new execution model.

Do not bury architectural decisions inside implementation commits.

## 17. Agent Workflow

For non-trivial tasks:

1.  Restate the task boundary.
2.  Identify relevant existing components/invariants.
3.  Give a short implementation plan.
4.  Identify any unresolved architecture decision.
5.  Implement only after the task is sufficiently specified.
6.  Run formatting, linting, tests and relevant benchmarks.
7.  Review the resulting diff for unrelated changes.
8.  Report what changed, tests run, benchmark results if applicable, and
    remaining risks/questions.

Do not automatically proceed into another task after completion.

## 18. Definition of Done

A task is done only when:

-   requested behaviour is implemented;
-   repository invariants are preserved;
-   relevant tests pass;
-   formatting/linting passes where configured;
-   relevant benchmarks are run for performance-sensitive work;
-   no unrelated changes remain;
-   new dependencies are justified;
-   architectural changes are documented;
-   remaining limitations or risks are explicitly reported.

A large generated diff is not evidence of progress. A small change whose
semantics are understood, measured and tested is preferred.
