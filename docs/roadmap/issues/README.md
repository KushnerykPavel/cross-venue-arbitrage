# HFT portfolio roadmap: issues

Source: [PRD](../hft-portfolio-prd.md). Work blockers first.

| # | Title | Type | Blocked by | Status |
|---|---|---|---|---|
| 01 | [ADR 0011: latency measurement, benchmark, CI and test tooling dependencies](./01-adr-0011-latency-ci-tooling.md) | HITL | — | done |
| 02 | [CI workflow: fmt, clippy, tests, pytest, cargo deny](./02-ci-workflow.md) | AFK | 01 | open |
| 03 | [Latency report: receive→processing percentiles per venue](./03-latency-report.md) | AFK | 01 | open |
| 04 | [Benchmark baseline: Binance decode → Order Book update](./04-bench-binance.md) | AFK | 01 | done |
| 05 | [Benchmarks for Aster and Lighter](./05-bench-aster-lighter.md) | AFK | 04 | done |
| 06 | [Property tests: ExactDecimal and Lighter Order Book invariants](./06-property-tests.md) | AFK | 01 | done |
| 07 | [Fuzz crate: adapter text entry points](./07-fuzz-adapters.md) | AFK | 01 | open |
| 08 | [ADR 0012: strategy event semantics](./08-adr-0012-strategy-events.md) | HITL | — | open |
| 09 | [ADR 0013: execution simulation](./09-adr-0013-execution-sim.md) | HITL | 08 | open |
| 10 | [ADR 0014: risk policy and kill switch](./10-adr-0014-risk-policy.md) | HITL | 08 | open |
| 11 | [Tracer bullet: replay simulation on Aster with taker policy](./11-tracer-bullet-taker-aster.md) | AFK | 08, 09 | open |
| 12 | [Add Lighter lag venue and position exits](./12-lighter-and-exits.md) | AFK | 11 | open |
| 13 | [Risk gate v1 in the simulation path](./13-risk-gate-v1.md) | AFK | 10, 11 | open |
| 14 | [Latched kill switch with deterministic reset](./14-kill-switch.md) | AFK | 13 | open |
| 15 | [Rust↔Python simulator parity test](./15-python-parity.md) | AFK | 12 | open |
| 16 | [Maker policy with trade-through fills](./16-maker-trade-through.md) | AFK | 12, 13 | open |
| 17 | [Queue-estimate maker fill model and side-by-side PnL](./17-maker-queue-estimate.md) | AFK | 16 | open |
| 18 | [Seeded latency sampling from measured distribution](./18-seeded-latency.md) | AFK | 03, 11 | open |
| 19 | [ADR 0015: metrics handoff](./19-adr-0015-metrics-handoff.md) | HITL | 03 | open |
| 20 | [Metrics crate: counters and histograms over bounded handoff](./20-metrics-crate.md) | AFK | 19 | open |
