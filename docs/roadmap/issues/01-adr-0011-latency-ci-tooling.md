# 01. ADR 0011: latency measurement, benchmark, CI and test tooling dependencies

Type: HITL
Label: ready-for-human
Status: done

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 15, 70, 71

## What to build

Write ADR 0011 recording how latency is measured and which dev-only tooling the project adopts. Covers: the offline receive→processing latency report computed from Capture Runs (Processing Completion Time minus Local Receive Time), the decode → Order Book update microbenchmark methodology, the GitHub Actions CI scope, and AGENTS.md §15 justification for each dev-only dependency: `criterion`, `hdrhistogram`, `proptest`, `cargo-fuzz` (separate crate outside the workspace build), `cargo-deny` (CI tool). Records that benchmarks are not run in CI and that in-process latency histograms are deferred to the metrics ADR.

## Acceptance criteria

- [x] ADR 0011 exists under docs/adr in the existing ADR format
- [x] Each dependency states requirement, why std/current deps are insufficient, hot-path impact (none), and correctness/performance implications
- [x] Benchmark methodology is defined: fixtures, warm-up, reported metrics (p50/p95/p99/p99.9/max, allocations per message)
- [x] ADR 0007 console/presentation caveat for live receive latency is recorded
- [x] ARCHITECTURE.md references ADR 0011
- [x] Project owner has approved the ADR

## Blocked by

None - can start immediately
