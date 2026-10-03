# ADR 0011: Latency measurement, benchmark, CI and test tooling

## Status

Accepted on 2026-10-03.

## Context

The project records Local Receive Time and Processing Completion Time on a
shared monotonic clock origin. These timestamps can support a reproducible
measure of local receive-to-processing latency, but no report or benchmark
methodology has yet been agreed. The project also needs consistent automated
correctness checks and dedicated tools for performance measurement, generated
input testing, fuzzing, and dependency policy checks.

Live console output is not suitable for measuring receive latency: as
[ADR 0007](0007-shared-live-market-data-runtime.md) records, presentation can
delay the next receive. Latency evidence must come from captured timestamps or
controlled offline benchmarks, not console timing.

## Decision

### Offline receive-to-processing report

- Compute each event's receive-to-processing duration as Processing
  Completion Time minus Local Receive Time. Both timestamps must retain their
  existing meaning and shared monotonic origin; exchange timestamps are not
  substituted.
- Calculate the report offline from a Capture Run's exported data under the
  capture contract in [ADR 0008](0008-reproducible-market-data-capture.md), and
  group observations by venue. Report p50, p95, p99, p99.9, and maximum, and
  retain Capture Sequence for identifying individual outliers.
- Treat this as observed local receive-to-processing time for the recorded
  process. It is not exchange-to-process or network latency, and it does not
  remove the effect of the approved live runtime and synchronous presentation
  behavior.

### Decode-to-Order Book update benchmarks

- Benchmark the adapter decode and Order Book update path using small,
  deterministic, committed fixtures. A benchmark must identify the exercised
  message types and venue path so results can be reproduced and compared.
- Warm up the code before collecting measured samples. Report p50, p95, p99,
  p99.9, maximum, and allocations per message, along with host, toolchain,
  commit, fixture identity, and benchmark command in a committed baseline
  report.
- Use Criterion to run and organize the benchmark suite and hdrhistogram to
  collect per-operation latency observations and calculate tail percentiles.
  Measure allocation counts in a separate pass from latency so allocation
  instrumentation does not contaminate latency samples. Report measured
  latency as observed; do not subtract an estimated timer overhead.
- Run benchmarks manually or through a documented local command, not in CI.
  Shared-runner scheduling noise must not be presented as comparable latency
  evidence.

### CI scope

- On every push and pull request, GitHub Actions runs `cargo fmt --check`,
  workspace Clippy with warnings denied, backend workspace tests, the Python
  experiment tests, and `cargo deny` using a committed configuration.
- CI does not run performance benchmarks. Benchmark execution and baseline
  updates remain an explicit, environment-controlled activity.

### Development and CI dependencies

All dependencies below are confined to development, test, benchmark, or CI
contexts. None is called by production hot-path code.

| Tool | Requirement and why existing tools are insufficient | Hot-path impact | Correctness and performance implications |
|---|---|---|---|
| `criterion` | Repeatable benchmark harness, fixture iteration, warm-up, and comparison support; ordinary unit-test timing does not provide a benchmark lifecycle or useful statistical summaries. | None; benchmark-only. | Makes controlled performance comparisons practical, but results still depend on the host and must record environment details. |
| `hdrhistogram` | Efficient recording and percentile summaries for per-operation latency observations; the standard library has no histogram implementation. | None; benchmark/report tooling only. | Exposes tail latency and outliers that averages hide. Histogram range and precision must be configured to represent the observations without silently clipping them. |
| `proptest` | Generate broad deterministic and shrinking input cases for decimal and Order Book invariants; hand-written fixtures alone cover only selected examples. | None; test-only. | Can discover edge cases and minimize failing inputs; generated cases do not replace explicit regression tests for discovered bugs. |
| `cargo-fuzz` | Run coverage-guided fuzz targets for adapter text entry points; unit tests and property tests do not continuously mutate inputs based on code coverage. | None; separate fuzz crate and fuzz-only builds. | Helps find panics and invalid state transitions for arbitrary inputs. It requires nightly tooling for fuzz runs, while normal workspace builds remain stable and do not include the fuzz crate. |
| `cargo-deny` | Enforce dependency license, advisory, source, and duplicate policy in CI; Cargo itself does not provide these project policy checks as a configured gate. | None; CI-only tool. | Makes dependency-policy violations visible and repeatable. Its committed policy must be reviewed as dependencies change and must not be weakened to make CI pass. |

The tooling choices follow the dependency discipline in `AGENTS.md` §15:
each tool serves a specific measurement, correctness, or dependency-policy
need and remains outside production runtime paths.

### Deferred work

- In-process latency histograms and their ownership/collection behavior are
  deferred to the metrics ADR. This decision does not add hot-path metrics or
  change event processing.
- CI workflow implementation, benchmark harnesses and reports, offline
  latency report implementation, property tests, fuzz targets, and tool
  installation are separate roadmap issues.

## Consequences

Latency claims can be tied to explicit observations and reproducible methods,
with tail behavior visible alongside maximums and allocation counts. CI checks
correctness and dependency policy without turning noisy shared-runner timing
into a performance gate. Development tools add maintenance and configuration
work, but do not add runtime dependencies or alter production hot-path
behavior.
