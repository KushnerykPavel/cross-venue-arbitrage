# Adapter benchmark suite

Run from the repository root:

```sh
cargo bench --manifest-path backend/benches/Cargo.toml --bench adapter_paths
```

The isolated benchmark workspace keeps Criterion and hdrhistogram outside the
normal backend workspace build. Criterion reports its benchmark distributions;
the harness then performs a separate 10,000-operation warm-up and 100,000
per-operation samples for p50, p95, p99, p99.9, and maximum. A separate
10,000-operation allocation pass reports allocator calls per message.
Allocation instrumentation is disabled during latency measurement, and timer
overhead is included in reported raw observations rather than subtracted.

Fixtures in `fixtures/` are small, deterministic adapter inputs. Binance
depth and aggregate-trade decode, Aster depth update, and Lighter incremental
update and level deletion are covered. Feature-gated `dev-fixtures` constructors
avoid live metadata requests; they are enabled only by this benchmark package
and the separate fuzz workspace.

The command prints the measurements needed for a baseline. Save a baseline
report with the host, Rust toolchain, clean commit, fixture identity, and this
command before using it for comparisons. Benchmarks are intentionally excluded
from CI.
