# 04. Benchmark baseline: Binance decode → Order Book update

Type: AFK
Label: ready-for-agent
Status: done

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 13, 14, 15

## What to build

A criterion benchmark replays fixed Binance depth and aggTrade fixtures through the adapter's decode and Order Book update path and reports tail percentiles and allocations per message, as defined in ADR 0011. A committed baseline report documents the result.

## Acceptance criteria

- [x] Benchmark runs from a documented command outside CI
- [x] Fixtures are small, deterministic and committed
- [x] Output includes p50/p95/p99/p99.9/max via hdrhistogram and allocations per message
- [x] Baseline report committed with host, toolchain and commit
- [x] No production code behaviour changes

## Blocked by

- [01](./01-adr-0011-latency-ci-tooling.md)
