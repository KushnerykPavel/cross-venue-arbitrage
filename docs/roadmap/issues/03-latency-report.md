# 03. Latency report: receive→processing percentiles per venue

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 8, 9, 10, 11, 12, 16

## What to build

Given a Parquet export of a Capture Run, an analysis produces a dated report of receive→processing latency (Processing Completion Time minus Local Receive Time) per venue, with tail percentiles and listed outliers, following ADR 0011.

## Acceptance criteria

- [ ] Analysis computes p50, p95, p99, p99.9 and max per venue
- [ ] Top outliers listed with their Capture Sequence
- [ ] Report records Capture Run identity, host and build
- [ ] Report states the ADR 0007 console/presentation caveat
- [ ] Pytest on a small Parquet fixture with known percentiles passes
- [ ] A report for one real Capture Run is committed under experiments/reports

## Blocked by

- [01](./01-adr-0011-latency-ci-tooling.md)
