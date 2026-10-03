# 20. Metrics crate: counters and histograms over bounded handoff

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 67, 68

## What to build

Implement the metrics crate per ADR 0015 and wire receive→processing latency histograms and Capture Run quality counters through it, with a periodic summary written by the consumer (no exporter).

## Acceptance criteria

- [ ] Hot-path producers never block (test or benchmark evidence)
- [ ] Dropped-metric count is itself reported
- [ ] Benchmark from ticket 04 re-run; any overhead reported
- [ ] Summary output documented

## Blocked by

- [19](./19-adr-0015-metrics-handoff.md)
