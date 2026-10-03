# 19. ADR 0015: metrics handoff

Type: HITL
Label: ready-for-human
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 67, 68, 69, 70

## What to build

Write ADR 0015 for in-process metrics: counters and latency histograms collected off the hot path through a bounded handoff to a dedicated consumer. Per AGENTS.md §10 state owner, producer, consumer, ordering, capacity, backpressure (metrics may be dropped and counted; market-data events never), shutdown and failure behaviour. Exporter choice explicitly deferred.

## Acceptance criteria

- [ ] ADR 0015 covers all §10 items
- [ ] States that metrics backpressure can never block or drop market-data events
- [ ] Project owner has approved the ADR

## Blocked by

- [03](./03-latency-report.md)
