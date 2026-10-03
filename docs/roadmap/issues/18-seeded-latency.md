# 18. Seeded latency sampling from measured distribution

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 32

## What to build

Simulated venue latency can be sampled from the measured distribution in the latency report using a fixed seed, keeping replay deterministic.

## Acceptance criteria

- [ ] Config chooses fixed or sampled latency
- [ ] Same seed → identical digest; different seed → documented variation
- [ ] Distribution source (report version) recorded in the simulation report

## Blocked by

- [03](./03-latency-report.md)
- [11](./11-tracer-bullet-taker-aster.md)
