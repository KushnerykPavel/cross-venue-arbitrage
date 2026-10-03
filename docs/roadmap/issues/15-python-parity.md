# 15. Rust↔Python simulator parity test

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 61, 62

## What to build

On one small committed capture and shared config, the Rust simulation and the Python research simulator produce the same fills and PnL within the ADR 0013 tolerance.

## Acceptance criteria

- [ ] Parity test runs in CI (or documented if capture size forbids)
- [ ] Differences beyond tolerance fail with a readable diff
- [ ] Python simulator remains research tooling; README states Rust is source of truth

## Blocked by

- [12](./12-lighter-and-exits.md)
