# 07. Fuzz crate: adapter text entry points

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 65, 66

## What to build

A separate fuzz crate (outside the workspace build, nightly only) fuzzes each active adapter's text decoding entry point (Binance, Aster, Lighter). Arbitrary input must never panic and must never leave an Order Book accepted with invalid state.

## Acceptance criteria

- [ ] Fuzz crate is not a workspace member; normal builds need no nightly
- [ ] One target per active venue adapter
- [ ] Each target asserts no panic and Order Book invariants after every input
- [ ] Documented command to run; any crash found gets a regression unit test
- [ ] A seed corpus from small real frames is committed

## Blocked by

- [01](./01-adr-0011-latency-ci-tooling.md)
