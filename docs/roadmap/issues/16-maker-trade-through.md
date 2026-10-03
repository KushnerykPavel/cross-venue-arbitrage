# 16. Maker policy with trade-through fills

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 25, 36, 39, 40

## What to build

Add a maker execution policy sharing the lead/lag signal: quote on the lag venue, lean or cancel when Binance moves. Simulated venue fills resting quotes only when a Market Trade prints strictly through the quote price; cancels take simulated latency so a quote can fill while its cancel is in flight.

## Acceptance criteria

- [ ] Policy tests: quote placement, lean, cancel on Binance move
- [ ] Venue tests: no fill on touch; fill on trade-through; partial fills; fill during in-flight cancel
- [ ] Maker fee (0) applied exactly
- [ ] Risk gate open-order limit enforced
- [ ] Digest test passes

## Blocked by

- [12](./12-lighter-and-exits.md)
- [13](./13-risk-gate-v1.md)
