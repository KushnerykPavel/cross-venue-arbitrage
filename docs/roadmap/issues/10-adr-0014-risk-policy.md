# 10. ADR 0014: risk policy and kill switch

Type: HITL
Label: ready-for-human
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 70, 71

## What to build

Write ADR 0014: six pre-trade checks — Binance cannot be an order venue (type-level), max position per venue, max notional, max open orders, reject on unavailable/stale Order Book (configured age), latched kill switch tripped by Order Book error, loss limit or manual command, reset only by explicit event (recorded/configured in replay). Every rejection carries a reason. Gate sits between strategy and venue for both simulation and any future live path.

## Acceptance criteria

- [ ] ADR 0014 exists covering all six checks and kill-switch semantics
- [ ] States that risk controls are never weakened to make tests pass
- [ ] ARCHITECTURE.md risk-policy open decision updated
- [ ] Project owner has approved the ADR

## Blocked by

- [08](./08-adr-0012-strategy-events.md)
