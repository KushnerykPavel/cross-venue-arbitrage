# 13. Risk gate v1 in the simulation path

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 48, 49, 50, 51, 52, 56, 57

## What to build

Insert the risk gate between strategy and simulated venue: Binance unrepresentable as order venue, max position per venue, max notional, max open orders, reject on unavailable/stale Order Book. Rejections with reasons appear in the report.

## Acceptance criteria

- [ ] Order venue type cannot express Binance (compile-time; documented)
- [ ] Each check has accept/reject boundary tests through the gate's public interface
- [ ] Report lists rejections with reasons
- [ ] Digest test still passes

## Blocked by

- [10](./10-adr-0014-risk-policy.md)
- [11](./11-tracer-bullet-taker-aster.md)
