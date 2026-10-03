# 09. ADR 0013: execution simulation

Type: HITL
Label: ready-for-human
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 61, 70, 71

## What to build

Write ADR 0013 settling execution timestamp representation and the simulation model: simulated clock from Local Receive Time; decision time = Processing Completion Time of the triggering event; fixed per-venue send/ack/fill latencies (seeded distributions later); exchange timestamps never drive fills; taker fills walk the replayed Order Book with no lasting impact (limitation stated); maker v1 trade-through, v2 queue estimate; partial fills; cancel latency; rejection on unavailable Order Book; flatten on max holding time or signal reversal; no cross-venue hedge; ExactDecimal money and integer tenths-of-bps fees; Rust↔Python parity tolerance.

## Acceptance criteria

- [ ] ADR 0013 exists covering every item above
- [ ] Order state lifecycle defined (pending, resting, partially filled, filled, cancelled, rejected)
- [ ] Parity tolerance stated with reason (Python uses floats)
- [ ] ARCHITECTURE.md open decisions (execution timestamps, execution policy) updated
- [ ] Project owner has approved the ADR

## Blocked by

- [08](./08-adr-0012-strategy-events.md)
