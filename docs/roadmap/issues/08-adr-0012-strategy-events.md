# 08. ADR 0012: strategy event semantics

Type: HITL
Label: ready-for-human
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 17, 18, 19, 20, 27, 70, 71

## What to build

Write ADR 0012 settling the open ARCHITECTURE.md decision on strategy-facing event semantics: after each engine update the strategy receives the triggering Normalized Market Event and a read-only market view (per venue and Market Coin: best bid/ask, depth access, Order Book availability, last Local Receive Time). No timer sampling. Strategy emits intents (place, cancel, flatten) only and owns no clients, persistence or credentials.

## Acceptance criteria

- [ ] ADR 0012 exists and defines the market view contents and borrowing (no book copies)
- [ ] Intent vocabulary defined
- [ ] Live/replay indifference stated (AGENTS.md §5.2)
- [ ] ARCHITECTURE.md open decision updated to reference ADR 0012
- [ ] Project owner has approved the ADR

## Blocked by

None - can start immediately
