# 11. Tracer bullet: replay simulation on Aster with taker policy

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 17, 18, 19, 20, 21, 22, 23, 24, 28, 29, 30, 31, 33, 34, 35, 39, 41, 42, 43, 44, 45, 46, 47, 58, 59, 60

## What to build

A replay simulation mode runs a Capture Run end-to-end: engine exposes the market view (ADR 0012) → lead/lag signal (pure, Binance lead) → taker policy on Aster only, fixed size → simulated venue (fixed latency, book walk per ADR 0013) → ledger (ExactDecimal positions, fees, realised/unrealised PnL) → report with fills, PnL breakdown and a decision digest.

## Acceptance criteria

- [ ] Engine test: market view reflects latest accepted Order Book and availability after each event
- [ ] Signal fixture tests incl. unavailable-book cases
- [ ] Simulated venue tests: book walk with partial depth, latency ordering, rejection on unavailable Order Book
- [ ] Ledger tests: exact fee (Aster taker 40 tenths-bps) and PnL for known fills
- [ ] Running twice on the same capture and config yields identical digests (test)
- [ ] Report separates gross edge, fees and slippage/latency effect; no floats before the report boundary
- [ ] Execution Fills are a distinct type from Market Trades

## Blocked by

- [08](./08-adr-0012-strategy-events.md)
- [09](./09-adr-0013-execution-sim.md)
