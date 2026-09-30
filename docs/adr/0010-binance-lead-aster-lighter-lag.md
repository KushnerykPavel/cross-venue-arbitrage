# ADR 0010: Binance lead with Aster and Lighter as lag venues

## Status

Accepted on 2026-09-30. Supersedes the active venue-set and role clauses in
ADRs 0006 and 0008 and the initial venue list in `PROJECT_SUMMARY.md`.

## Context

The project cannot use Hyperliquid's order book. The intended arbitrage
research compares Binance market movement with lagging markets on Aster and
Lighter. Binance is used to identify the lead signal; the strategy does not
open positions on Binance.

## Decision

- Binance Futures is the lead market-data and signal venue only.
- Aster and Lighter are lag venues. Arbitrage positions may be opened on Aster
  and/or Lighter according to the strategy and execution policy.
- The strategy must never create an order or position on Binance.
- Hyperliquid is excluded from the active venue set because its order book is
  unavailable to this project.
- Active capture and research configurations use Binance, Aster, and Lighter.
- Existing Hyperliquid adapter code and ADRs remain historical records. They do
  not make Hyperliquid an active venue; removing the implementation is separate
  implementation work.

## Consequences

Market-data comparison is intentionally asymmetric: Binance supplies the lead
observation, while Aster and Lighter supply lag observations and possible
execution venues. Market-data capture does not itself authorize orders; any
position opening on Aster or Lighter remains subject to the separately
documented execution and risk policy.

The shared market configuration contract continues to apply to the active
venues, but the prior assumption that Hyperliquid is part of every configured
set is superseded. Strategy and execution code must enforce Binance's
signal-only role before any order path is introduced or changed.
