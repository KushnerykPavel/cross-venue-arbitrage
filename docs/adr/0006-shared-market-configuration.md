# ADR 0006: Shared market configuration

## Status

Accepted on 2026-09-18. Supersedes the venue-specific configuration clauses in
ADRs 0003, 0004, and 0005.

## Context

Lead/lag research compares the same markets across all configured venues.
Separate venue lists can silently produce incomplete comparisons and require
duplicating identical local configuration.

## Decision

- `MARKET_COINS` is the only market-list environment variable.
- It contains one comma-separated Configured Market Set used by Aster,
  Hyperliquid, and Lighter.
- Entries are trimmed. Empty entries, remaining whitespace or control
  characters, and exact duplicates are rejected at startup. Case and
  punctuation are preserved.
- `trader` parses and validates the value once, then supplies the same ordered
  list to every Live Market Data Session.
- Each venue still performs its own metadata or protocol validation. Therefore,
  startup fails if a configured Market Coin is not supported by a venue that
  requires metadata resolution.
- `backend/.env` supplies local values and remains ignored. Process environment
  values take precedence over the file; `backend/.env.example` documents the
  contract.

## Consequences

Every live run observes the same intended markets on all three venues. A future
requirement for asymmetric venue coverage must explicitly replace this contract
rather than introducing an undocumented fallback.
