# 12. Add Lighter lag venue and position exits

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 24, 26

## What to build

Extend the simulation to Lighter as a second lag venue (with its configured fees and taker delay) and add flatten-on-max-holding-time and flatten-on-signal-reversal to the taker policy.

## Acceptance criteria

- [ ] Config selects Aster, Lighter or both
- [ ] Lighter fees (0/0) and configured taker delay applied
- [ ] Policy tests: flatten after max holding time; flatten on reversal
- [ ] Digest test still passes

## Blocked by

- [11](./11-tracer-bullet-taker-aster.md)
