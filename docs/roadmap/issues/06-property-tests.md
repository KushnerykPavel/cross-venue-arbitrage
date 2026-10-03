# 06. Property tests: ExactDecimal and Lighter Order Book invariants

Type: AFK
Label: ready-for-agent
Status: done

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 63, 64

## What to build

Generated-input tests verify exactness of `ExactDecimal` parsing/arithmetic and Order Book reconstruction invariants for the Lighter book.

## Acceptance criteria

- [x] Round-trip parse/format property for ExactDecimal
- [x] Arithmetic properties hold without loss (e.g. commutativity, scale handling)
- [x] After any sequence of valid updates, levels are sorted and quantities positive
- [x] A delete removes its level; an invalid update makes the Order Book unavailable rather than corrupting it
- [x] Tests run in CI

## Blocked by

- [01](./01-adr-0011-latency-ci-tooling.md)
