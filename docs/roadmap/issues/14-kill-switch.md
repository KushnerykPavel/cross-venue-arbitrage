# 14. Latched kill switch with deterministic reset

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 53, 54, 55

## What to build

Add the latched kill switch to the risk gate: trips on Order Book error, loss limit or manual command; stays tripped until an explicit reset event; in replay the reset is a recorded or configured event.

## Acceptance criteria

- [ ] Tests: each trigger trips the switch; switch rejects all new intents while tripped
- [ ] Book recovery alone does not reset it
- [ ] Reset event in replay config resets it deterministically (digest stable)
- [ ] Report shows trip and reset events

## Blocked by

- [13](./13-risk-gate-v1.md)
