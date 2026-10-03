# 17. Queue-estimate maker fill model and side-by-side PnL

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 37, 38

## What to build

Add the queue-position fill model: on joining a level, queue ahead = displayed size; trades at the level reduce it; cancels reduce it pro rata; fill when it reaches zero. Report shows maker PnL under both fill models side by side.

## Acceptance criteria

- [ ] Venue tests for queue decrement by trades and pro-rata cancels
- [ ] Report has both PnLs and their difference
- [ ] Fill model selectable in config; digest stable per model

## Blocked by

- [16](./16-maker-trade-through.md)
