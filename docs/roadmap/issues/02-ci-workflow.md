# 02. CI workflow: fmt, clippy, tests, pytest, cargo deny

Type: AFK
Label: ready-for-agent
Status: open

## Parent

[HFT portfolio roadmap PRD](../hft-portfolio-prd.md) — user stories 1, 2, 3, 4, 5, 6, 7

## What to build

Every push and pull request on GitHub runs the checks agreed in ADR 0011 and fails on any violation. Existing code is brought to a green state so the workflow passes on main.

## Acceptance criteria

- [ ] Workflow runs `cargo fmt --check`, `cargo clippy` with warnings denied, `cargo test` for the backend workspace
- [ ] Workflow runs the Python experiment tests
- [ ] Workflow runs `cargo deny` with a committed configuration
- [ ] No benchmark job is present
- [ ] Any pre-existing lint or format failures are fixed without changing behaviour (tests still pass)
- [ ] Workflow is green on main; README or PROJECT_SUMMARY shows a CI badge

## Blocked by

- [01](./01-adr-0011-latency-ci-tooling.md)
