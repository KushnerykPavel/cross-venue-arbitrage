# Kraken and Keyrock HFT engineering requirements: repository gap analysis

Research date: 2026-10-02.

This note answers one question: which parts of this repository already show
the skills that Kraken (an exchange) and Keyrock (a crypto market maker)
advertise for trading-systems roles, and which small additions would show
more. It recommends nothing. Section 5 lists candidates. Choosing among them
is an architecture decision for the human owner (AGENTS.md §2).

## 1. Sources

### 1.1 Job postings (primary)

Both firms now post on Ashby. The old Lever boards, `jobs.lever.co/kraken` and
`jobs.lever.co/keyrock`, return HTTP 404. The careers pages
`kraken.com/careers` and `keyrock.com/careers` link to Ashby. Posting text was
retrieved on 2026-10-02 from Ashby's public posting API
(`api.ashbyhq.com/posting-api/job-board/{kraken.com|keyrock}`). That API
returns the same text as the public `jobs.ashbyhq.com` pages cited below.
"Published" is the `publishedAt` value Ashby reports.

| ID | Firm | Title (location) | Published | Role type | URL |
|---|---|---|---|---|---|
| K1 | Kraken | Senior Software Engineer - Rust - Backend - Pro (Romania) | 2026-08-31 | IC, direct match | <https://jobs.ashbyhq.com/kraken.com/c0d90cde-96ab-455a-b613-16b2f395e683> |
| K2 | Kraken | Senior Software Engineer - Rust - Core Services (Romania) | 2026-09-15 | IC, platform | <https://jobs.ashbyhq.com/kraken.com/b2a32764-7b75-4c6d-a45a-58b7af0887ad> |
| K3 | Kraken | Senior Software Engineer - Rust - Payward Services (Poland) | 2026-07-13 | IC, adjacent | <https://jobs.ashbyhq.com/kraken.com/d6d4cda0-81c5-4b88-9af5-f3bc1d48e1bc> |
| K4 | Kraken | Engineering Manager - Pro (United Kingdom) | 2026-08-26 | Manager; team-value signal | <https://jobs.ashbyhq.com/kraken.com/24cb6a23-0ac4-4198-baf0-d8099067300d> |
| K5 | Kraken | Head of Exchange Infrastructure (US; UK copy `d7e00f01-…`) | 2026-09-17 | Product; team-value signal | <https://jobs.ashbyhq.com/kraken.com/e781688a-34b4-4b74-a817-f689cbddb1b7> |
| R1 | Keyrock | Senior Rust Engineer - High Frequency Trading (HFT) (Brussels) | 2026-08-10 | IC, direct match | <https://jobs.ashbyhq.com/keyrock/e0cbd0a8-7acf-4513-9f0b-1536624babc6> |
| R1′ | Keyrock | Rust Engineer - Trading Systems (Brussels) | 2026-07-15 | IC; **body identical to R1** | <https://jobs.ashbyhq.com/keyrock/7329eacf-83d2-419c-b519-8288659bf71b> |
| R2 | Keyrock | Engineering Lead - Options (New York) | 2026-04-22 | Lead; team-value signal | <https://jobs.ashbyhq.com/keyrock/638f8c1e-381f-481f-9bc7-5b34fc694aae> |

Notes on the postings:

- R1 and R1′ have the same body text, so this note treats them as one
  requirement source.
- K1 is the closest Kraken IC match. Its team, Pro, "is responsible for
  everything to do with the trading backend services such as the matching
  engine, market data gateways, internal and external APIs". K4 describes the
  same area under the Exchange / Trading Technologies name.
- K4, K5 and R2 are management or product roles. Their technical items
  (Aeron, multicast, p99 latency targets, gRPC, PostgreSQL, FPGA, order types)
  are tagged *team-value signal*. They show what those teams care about, but
  they are not IC requirements.
- At this date Kraken's live board has no posting titled HFT, low-latency,
  C++ trading or matching-engine IC. Of 81 Kraken postings, those matching
  "matching engine" or "market data" were K1, K4 and K5.

Fetch failures and unverified listings:

- `jobs.lever.co/kraken`: HTTP 404. `jobs.lever.co/keyrock`: HTTP 404.
- Aggregators show a Kraken "Senior Software Engineer - C++ - Trading
  Technologies" listing, for example
  `builtinnyc.com/job/senior-software-engineer-c-trading-technologies/9508862`.
  That page returned HTTP 403, and the role is not on Kraken's live Ashby
  board. It is **unverified**, and no requirement in this note comes from it.
- Search snippets from aggregators (web3.career, workingnomads, builtin*) were
  used only to find postings, never as a requirement source.

### 1.2 First-party engineering content

| ID | Firm | Title | Date | URL | What it says (summarised from fetch) |
|---|---|---|---|---|---|
| KB1 | Kraken | Oxidizing Kraken: Improving Kraken Infrastructure Using Rust (S. Chemouil) | 2021-02-19 | <https://blog.kraken.com/post/7964/oxidizing-kraken-improving-kraken-infrastructure-using-rust/> | Rust rewrite since mid-2018. Tokio RPC servers "support a throughput of 150k request/second per instance while keeping p99.9 latencies below 3ms". An integration suite runs against both old and new services to check behavioural parity. Clippy, sccache, rust-analyzer. |
| KB2 | Kraken | Oxidizing Kraken, Part 2: from bet to backbone (S. Chemouil) | 2025-12-16 | <https://blog.kraken.com/product/engineering/rust-part-2-from-bet-to-backbone> | Async Tokio migration after "connection storms and blocking I/O". In-house Kafka stream-processing framework in Rust. gRPC/Protobuf between services. Shared telemetry/tracing libraries. |
| KB3 | Kraken | Performance at Kraken (Kurtas, Kaplan, Gandhi, Hunt) | 2023-04-06 | <https://blog.kraken.com/post/17936/performance-at-kraken/> | Latency is measured as request-to-acknowledgement round trip and reported as percentiles (P25, P95). Jitter is treated as a first-class metric (stayed under 30 ms during Nov 2022). Matching-engine latency went "from milliseconds to microseconds". Planned FIX API and Aeron. K4 links to this post. |
| KB4 | Kraken | War games: how we built Kraken to handle 10x the load | 2026-08-06 | <https://blog.kraken.com/product/engineering/10x-the-load> | Production load tests at ≥ 2× the historical peak, and >230k successful req/s. The post points to a technical companion post for details. **That companion post was not found.** |
| RB1 | Keyrock (third-party host) | Rust Foundation Member Spotlight: Keyrock | 2022-09-27 | <https://rustfoundation.org/media/member-spotlight-keyrock/> | Hosted by the Rust Foundation. It quotes Keyrock's CTO, Jeremy de Groodt: Rust is used for "complex, distributed trading systems", with emphasis on speed, safety and 24/7 operation. **Secondary host with first-party quotes.** |

No first-party Keyrock engineering blog post was found. A `site:keyrock.com`
search returned only aggregator job listings. Keyrock's technical priorities
here therefore come mostly from R1 and R2.

## 2. Extracted requirements

The wording below is close to the source text. "Req" means the posting lists
the item under required skills (*What you bring*, *Technical skills*,
*Required experience*). "Pref" means *Nice to have*, *strong advantage*,
*preferred* or *a plus*.

### Kraken

- K1/K2/K3 req: "Proficient in writing network services or asynchronous code
  in Rust"; "security-first mindset during system design"; "autonomously debug
  issues across the stack (OS, network, application)"; "solid understanding of
  distributed systems and technologies, including RPC protocols, Kafka, and
  Event-Driven Systems"; "complete end-to-end ownership of systems and
  libraries".
- K1 req: "7+ years of software engineering experience" (K3: 5+); "Experience
  building financial trading focused products"; mentorship.
- K1/K2 duties: "reusable, testable, and highly efficient code"; "resilient,
  low-latency solutions". K2 adds "observability, auditability".
- K4 (team-value signal): Linux experience with "distributed and/or
  highly-concurrent systems; low-latency and/or high-volume transaction
  environments"; "Proficiency in Rust and/or C++"; "knowledge of order types,
  trading systems, and financial products"; "Aeron or similar messaging/
  transport systems a strong plus"; "multicast-based market data distribution a
  plus"; "Champion performance and market data work as top priorities"; BS in a
  technical or quantitative field.
- K5 (team-value signal): "Define success metrics (p99 latency, throughput,
  uptime, resiliency)"; "matching engine and market data architecture, and the
  tradeoffs low latency forces".

### Keyrock

- R1 req: "Production-grade Rust experience building high-performance,
  concurrent systems; proficiency with Tokio is a strong advantage";
  "Hands-on experience in a systematic trading environment (HFT, Market Making,
  Quantitative Trading, or similar), with solid understanding of execution,
  market data, and operational risk"; "Strong AWS and Linux expertise ...
  cloud infrastructure trade-offs for latency-sensitive workloads"; "Python ...
  for strategy development, automation, and operational tooling"; "trading
  system protocols (FIX, REST, WebSocket), exchange connectivity, and market
  data feeds"; "low-latency networking: TCP/UDP, packet and latency path
  analysis, dedicated network infrastructure, and kernel bypass techniques
  (DPDK)"; agentic engineering tools; "sound architectural decisions, articulate
  trade-offs clearly".
- R1 duties: "observability, and operational excellence"; "Troubleshoot complex
  production issues across exchange connectivity, execution, and market data
  handling"; "functional, component, and production-readiness testing".
- R2 (team-value signal): 7+ years; "Rust, C++, or C"; "automated quoting,
  execution, pricing, risk controls, and market data workflows"; "PostgreSQL
  preferred"; "functional testing, component testing, and production-grade
  validation"; "gRPC ... preferred"; "crypto exchange connectivity and/or FPGA
  technologies is strongly preferred".

## 3. Current repository state (evidence)

| Area | State | Evidence |
|---|---|---|
| Venue adapters as deterministic state machines over a shared Tokio/Tungstenite runtime | Have | `backend/crates/venue/src/runtime.rs`, `transport.rs`; ADR 0007 |
| Binance depth + aggTrade, Aster, Lighter adapters with recovery and dedup | Have | `backend/crates/venue-{binance,aster,lighter}/src/adapter.rs`; `backend/crates/venue/src/deduplication.rs`; ADRs 0004, 0005, 0009 |
| Incremental L2 reconstruction with nonce continuity (Lighter) | Have | `BTreeMap<Price, Quantity>` book at `backend/crates/venue-lighter/src/adapter.rs:290-291`; `docs/research/lighter-order-book-protocol.md` |
| Exact price/quantity | Have | `ExactDecimal { coefficient: i128, scale: u8 }` in `backend/crates/domain/src/decimal.rs`; `Price`/`Quantity` wrappers. `ARCHITECTURE.md` still lists "canonical price and quantity scales" as an open decision |
| Separate time notions | Have | Local Receive Time and Processing Completion Time from one monotonic origin (`backend/crates/venue/src/clock.rs`, `Instant`); exchange time preserved as observation (`CONTEXT.md`) |
| Append-only capture with bounded, fail-closed handoff | Have | `crossbeam_channel::bounded` at `backend/crates/recorder/src/capture.rs:103`; CRC32C, SHA-256, manifest, crash recovery in `backend/crates/recorder/src/`; ADR 0008 |
| Deterministic replay into the same engine entry point | Have | `MarketDataEngine::process` checks Capture Sequence and updates a digest (`backend/crates/engine/src/lib.rs:86-97`); `replay_is_deterministic_and_ends_on_the_last_recorded_books` at `backend/apps/replay/src/main.rs:233` |
| Shared mutable state in live path | Documented decision, contention not measured | `Arc<Mutex<_>>` for capture and engine at `backend/apps/trader/src/main.rs:28-29`; justified in the ADR 0007 amendment (2026-09-30) to keep each frame's batch contiguous |
| Parquet export for analytics | Have | `backend/apps/export-parquet/` |
| Rust strategy / execution / risk / metrics / reconciliation | Missing (one-line stubs) | `backend/crates/strategy/src/*.rs`, `execution/src/*.rs`, `risk/src/lib.rs`, `metrics/src/lib.rs`, `reconciliation/src/lib.rs` |
| Execution simulator and strategies | Python only, float values | `experiments/simulator/` (floats at the research adapter, `experiments/simulator/data.py:21`). AGENTS.md §5.2 forbids a separate backtest strategy implementation; the Python simulator is research tooling, not the shared core |
| Lead/lag research with honest cost accounting | Have (research) | `experiments/reports/2026-10-01-lead-lag.md`: lead is real, but a taker/taker round trip does not beat a 5 bps fee; `experiments/reports/2026-10-01-binance-book-quality.md` |
| Feed cadence percentiles | Have (offline) | Update p50/p99 per venue in `experiments/reports/2026-10-01-lead-lag.md` |
| Receive→processing latency percentiles (p50/p99/p99.9/max) | Missing | Fields exist in the Parquet schema (printed in `experiments/notebooks/06_opportunity_lifetime.ipynb`). No analysis or report computes them |
| Benchmarks | Missing | `backend/benches/` is empty. No `criterion`/`hdrhistogram` dependency in any `Cargo.toml` |
| Workspace integration tests | Missing (unit/module tests exist) | `backend/tests/` is empty. Roughly 90 `#[test]` functions in crate modules, e.g. `backend/crates/recorder/src/tests.rs` (17) |
| Property/fuzz tests for decoders and books | Missing | No `proptest`/`cargo-fuzz` dependency |
| Observability | Missing | `observability/` is empty. No `tracing`/`prometheus` dependency |
| CI (fmt/clippy/test) | Missing | No `.github/` or other CI config in repository root |
| Decode-path allocation | Unmeasured | `serde_json` decoding in `venue-*/src/adapter.rs` (e.g. `venue-binance/src/adapter.rs:129`); `format!` in `ExactDecimal::parse_positive` (`domain/src/decimal.rs`). These are allocation sites; their cost has not been measured |
| Containerised VPS capture | Have | `compose.yaml`, `Taskfile.yaml`, `DEPLOYMENT.md`, `backend/Dockerfile` |

## 4. Gap matrix (project-demonstrable requirements)

Firm tags: **K** = Kraken, **R** = Keyrock. *(signal)* = taken only from a
manager/product posting.

| # | Requirement | Firm(s) | Source | Category | Demonstrable by project? | Current state | What would close the gap (candidate) |
|---|---|---|---|---|---|---|---|
| 1 | Async/network services in Rust; Tokio | K, R | K1–K3 req; R1 req (Tokio "strong advantage"); KB1, KB2 | Rust | Yes | **Have**: Tokio + Tungstenite runtime with reconnect, heartbeats, scripted transport tests (`venue/src/runtime.rs`, `transport.rs`) | Mostly shown already. Main remaining lever is measurement (row 3) |
| 2 | Market-data feeds, exchange connectivity (WebSocket/REST) | K, R | K1 team; R1 req; K4/K5 *(signal)* | Market data | Yes | **Have**: three active venues, snapshot/incremental/gap/reconnect handling (ADRs 0004, 0005, 0009) | Workspace integration tests under `backend/tests/` replaying small recorded fixtures through each adapter (AGENTS.md §13) |
| 3 | Low latency, reported as percentiles (p99/p99.9, jitter) | K, R | K1–K3 duties; K5 *(signal)* p99; KB1 p99.9; KB3 P25/P95/jitter; R1 | Performance | Yes | **Partial**: monotonic timestamps exist (`venue/src/clock.rs`); feed-cadence percentiles offline only. **Missing**: receive→processing p50/p95/p99/p99.9/max, and any benchmark (`backend/benches/` empty) | (a) Offline report computing `processing_completion_time − local_receive_time` per venue from an existing capture, with the caveat from ADR 0007 that console/presentation can delay receives. (b) A documented benchmark of decode→book-update on fixed fixtures, reporting tail percentiles and allocations (AGENTS.md §14). Adding `criterion`/`hdrhistogram` needs a §15 justification |
| 4 | High-performance, concurrent Rust systems | R | R1 req | Concurrency | Partial | **Partial**: bounded recorder thread with explicit backpressure (`recorder/src/capture.rs:103`); `Arc<Mutex<_>>` documented (ADR 0007 amendment) with no contention data | Measure lock hold and wait time on the engine/capture mutex under recorded load before any change. Any redesign needs an ADR (AGENTS.md §5.4, §10) |
| 5 | Execution, operational risk, risk controls | R; K4 *(signal)* order types | R1 req; R2 *(signal)* | Execution / risk | Yes (simulated) | **Missing in Rust**: `execution`, `risk` are stubs. Python simulator models latency, partial fills, cancels, fees (`experiments/simulator/`) | A Rust execution simulator and pre-trade risk checks (position, notional, Binance signal-only rule from ADR 0010, kill switch) fed by deterministic replay. This is the "execution simulation" step that AGENTS.md §12 requires before any live path. Needs human decisions on execution timestamps and risk policy (ARCHITECTURE.md open decisions) |
| 6 | Strategy development; systematic trading / market making | R | R1 req; R2 *(signal)* quoting/pricing | Strategy | Partial | **Partial**: lead/lag research with honest net-of-fee results (`experiments/reports/2026-10-01-lead-lag.md`). Rust `strategy` crate empty | Port one fixed lead/lag rule into the `strategy` crate as a pure function of normalized events, then verify it reproduces identical decisions across replays (digest-style test as in `apps/replay/src/main.rs:233`). Market making stays a later milestone (PROJECT_SUMMARY.md roadmap) |
| 7 | Order types, matching engine, order-book architecture | K | K1 team; K4, K5 *(signal)* | Exchange core | Partial | **Partial**: L2 book reconstruction (`venue-lighter/src/adapter.rs`, `market-data/src/order_book.rs`). No order-level or matching semantics | The exchange simulator in row 5 is the natural place for limit/IOC/post-only fill and cancel semantics. A standalone matching engine is outside the stated mission (AGENTS.md §1) and would be scope creep unless the owner decides otherwise |
| 8 | Reusable, testable code; component/functional/production-readiness testing | K, R | K1/K2 duties; R1 duties; R2 *(signal)*; KB1 parity suite | Testing | Yes | **Partial**: about 90 module tests; deterministic replay test. **Missing**: `backend/tests/`, property/fuzz tests, CI | (a) CI running `cargo fmt --check`, `clippy -D warnings`, `cargo test`, plus `pytest` for `experiments/`. (b) Property tests for `ExactDecimal` parsing and Lighter book apply/delete invariants. (c) Fuzzing the adapters' `on_text` entry points (no panic, invalid input never corrupts a book) |
| 9 | Observability, auditability, operational excellence | K2, R1 | K2 duties; R1 duties; KB2 telemetry | Operations | Yes | **Partial**: Capture Run quality counters (`invalid_messages`, dedup, disconnects) and SHA-256 manifests (`recorder/src/manifest.rs`). **Missing**: `metrics` crate stub, `observability/` empty | Fill `metrics` with counters/histograms written off the hot path (bounded handoff, like the recorder), plus one dashboard or report. Exporter choice (e.g. Prometheus) is a dependency decision (AGENTS.md §15) |
| 10 | Troubleshoot production issues across connectivity and market data | R | R1 duties | Operations | Partial | **Have (evidence)**: `experiments/reports/2026-10-01-binance-book-quality.md` is a root-cause investigation into Binance `depth10@100ms` cadence | Keep writing reports like this. They are strong evidence. A short incident-style write-up of each recorded disconnect or gap would add more |
| 11 | Security-first design | K | K1–K3 req | Security | Partial | **Partial**: rustls TLS; no credentials yet (public data only, `docs/research/lighter-order-book-protocol.md`); non-root container UID 10001 (`Taskfile.yaml`) | When execution arrives, a documented credential-handling boundary outside strategy (AGENTS.md §11). Until then, `cargo audit` or `cargo deny` in CI (needs §15 approval) |
| 12 | Distributed systems: RPC, Kafka, event-driven | K | K1–K3 req; KB2 | Architecture | Partial | **Partial**: event-driven core with an append-only log and gap-free sequence (ADR 0008). No RPC or Kafka | **Conflicts with AGENTS.md §9**, which excludes Kafka without a demonstrated need. The replay-log design already shows the event-sourcing concepts. Do not add Kafka or gRPC to signal fit |
| 13 | Python for strategy and tooling | R | R1 req | Tooling | Yes | **Have**: `experiments/simulator/`, `experiments/analysis/lead_lag.py`, `experiments/tests/` | Already shown |
| 14 | Articulate architectural trade-offs | R | R1 req; K5 *(signal)* | Communication | Yes | **Have**: ten ADRs (`docs/adr/`), a design doc (`docs/design/`), protocol research (`docs/research/`) | Already shown. ADRs for rows 3–5 decisions would extend it |
| 15 | FIX protocol | R; K (KB3) | R1 req; KB3 | Protocols | Partial | **Missing**. The active venues expose WebSocket/REST | Only possible against a venue that offers FIX. None of the active venues are known to here (not researched). Low priority |
| 16 | Aeron / multicast market-data distribution | K | K4 *(signal)*; KB3 | Transport | Partial | **Missing** | Out of scope for a single-process collector. Mention as a learning topic, not a repository task |
| 17 | Agentic engineering / AI-assisted workflow | R; K4 *(signal)* | R1 req; K4 duties | Tooling | Partial | **Have (process)**: `AGENTS.md` governs agent-assisted work in this repo | Already shown by the repository's documented agent workflow |
| 18 | PostgreSQL; gRPC | R | R2 *(signal)* "preferred" | Storage / RPC | Partial | **Missing** by design (AGENTS.md §9) | Not recommended. It conflicts with the storage invariants and comes only from a lead-role posting |

## 5. Person-level requirements (not demonstrable by this project)

| Requirement | Firm(s) | Source |
|---|---|---|
| 5+ / 7+ years of software engineering experience | K | K1 (7+), K2 (7+), K3 (5+) |
| 7+ years in systematic trading, options MM or low-latency systems | R | R2 *(signal)* |
| Production experience building financial trading products | K | K1 req |
| Hands-on experience in a systematic trading environment (production) | R | R1 req |
| Production-grade Rust (a portfolio project is supporting evidence, not equivalent) | R | R1 req |
| Strong AWS expertise; cloud trade-offs for latency-sensitive workloads | R | R1 req. The project runs on one European VPS (`PROJECT_SUMMARY.md`) |
| Kernel bypass (DPDK), dedicated network infrastructure | R | R1 req. Needs NIC/hardware access a VPS cannot provide |
| FPGA | R | R2 *(signal)* "strongly preferred" |
| Proficiency in C++ (alternative to Rust) | K, R | K4 *(signal)*, R2 *(signal)* |
| Mentoring, technical leadership, code review of others | K, R | K1–K3; R1 |
| Team management | K, R | K4, R2 *(signal)* |
| BS in a technical or quantitative field | K | K4 *(signal)* |
| Production on-call / operating live markets without disruption | K, R | K5 *(signal)*; R1 duties |
| European timezone / location | R | R1, R2 |
| Background check | R | R1, R2 |

## 6. Highest-leverage candidates (ranked; decisions belong to the owner)

These candidates are ranked by how many requirements each touches across
both firms, weighted toward the direct IC postings (K1, R1). Each fits the
AGENTS.md invariants. None is a plan.

1. **Measured latency with tail percentiles** (row 3). Kraken describes
   performance in p99/p99.9/jitter terms (KB1, KB3, K5). R1 asks for
   low-latency systems and latency path analysis. The repository already
   captures the timestamps but reports no receive→processing distribution and
   has no benchmark. This is the smallest step that turns "low latency" from
   a claim into evidence. Open question for the owner: an offline report from
   existing captures, an in-process histogram, or both. The in-process option
   touches the hot path and the `metrics` crate's design.
2. **CI with fmt, clippy and tests** (row 8). It is cheap and both firms
   value it (K1/K2 "testable"; R1 testing duties; KB1 Clippy). It also makes
   every later claim checkable by a reviewer.
3. **Rust strategy and execution simulation driven by deterministic replay**
   (rows 5–7). This covers R1's "execution, market data, and operational
   risk", R2's quoting and risk-control focus, and K1/K4's order-type
   semantics. It is also the next gate in AGENTS.md §12. It needs explicit
   decisions on execution timestamps, fill model and risk policy, which
   ARCHITECTURE.md lists as open.
4. **Property and fuzz tests for decoders and the Lighter book** (row 8).
   These show correctness discipline on the parts both firms rely on most
   (market-data handling). Dependencies need §15 justification.
5. **Observability in the `metrics` crate and `observability/` directory**
   (row 9). K2 and R1 name observability explicitly. Lower than items 1–4
   because it overlaps with item 1 and needs an exporter/dependency decision.

Deliberately not ranked: Kafka, gRPC, PostgreSQL, Aeron, FIX, DPDK and a
standalone matching engine. They conflict with AGENTS.md §9 or §1, need
infrastructure a VPS cannot provide, or come only from manager-level
postings.

## 7. Uncertainty

- Postings change. The text above is as fetched on 2026-10-02. Kraken's live
  board had no dedicated HFT or C++ trading IC role that day, and R2 dates
  from April 2026.
- KB3 is from 2023, and its latency figures may no longer describe Kraken's
  current stack.
- No first-party Keyrock engineering blog was found. Keyrock's technical
  values here rest on postings and one third-party-hosted interview.
- The test count (about 90) is a grep of `#[test]` attributes, not a
  `cargo test` run.
- Whether any active venue offers FIX was not researched.
