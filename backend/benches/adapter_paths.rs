use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use criterion::{BatchSize, Criterion};
use hdrhistogram::Histogram;
use market_data::LocalObservationTime;
use venue::{MarketDataAdapter, ObservationClock};
use venue_aster::AsterAdapter;
use venue_binance::BinanceAdapters;
use venue_lighter::LighterAdapter;

const WARMUP: u64 = 10_000;
const SAMPLES: u64 = 100_000;
const ALLOCATION_SAMPLES: u64 = 10_000;
const BINANCE_DEPTH: &str = include_str!("fixtures/binance_depth.json");
const BINANCE_TRADE: &str = include_str!("fixtures/binance_aggtrade.json");
const ASTER_DEPTH: &str = include_str!("fixtures/aster_depth.json");
const LIGHTER_SNAPSHOT: &str = include_str!("fixtures/lighter_snapshot.json");
const LIGHTER_UPDATE: &str = include_str!("fixtures/lighter_update.json");
const LIGHTER_DELETE: &str = include_str!("fixtures/lighter_delete.json");

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: delegated unchanged to the system allocator.
        let pointer = unsafe { System.alloc(layout) };
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: delegated unchanged to the system allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: delegated unchanged to the system allocator.
        let result = unsafe { System.realloc(pointer, layout, new_size) };
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct Clock(Cell<u64>);

impl ObservationClock for Clock {
    fn now(&self) -> LocalObservationTime {
        let next = self.0.get().wrapping_add(1);
        self.0.set(next);
        LocalObservationTime::from_nanos_since_start(next)
    }
}

fn received(value: u64) -> LocalObservationTime {
    LocalObservationTime::from_nanos_since_start(value)
}

fn criterion_benchmarks(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("decode_and_book_update");
    group.bench_function("binance_depth10", |bencher| {
        let mut adapters = BinanceAdapters::dev_fixture();
        let clock = Clock(Cell::new(1));
        bencher.iter(|| {
            black_box(
                adapters
                    .depth
                    .on_text(black_box(BINANCE_DEPTH), received(1), &clock),
            );
        });
    });
    group.bench_function("binance_aggtrade_decode", |bencher| {
        let mut next_id = 1_000_u64;
        let clock = Clock(Cell::new(1));
        bencher.iter_batched(
            || {
                let adapters = BinanceAdapters::dev_fixture();
                next_id += 1;
                let frame = BINANCE_TRADE.replace("\"a\":91", &format!("\"a\":{next_id}"));
                (adapters.trades, frame)
            },
            |(mut adapter, frame)| {
                black_box(adapter.on_text(black_box(&frame), received(1), &clock))
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("aster_depth10", |bencher| {
        let mut adapter = AsterAdapter::dev_fixture();
        let clock = Clock(Cell::new(1));
        bencher.iter(|| black_box(adapter.on_text(black_box(ASTER_DEPTH), received(1), &clock)));
    });
    group.bench_function("lighter_incremental_update", |bencher| {
        let clock = Clock(Cell::new(1));
        bencher.iter_batched(
            || {
                let mut adapter = LighterAdapter::dev_fixture();
                adapter.on_text(LIGHTER_SNAPSHOT, received(1), &clock);
                adapter
            },
            |mut adapter| {
                black_box(adapter.on_text(black_box(LIGHTER_UPDATE), received(2), &clock))
            },
            BatchSize::SmallInput,
        );
    });
    group.bench_function("lighter_level_delete", |bencher| {
        let clock = Clock(Cell::new(1));
        bencher.iter_batched(
            || {
                let mut adapter = LighterAdapter::dev_fixture();
                adapter.on_text(LIGHTER_SNAPSHOT, received(1), &clock);
                adapter
            },
            |mut adapter| {
                black_box(adapter.on_text(black_box(LIGHTER_DELETE), received(2), &clock))
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn print_histogram<F>(name: &str, mut operation: F)
where
    F: FnMut(u64),
{
    for index in 0..WARMUP {
        operation(index);
    }
    let mut histogram = Histogram::<u64>::new(3).expect("valid histogram precision");
    for index in 0..SAMPLES {
        let start = Instant::now();
        operation(index + WARMUP);
        let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        histogram
            .record(nanos.max(1))
            .expect("benchmark duration fits histogram");
    }
    println!(
        "latency {name}: n={} p50={}ns p95={}ns p99={}ns p99.9={}ns max={}ns",
        histogram.len(),
        histogram.value_at_quantile(0.50),
        histogram.value_at_quantile(0.95),
        histogram.value_at_quantile(0.99),
        histogram.value_at_quantile(0.999),
        histogram.max(),
    );
}

fn print_allocations<F>(name: &str, mut operation: F)
where
    F: FnMut(u64),
{
    for index in 0..WARMUP {
        operation(index);
    }
    ALLOCATIONS.store(0, Ordering::Relaxed);
    for index in 0..ALLOCATION_SAMPLES {
        COUNTING.store(true, Ordering::SeqCst);
        operation(index + WARMUP);
        COUNTING.store(false, Ordering::SeqCst);
    }
    let allocations = ALLOCATIONS.load(Ordering::Relaxed);
    println!(
        "allocations {name}: n={ALLOCATION_SAMPLES} total={allocations} per_message={:.6}",
        allocations as f64 / ALLOCATION_SAMPLES as f64
    );
}

fn supplemental_measurements() {
    let clock = Clock(Cell::new(1));
    let mut binance = BinanceAdapters::dev_fixture();
    print_histogram("binance_depth10", |index| {
        black_box(
            binance
                .depth
                .on_text(BINANCE_DEPTH, received(index), &clock),
        );
    });
    let mut binance = BinanceAdapters::dev_fixture();
    print_allocations("binance_depth10", |index| {
        black_box(
            binance
                .depth
                .on_text(BINANCE_DEPTH, received(index), &clock),
        );
    });

    let trade_frames = (0..(WARMUP + SAMPLES + ALLOCATION_SAMPLES))
        .map(|index| BINANCE_TRADE.replace("\"a\":91", &format!("\"a\":{}", index + 10_000)))
        .collect::<Vec<_>>();
    let mut binance = BinanceAdapters::dev_fixture();
    print_histogram("binance_aggtrade_decode", |index| {
        black_box(
            binance
                .trades
                .on_text(&trade_frames[index as usize], received(index), &clock),
        );
    });
    let mut binance = BinanceAdapters::dev_fixture();
    print_allocations("binance_aggtrade_decode", |index| {
        black_box(
            binance
                .trades
                .on_text(&trade_frames[index as usize], received(index), &clock),
        );
    });

    let mut aster = AsterAdapter::dev_fixture();
    print_histogram("aster_depth10", |index| {
        black_box(aster.on_text(ASTER_DEPTH, received(index), &clock));
    });
    let mut aster = AsterAdapter::dev_fixture();
    print_allocations("aster_depth10", |index| {
        black_box(aster.on_text(ASTER_DEPTH, received(index), &clock));
    });

    let lighter_updates = (0..(WARMUP + SAMPLES))
        .map(|index| {
            let fixture = if index % 2 == 0 {
                LIGHTER_UPDATE
            } else {
                LIGHTER_DELETE
            };
            fixture
                .replace("\"nonce\":11", &format!("\"nonce\":{}", index + 11))
                .replace(
                    "\"begin_nonce\":10",
                    &format!("\"begin_nonce\":{}", index + 10),
                )
        })
        .collect::<Vec<_>>();
    let mut lighter = LighterAdapter::dev_fixture();
    lighter.on_text(LIGHTER_SNAPSHOT, received(1), &clock);
    print_histogram("lighter_incremental_update_delete", |index| {
        black_box(lighter.on_text(
            &lighter_updates[index as usize],
            received(index + 2),
            &clock,
        ));
    });
    let mut lighter = LighterAdapter::dev_fixture();
    lighter.on_text(LIGHTER_SNAPSHOT, received(1), &clock);
    print_allocations("lighter_incremental_update_delete", |index| {
        black_box(lighter.on_text(
            &lighter_updates[index as usize],
            received(index + 2),
            &clock,
        ));
    });
}

fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn print_environment() {
    let host = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| command_output("hostname", &[]))
        .unwrap_or_else(|| "unknown".into());
    let rustc = command_output("rustc", &["--version"]).unwrap_or_else(|| "unknown".into());
    let commit = command_output("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty =
        command_output("git", &["status", "--porcelain"]).is_some_and(|status| !status.is_empty());
    println!("benchmark environment: host={host} rustc={rustc} commit={commit} dirty={dirty}");
    println!(
        "benchmark fixtures: binance_depth.json binance_aggtrade.json aster_depth.json lighter_snapshot.json lighter_update.json lighter_delete.json"
    );
}

fn main() {
    print_environment();
    let mut criterion = Criterion::default().configure_from_args();
    criterion_benchmarks(&mut criterion);
    criterion.final_summary();
    supplemental_measurements();
}
