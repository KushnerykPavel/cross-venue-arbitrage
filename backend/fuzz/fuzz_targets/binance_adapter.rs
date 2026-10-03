#![no_main]

mod support;

use libfuzzer_sys::fuzz_target;
use market_data::LocalObservationTime;
use venue::{MarketDataAdapter, ObservationClock};
use venue_binance::BinanceAdapters;

struct Clock;

impl ObservationClock for Clock {
    fn now(&self) -> LocalObservationTime {
        LocalObservationTime::from_nanos_since_start(1)
    }
}

const DEPTH_SEED: &str = include_str!("../corpus/binance_adapter/depth_snapshot");

fuzz_target!(|data: &[u8]| {
    let input = String::from_utf8_lossy(data);
    let mut adapters = BinanceAdapters::dev_fixture();
    let clock = Clock;
    let now = LocalObservationTime::from_nanos_since_start(1);
    let seeded = adapters.depth.on_text(DEPTH_SEED, now, &clock);
    support::assert_valid_published_books(&seeded);
    for frame in input.split('\0') {
        let depth_actions = adapters.depth.on_text(frame, now, &clock);
        support::assert_valid_published_books(&depth_actions);
        let trade_actions = adapters.trades.on_text(frame, now, &clock);
        support::assert_valid_published_books(&trade_actions);
    }
});
