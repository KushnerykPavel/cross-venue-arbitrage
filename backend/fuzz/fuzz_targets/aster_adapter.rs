#![no_main]

mod support;

use libfuzzer_sys::fuzz_target;
use market_data::LocalObservationTime;
use venue::{MarketDataAdapter, ObservationClock};
use venue_aster::AsterAdapter;

struct Clock;

impl ObservationClock for Clock {
    fn now(&self) -> LocalObservationTime {
        LocalObservationTime::from_nanos_since_start(1)
    }
}

const SNAPSHOT_SEED: &str = include_str!("../corpus/aster_adapter/depth_snapshot");

fuzz_target!(|data: &[u8]| {
    let input = String::from_utf8_lossy(data);
    let mut adapter = AsterAdapter::dev_fixture();
    let clock = Clock;
    let now = LocalObservationTime::from_nanos_since_start(1);
    let seeded = adapter.on_text(SNAPSHOT_SEED, now, &clock);
    support::assert_valid_published_books(&seeded);
    for frame in input.split('\0') {
        let actions = adapter.on_text(frame, now, &clock);
        support::assert_valid_published_books(&actions);
    }
});
