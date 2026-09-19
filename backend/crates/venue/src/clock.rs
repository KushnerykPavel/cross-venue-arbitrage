use std::time::Instant;

use market_data::LocalObservationTime;

pub trait ObservationClock {
    fn now(&self) -> LocalObservationTime;
}

#[derive(Clone, Debug)]
pub struct MonotonicClock {
    origin: Instant,
}

impl MonotonicClock {
    pub fn start() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl ObservationClock for MonotonicClock {
    fn now(&self) -> LocalObservationTime {
        LocalObservationTime::from_nanos_since_start(
            self.origin
                .elapsed()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX),
        )
    }
}
