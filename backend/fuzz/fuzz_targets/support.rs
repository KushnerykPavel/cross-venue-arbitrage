use market_data::{BookLevel, NormalizedMarketEvent};
use venue::AdapterAction;

pub fn assert_valid_published_books(actions: &[AdapterAction]) {
    for action in actions {
        let AdapterAction::Publish(NormalizedMarketEvent::OrderBookSnapshot(snapshot)) = action
        else {
            continue;
        };
        assert!(!snapshot.bids().is_empty());
        assert!(!snapshot.asks().is_empty());
        assert!(
            snapshot
                .bids()
                .windows(2)
                .all(|levels| levels[0].price() > levels[1].price())
        );
        assert!(
            snapshot
                .asks()
                .windows(2)
                .all(|levels| levels[0].price() < levels[1].price())
        );
        assert!(
            snapshot
                .bids()
                .iter()
                .chain(snapshot.asks())
                .all(|level: &BookLevel| level.quantity().coefficient() > 0)
        );
        assert!(snapshot.bids()[0].price() < snapshot.asks()[0].price());
    }
}
