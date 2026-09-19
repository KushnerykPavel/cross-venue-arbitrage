use std::cmp::Reverse;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroU32;

use domain::{Price, Quantity, Symbol, Venue};

use crate::{EventTimestamps, LocalObservationTime};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BookLevel {
    price: Price,
    quantity: Quantity,
    order_count: Option<NonZeroU32>,
}

impl BookLevel {
    pub const fn new(price: Price, quantity: Quantity, order_count: Option<NonZeroU32>) -> Self {
        Self {
            price,
            quantity,
            order_count,
        }
    }

    pub const fn price(self) -> Price {
        self.price
    }

    pub const fn quantity(self) -> Quantity {
        self.quantity
    }

    pub const fn order_count(self) -> Option<NonZeroU32> {
        self.order_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBookSnapshot {
    venue: Venue,
    symbol: Symbol,
    source_sequence: Option<u64>,
    timestamps: EventTimestamps,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotValidationError {
    EmptyBids,
    EmptyAsks,
    DuplicateBid(Price),
    DuplicateAsk(Price),
    LockedOrCrossed { best_bid: Price, best_ask: Price },
}

impl OrderBookSnapshot {
    pub fn try_new(
        venue: Venue,
        symbol: Symbol,
        source_sequence: Option<u64>,
        timestamps: EventTimestamps,
        mut bids: Vec<BookLevel>,
        mut asks: Vec<BookLevel>,
    ) -> Result<Self, SnapshotValidationError> {
        if bids.is_empty() {
            return Err(SnapshotValidationError::EmptyBids);
        }
        if asks.is_empty() {
            return Err(SnapshotValidationError::EmptyAsks);
        }

        bids.sort_unstable_by_key(|level| Reverse(level.price));
        asks.sort_unstable_by_key(|level| level.price);

        if let Some(price) = duplicate_price(&bids) {
            return Err(SnapshotValidationError::DuplicateBid(price));
        }
        if let Some(price) = duplicate_price(&asks) {
            return Err(SnapshotValidationError::DuplicateAsk(price));
        }

        let best_bid = bids[0].price;
        let best_ask = asks[0].price;
        if best_bid >= best_ask {
            return Err(SnapshotValidationError::LockedOrCrossed { best_bid, best_ask });
        }

        Ok(Self {
            venue,
            symbol,
            source_sequence,
            timestamps,
            bids,
            asks,
        })
    }

    pub const fn venue(&self) -> Venue {
        self.venue
    }

    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }

    pub const fn timestamps(&self) -> &EventTimestamps {
        &self.timestamps
    }

    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderBookStatus {
    AwaitingSnapshot,
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBookIdentityError {
    expected_venue: Venue,
    actual_venue: Venue,
    expected_symbol: Symbol,
    actual_symbol: Symbol,
}

#[derive(Debug)]
pub struct OrderBook {
    venue: Venue,
    symbol: Symbol,
    status: OrderBookStatus,
    last_snapshot: Option<OrderBookSnapshot>,
}

impl OrderBook {
    pub fn new(venue: Venue, symbol: Symbol) -> Self {
        Self {
            venue,
            symbol,
            status: OrderBookStatus::AwaitingSnapshot,
            last_snapshot: None,
        }
    }

    pub fn replace(&mut self, snapshot: OrderBookSnapshot) -> Result<(), OrderBookIdentityError> {
        self.replace_inner(snapshot, None::<fn() -> LocalObservationTime>)
    }

    pub fn replace_at_processing_completion<F>(
        &mut self,
        snapshot: OrderBookSnapshot,
        completed: F,
    ) -> Result<(), OrderBookIdentityError>
    where
        F: FnOnce() -> LocalObservationTime,
    {
        self.replace_inner(snapshot, Some(completed))
    }

    fn replace_inner<F>(
        &mut self,
        snapshot: OrderBookSnapshot,
        completed: Option<F>,
    ) -> Result<(), OrderBookIdentityError>
    where
        F: FnOnce() -> LocalObservationTime,
    {
        if snapshot.venue != self.venue || snapshot.symbol != self.symbol {
            self.status = OrderBookStatus::Unhealthy;
            return Err(OrderBookIdentityError {
                expected_venue: self.venue,
                actual_venue: snapshot.venue,
                expected_symbol: self.symbol.clone(),
                actual_symbol: snapshot.symbol.clone(),
            });
        }

        self.last_snapshot = Some(snapshot);
        if let Some(completed) = completed {
            self.last_snapshot
                .as_mut()
                .expect("snapshot was just installed")
                .timestamps
                .set_processing_completed(completed());
        }
        self.status = OrderBookStatus::Healthy;
        Ok(())
    }

    pub fn mark_unhealthy(&mut self) {
        self.status = OrderBookStatus::Unhealthy;
    }

    pub const fn status(&self) -> OrderBookStatus {
        self.status
    }

    pub fn current(&self) -> Option<&OrderBookSnapshot> {
        (self.status == OrderBookStatus::Healthy)
            .then_some(self.last_snapshot.as_ref())
            .flatten()
    }

    pub fn last_snapshot(&self) -> Option<&OrderBookSnapshot> {
        self.last_snapshot.as_ref()
    }
}

impl Display for SnapshotValidationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBids => formatter.write_str("snapshot has no bids"),
            Self::EmptyAsks => formatter.write_str("snapshot has no asks"),
            Self::DuplicateBid(price) => write!(formatter, "duplicate bid price {price}"),
            Self::DuplicateAsk(price) => write!(formatter, "duplicate ask price {price}"),
            Self::LockedOrCrossed { best_bid, best_ask } => write!(
                formatter,
                "snapshot is locked or crossed: best bid {best_bid}, best ask {best_ask}"
            ),
        }
    }
}

impl Error for SnapshotValidationError {}

impl Display for OrderBookIdentityError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "snapshot identity mismatch: expected {:?}/{:?}, got {:?}/{:?}",
            self.expected_venue, self.expected_symbol, self.actual_venue, self.actual_symbol
        )
    }
}

impl Error for OrderBookIdentityError {}

fn duplicate_price(levels: &[BookLevel]) -> Option<Price> {
    levels
        .windows(2)
        .find(|pair| pair[0].price == pair[1].price)
        .map(|pair| pair[0].price)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::str::FromStr;

    use domain::{MarketCoin, Price, Quantity, Symbol, Venue};

    use super::{
        BookLevel, OrderBook, OrderBookSnapshot, OrderBookStatus, SnapshotValidationError,
    };
    use crate::{
        EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit,
        LocalObservationTime,
    };

    fn level(price: &str) -> BookLevel {
        BookLevel::new(
            Price::from_str(price).unwrap(),
            Quantity::from_str("1").unwrap(),
            NonZeroU32::new(1),
        )
    }

    fn snapshot(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> OrderBookSnapshot {
        OrderBookSnapshot::try_new(
            Venue::Hyperliquid,
            btc_perpetual(),
            None,
            timestamps(),
            bids,
            asks,
        )
        .unwrap()
    }

    fn btc_perpetual() -> Symbol {
        Symbol::perpetual(MarketCoin::try_new("BTC").unwrap())
    }

    fn timestamps() -> EventTimestamps {
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                1,
                ExchangeTimeUnit::Unknown,
            )],
            LocalObservationTime::from_nanos_since_start(2),
            LocalObservationTime::from_nanos_since_start(3),
        )
    }

    #[test]
    fn sorts_bids_and_asks_best_first() {
        let snapshot = snapshot(
            vec![level("99"), level("100")],
            vec![level("102"), level("101")],
        );

        assert_eq!(snapshot.bids()[0].price().to_string(), "100");
        assert_eq!(snapshot.asks()[0].price().to_string(), "101");
    }

    #[test]
    fn rejects_duplicate_and_crossed_prices() {
        let duplicate = OrderBookSnapshot::try_new(
            Venue::Hyperliquid,
            btc_perpetual(),
            None,
            timestamps(),
            vec![level("100"), level("100.0")],
            vec![level("101")],
        );
        assert!(matches!(
            duplicate,
            Err(SnapshotValidationError::DuplicateBid(_))
        ));

        let crossed = OrderBookSnapshot::try_new(
            Venue::Hyperliquid,
            btc_perpetual(),
            None,
            timestamps(),
            vec![level("101")],
            vec![level("100")],
        );
        assert!(matches!(
            crossed,
            Err(SnapshotValidationError::LockedOrCrossed { .. })
        ));
    }

    #[test]
    fn unhealthy_book_does_not_expose_stale_snapshot_as_current() {
        let mut book = OrderBook::new(Venue::Hyperliquid, btc_perpetual());
        book.replace(snapshot(vec![level("100")], vec![level("101")]))
            .unwrap();
        assert!(book.current().is_some());

        book.mark_unhealthy();

        assert_eq!(book.status(), OrderBookStatus::Unhealthy);
        assert!(book.current().is_none());
        assert!(book.last_snapshot().is_some());
    }
}
