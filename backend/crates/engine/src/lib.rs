use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use domain::{MarketCoin, Venue};
use market_data::{
    AggressorSide, AggressorSideClassification, BestBidOffer, ExchangeTimeKind, ExchangeTimeUnit,
    MarketTradeIdentity, MarketTradeKind, MarketTradeReportingKind, NormalizedMarketEvent,
    OrderBook, OrderBookSnapshot, OrderBookStatus, UnavailabilityCategory,
};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct MarketDataEngine {
    next_capture_sequence: u64,
    events_processed: u64,
    digest: Sha256,
    order_books: HashMap<(Venue, MarketCoin), OrderBook>,
    best_bid_offers: HashMap<(Venue, MarketCoin), BestBidOfferState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BestBidOfferStatus {
    AwaitingFirstUpdate,
    Available,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BestBidOfferReport {
    pub venue: Venue,
    pub market_coin: MarketCoin,
    pub status: BestBidOfferStatus,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
}

#[derive(Clone, Debug)]
struct BestBidOfferState {
    value: Option<BestBidOffer>,
    status: BestBidOfferStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineReport {
    pub events_processed: u64,
    pub last_capture_sequence: Option<u64>,
    pub event_digest_sha256: String,
    pub order_books: Vec<OrderBookReport>,
    pub best_bid_offers: Vec<BestBidOfferReport>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBookReport {
    pub venue: Venue,
    pub market_coin: MarketCoin,
    pub status: OrderBookStatus,
    pub source_sequence: Option<u64>,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineError {
    CaptureSequenceMismatch { expected: u64, actual: u64 },
    CaptureSequenceOverflow,
}

impl Default for MarketDataEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl MarketDataEngine {
    pub fn new() -> Self {
        Self {
            next_capture_sequence: 1,
            events_processed: 0,
            digest: Sha256::new(),
            order_books: HashMap::new(),
            best_bid_offers: HashMap::new(),
        }
    }

    pub fn process(
        &mut self,
        capture_sequence: u64,
        event: &NormalizedMarketEvent,
    ) -> Result<(), EngineError> {
        if capture_sequence != self.next_capture_sequence {
            return Err(EngineError::CaptureSequenceMismatch {
                expected: self.next_capture_sequence,
                actual: capture_sequence,
            });
        }
        hash_event(&mut self.digest, capture_sequence, event);
        match event {
            NormalizedMarketEvent::OrderBookSnapshot(snapshot) => {
                let key = (snapshot.venue(), snapshot.symbol().market_coin().clone());
                self.order_books
                    .entry(key)
                    .or_insert_with(|| OrderBook::new(snapshot.venue(), snapshot.symbol().clone()))
                    .replace(snapshot.clone())
                    .expect("Order Book key is derived from the snapshot identity");
                self.reconcile_bbo(snapshot.venue(), snapshot.symbol().market_coin());
            }
            NormalizedMarketEvent::BestBidOfferUpdated(bbo) => {
                let key = (bbo.venue(), bbo.symbol().market_coin().clone());
                let status = self.bbo_status(bbo);
                self.best_bid_offers.insert(
                    key,
                    BestBidOfferState {
                        value: Some(bbo.clone()),
                        status,
                    },
                );
            }
            NormalizedMarketEvent::OrderBookUnavailable(unavailable) => {
                let key = (unavailable.venue(), unavailable.market_coin().clone());
                self.order_books
                    .entry(key)
                    .or_insert_with(|| {
                        OrderBook::new(
                            unavailable.venue(),
                            domain::Symbol::perpetual(unavailable.market_coin().clone()),
                        )
                    })
                    .mark_unhealthy();
            }
            NormalizedMarketEvent::MarketTrade(_)
            | NormalizedMarketEvent::TradeStreamUnavailable(_)
            | NormalizedMarketEvent::TradeStreamResumed(_) => {}
        }
        self.events_processed = self.events_processed.saturating_add(1);
        self.next_capture_sequence = capture_sequence
            .checked_add(1)
            .ok_or(EngineError::CaptureSequenceOverflow)?;
        Ok(())
    }

    pub fn report(&self) -> EngineReport {
        let mut order_books = self
            .order_books
            .iter()
            .map(|((venue, market_coin), book)| {
                let snapshot = book.last_snapshot();
                OrderBookReport {
                    venue: *venue,
                    market_coin: market_coin.clone(),
                    status: book.status(),
                    source_sequence: snapshot.and_then(OrderBookSnapshot::source_sequence),
                    best_bid: snapshot.map(|value| value.bids()[0].price().to_string()),
                    best_ask: snapshot.map(|value| value.asks()[0].price().to_string()),
                }
            })
            .collect::<Vec<_>>();
        order_books.sort_by(|left, right| {
            venue_rank(left.venue)
                .cmp(&venue_rank(right.venue))
                .then_with(|| left.market_coin.cmp(&right.market_coin))
        });
        let mut best_bid_offers = self
            .best_bid_offers
            .iter()
            .map(|((venue, market_coin), state)| BestBidOfferReport {
                venue: *venue,
                market_coin: market_coin.clone(),
                status: state.status,
                best_bid: state
                    .value
                    .as_ref()
                    .and_then(|bbo| bbo.bid().map(|level| level.price().to_string())),
                best_ask: state
                    .value
                    .as_ref()
                    .and_then(|bbo| bbo.ask().map(|level| level.price().to_string())),
            })
            .collect::<Vec<_>>();
        best_bid_offers.sort_by(|left, right| {
            venue_rank(left.venue)
                .cmp(&venue_rank(right.venue))
                .then_with(|| left.market_coin.cmp(&right.market_coin))
        });
        EngineReport {
            events_processed: self.events_processed,
            last_capture_sequence: self
                .next_capture_sequence
                .checked_sub(1)
                .filter(|_| self.events_processed > 0),
            event_digest_sha256: encode_hex(&self.digest.clone().finalize()),
            order_books,
            best_bid_offers,
        }
    }

    fn bbo_status(&self, bbo: &BestBidOffer) -> BestBidOfferStatus {
        let Some(book) = self
            .order_books
            .get(&(bbo.venue(), bbo.symbol().market_coin().clone()))
        else {
            return BestBidOfferStatus::Unknown;
        };
        let Some(snapshot) = book.current() else {
            return BestBidOfferStatus::Unknown;
        };
        if snapshot
            .timestamps()
            .local_receive()
            .nanos_since_start()
            .abs_diff(bbo.timestamps().local_receive().nanos_since_start())
            > 500_000_000
        {
            return BestBidOfferStatus::Unknown;
        }
        let bid_matches = bbo
            .bid()
            .zip(snapshot.bids().first())
            .map_or(false, |(a, b)| a == b);
        let ask_matches = bbo
            .ask()
            .zip(snapshot.asks().first())
            .map_or(false, |(a, b)| a == b);
        if bid_matches && ask_matches {
            BestBidOfferStatus::Available
        } else {
            BestBidOfferStatus::Unknown
        }
    }

    fn reconcile_bbo(&mut self, venue: Venue, market_coin: &MarketCoin) {
        let key = (venue, market_coin.clone());
        let status = self
            .best_bid_offers
            .get(&key)
            .and_then(|state| state.value.as_ref())
            .map_or(BestBidOfferStatus::Unknown, |bbo| self.bbo_status(bbo));
        let Some(state) = self.best_bid_offers.get_mut(&key) else {
            return;
        };
        state.status = status;
    }
}

fn hash_event(hasher: &mut Sha256, sequence: u64, event: &NormalizedMarketEvent) {
    hash_u64(hasher, sequence);
    hash_u8(hasher, venue_rank(event.venue()));
    hash_str(hasher, event.market_coin().as_str());
    match event {
        NormalizedMarketEvent::OrderBookSnapshot(snapshot) => {
            hash_u8(hasher, 0);
            hash_timestamps(hasher, snapshot.timestamps());
            hash_optional_u64(hasher, snapshot.source_sequence());
            hash_u64(hasher, snapshot.bids().len() as u64);
            for level in snapshot.bids() {
                hash_i128(hasher, level.price().coefficient());
                hash_u8(hasher, level.price().scale());
                hash_i128(hasher, level.quantity().coefficient());
                hash_u8(hasher, level.quantity().scale());
                hash_optional_u64(
                    hasher,
                    level.order_count().map(|value| u64::from(value.get())),
                );
            }
            hash_u64(hasher, snapshot.asks().len() as u64);
            for level in snapshot.asks() {
                hash_i128(hasher, level.price().coefficient());
                hash_u8(hasher, level.price().scale());
                hash_i128(hasher, level.quantity().coefficient());
                hash_u8(hasher, level.quantity().scale());
                hash_optional_u64(
                    hasher,
                    level.order_count().map(|value| u64::from(value.get())),
                );
            }
        }
        NormalizedMarketEvent::BestBidOfferUpdated(bbo) => {
            hash_u8(hasher, 5);
            hash_timestamps(hasher, bbo.timestamps());
            hash_optional_book_level(hasher, bbo.bid());
            hash_optional_book_level(hasher, bbo.ask());
        }
        NormalizedMarketEvent::OrderBookUnavailable(event) => {
            hash_u8(hasher, 1);
            hash_u64(hasher, event.observed_at().nanos_since_start());
            hash_u8(hasher, unavailability_rank(event.category()));
            hash_str(hasher, event.diagnostic());
        }
        NormalizedMarketEvent::MarketTrade(trade) => {
            hash_u8(hasher, 2);
            hash_timestamps(hasher, trade.timestamps());
            hash_i128(hasher, trade.price().coefficient());
            hash_u8(hasher, trade.price().scale());
            hash_i128(hasher, trade.quantity().coefficient());
            hash_u8(hasher, trade.quantity().scale());
            hash_u8(
                hasher,
                match trade.reporting_kind() {
                    MarketTradeReportingKind::Individual => 0,
                    MarketTradeReportingKind::TakerOrderAggregate => 1,
                },
            );
            hash_u8(
                hasher,
                match trade.trade_kind() {
                    MarketTradeKind::Regular => 0,
                    MarketTradeKind::Liquidation => 1,
                    MarketTradeKind::Deleverage => 2,
                    MarketTradeKind::MarketSettlement => 3,
                    MarketTradeKind::Other => 4,
                },
            );
            hash_u8(
                hasher,
                match trade.aggressor_side() {
                    AggressorSide::Buy => 0,
                    AggressorSide::Sell => 1,
                    AggressorSide::Unknown => 2,
                },
            );
            hash_u8(
                hasher,
                match trade.aggressor_side_classification() {
                    AggressorSideClassification::VenueProvided => 0,
                    AggressorSideClassification::DerivedFromMakerSide => 1,
                    AggressorSideClassification::Unknown => 2,
                },
            );
            match trade.identity() {
                MarketTradeIdentity::Aster {
                    aggregate_trade_id,
                    first_trade_id,
                    last_trade_id,
                } => {
                    hash_u8(hasher, 0);
                    hash_u64(hasher, *aggregate_trade_id);
                    hash_u64(hasher, *first_trade_id);
                    hash_u64(hasher, *last_trade_id);
                }
                MarketTradeIdentity::Hyperliquid {
                    block_time,
                    trade_id,
                    transaction_hash,
                } => {
                    hash_u8(hasher, 1);
                    hash_u64(hasher, *block_time);
                    hash_u64(hasher, *trade_id);
                    hash_str(hasher, transaction_hash);
                }
                MarketTradeIdentity::Lighter {
                    market_id,
                    trade_id,
                    message_nonce,
                } => {
                    hash_u8(hasher, 2);
                    hash_u64(hasher, u64::from(*market_id));
                    hash_str(hasher, trade_id);
                    hash_optional_u64(hasher, *message_nonce);
                }
            }
        }
        NormalizedMarketEvent::TradeStreamUnavailable(event) => {
            hash_u8(hasher, 3);
            hash_u64(hasher, event.observed_at().nanos_since_start());
            hash_u8(hasher, unavailability_rank(event.category()));
            hash_str(hasher, event.diagnostic());
        }
        NormalizedMarketEvent::TradeStreamResumed(event) => {
            hash_u8(hasher, 4);
            hash_u64(hasher, event.observed_at().nanos_since_start());
        }
    }
}

fn hash_optional_book_level(hasher: &mut Sha256, level: Option<&market_data::BookLevel>) {
    match level {
        Some(level) => {
            hash_u8(hasher, 1);
            hash_i128(hasher, level.price().coefficient());
            hash_u8(hasher, level.price().scale());
            hash_i128(hasher, level.quantity().coefficient());
            hash_u8(hasher, level.quantity().scale());
            hash_optional_u64(
                hasher,
                level.order_count().map(|count| u64::from(count.get())),
            );
        }
        None => hash_u8(hasher, 0),
    }
}

fn hash_timestamps(hasher: &mut Sha256, timestamps: &market_data::EventTimestamps) {
    hash_u64(hasher, timestamps.local_receive().nanos_since_start());
    hash_u64(
        hasher,
        timestamps.processing_completed().nanos_since_start(),
    );
    hash_u64(hasher, timestamps.exchange_times().len() as u64);
    for observation in timestamps.exchange_times() {
        hash_u8(
            hasher,
            match observation.kind() {
                ExchangeTimeKind::EventTime => 0,
                ExchangeTimeKind::TradeTime => 1,
                ExchangeTimeKind::BlockTime => 2,
                ExchangeTimeKind::TransactionTime => 3,
                ExchangeTimeKind::Other => 4,
            },
        );
        hash_u64(hasher, observation.raw_value());
        hash_u8(
            hasher,
            match observation.declared_unit() {
                ExchangeTimeUnit::Milliseconds => 0,
                ExchangeTimeUnit::Microseconds => 1,
                ExchangeTimeUnit::Nanoseconds => 2,
                ExchangeTimeUnit::Unknown => 3,
            },
        );
    }
}

fn venue_rank(venue: Venue) -> u8 {
    match venue {
        Venue::Aster => 0,
        Venue::Hyperliquid => 1,
        Venue::Lighter => 2,
    }
}

fn unavailability_rank(category: UnavailabilityCategory) -> u8 {
    match category {
        UnavailabilityCategory::Disconnected => 0,
        UnavailabilityCategory::InvalidMarketData => 1,
        UnavailabilityCategory::SequenceGap => 2,
        UnavailabilityCategory::RecorderShutdown => 3,
        UnavailabilityCategory::Other => 4,
    }
}

fn hash_u8(hasher: &mut Sha256, value: u8) {
    hasher.update([value]);
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

fn hash_i128(hasher: &mut Sha256, value: i128) {
    hasher.update(value.to_le_bytes());
}

fn hash_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hash_u8(hasher, 1);
            hash_u64(hasher, value);
        }
        None => hash_u8(hasher, 0),
    }
}

fn hash_str(hasher: &mut Sha256, value: &str) {
    hash_u64(hasher, value.len() as u64);
    hasher.update(value.as_bytes());
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

impl Display for EngineError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CaptureSequenceMismatch { expected, actual } => write!(
                formatter,
                "non-contiguous Capture Sequence: expected {expected}, received {actual}"
            ),
            Self::CaptureSequenceOverflow => formatter.write_str("Capture Sequence overflow"),
        }
    }
}

impl Error for EngineError {}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use domain::{MarketCoin, Price, Quantity, Symbol};
    use market_data::{
        BookLevel, EventTimestamps, LocalObservationTime, OrderBookSnapshot, TradeStreamResumed,
    };

    use super::*;

    fn resumed(nanos: u64) -> NormalizedMarketEvent {
        NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
            Venue::Aster,
            MarketCoin::try_new("BTC").unwrap(),
            LocalObservationTime::from_nanos_since_start(nanos),
        ))
    }

    #[test]
    fn identical_inputs_produce_identical_digest() {
        let mut first = MarketDataEngine::new();
        let mut second = MarketDataEngine::new();
        for sequence in 1..=2 {
            let event = resumed(sequence);
            first.process(sequence, &event).unwrap();
            second.process(sequence, &event).unwrap();
        }

        assert_eq!(first.report(), second.report());
        assert_eq!(first.report().events_processed, 2);
    }

    #[test]
    fn rejects_non_contiguous_capture_sequence() {
        let mut engine = MarketDataEngine::new();
        let error = engine.process(2, &resumed(1)).unwrap_err();

        assert_eq!(
            error,
            EngineError::CaptureSequenceMismatch {
                expected: 1,
                actual: 2
            }
        );
        assert_eq!(engine.report().events_processed, 0);
    }

    fn bbo_fixture(receive_time: u64, bid_price: &str) -> BestBidOffer {
        let coin = MarketCoin::try_new("BTC").unwrap();
        BestBidOffer::new(
            Venue::Hyperliquid,
            Symbol::perpetual(coin),
            EventTimestamps::new(
                Vec::new(),
                LocalObservationTime::from_nanos_since_start(receive_time),
                LocalObservationTime::from_nanos_since_start(receive_time),
            ),
            Some(BookLevel::new(
                Price::from_str(bid_price).unwrap(),
                Quantity::from_str("1").unwrap(),
                None,
            )),
            Some(BookLevel::new(
                Price::from_str("101").unwrap(),
                Quantity::from_str("2").unwrap(),
                None,
            )),
        )
    }

    fn snapshot_fixture(receive_time: u64) -> OrderBookSnapshot {
        let coin = MarketCoin::try_new("BTC").unwrap();
        OrderBookSnapshot::try_new(
            Venue::Hyperliquid,
            Symbol::perpetual(coin),
            None,
            EventTimestamps::new(
                Vec::new(),
                LocalObservationTime::from_nanos_since_start(receive_time),
                LocalObservationTime::from_nanos_since_start(receive_time),
            ),
            vec![BookLevel::new(
                Price::from_str("100").unwrap(),
                Quantity::from_str("1").unwrap(),
                None,
            )],
            vec![BookLevel::new(
                Price::from_str("101").unwrap(),
                Quantity::from_str("2").unwrap(),
                None,
            )],
        )
        .unwrap()
    }

    #[test]
    fn bbo_is_available_only_when_fresh_and_exactly_matches_l2_top() {
        let mut engine = MarketDataEngine::new();
        engine
            .process(
                1,
                &NormalizedMarketEvent::OrderBookSnapshot(snapshot_fixture(100)),
            )
            .unwrap();
        engine
            .process(
                2,
                &NormalizedMarketEvent::BestBidOfferUpdated(bbo_fixture(200, "100")),
            )
            .unwrap();
        assert_eq!(
            engine.report().best_bid_offers[0].status,
            BestBidOfferStatus::Available
        );

        engine
            .process(
                3,
                &NormalizedMarketEvent::BestBidOfferUpdated(bbo_fixture(300, "102")),
            )
            .unwrap();
        assert_eq!(
            engine.report().best_bid_offers[0].status,
            BestBidOfferStatus::Unknown
        );

        engine
            .process(
                4,
                &NormalizedMarketEvent::BestBidOfferUpdated(bbo_fixture(500_000_101, "100")),
            )
            .unwrap();
        assert_eq!(
            engine.report().best_bid_offers[0].status,
            BestBidOfferStatus::Unknown
        );
    }
}
