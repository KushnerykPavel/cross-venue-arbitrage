use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::{NonZeroU32, NonZeroUsize};
use std::str::FromStr;
use std::time::Duration;

use domain::{DecimalParseError, MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    BestBidOffer, BookLevel, EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation,
    ExchangeTimeUnit, LocalObservationTime, MarketDataUnavailable, NormalizedMarketEvent,
    OrderBook, OrderBookIdentityError, OrderBookSnapshot, SnapshotValidationError,
    TradeStreamResumed, UnavailabilityCategory,
};
use serde::Deserialize;
use serde_json::Value;
use venue::{
    AdapterAction, BoundedDeduplicator, DeduplicationResult, MarketDataAdapter, MarketKey,
    ObservationClock,
};

use crate::trade::{WireTrade, decode_trade};

const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(50);
const APPLICATION_PING: &str = r#"{"method":"ping"}"#;
const MAX_LEVELS_PER_SIDE: usize = 20;

#[derive(Debug)]
pub struct HyperliquidAdapter {
    markets: Vec<MarketOrderBook>,
}

impl HyperliquidAdapter {
    pub fn new(market_coins: Vec<MarketCoin>, trade_dedup_capacity: NonZeroUsize) -> Self {
        Self {
            markets: market_coins
                .into_iter()
                .map(|coin| MarketOrderBook::new(coin, trade_dedup_capacity))
                .collect(),
        }
    }

    pub fn resolved_markets(&self) -> Vec<MarketCoin> {
        self.markets
            .iter()
            .map(|market| market.market_coin().clone())
            .collect()
    }
}

impl MarketDataAdapter for HyperliquidAdapter {
    fn venue(&self) -> Venue {
        Venue::Hyperliquid
    }
    fn endpoint(&self) -> &str {
        HYPERLIQUID_WS_URL
    }
    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(HEARTBEAT_INTERVAL)
    }

    fn on_connected(&mut self) -> Vec<AdapterAction> {
        self.markets
            .iter()
            .enumerate()
            .flat_map(|(index, market)| {
                let key = MarketKey::new(index);
                [
                    AdapterAction::SendText(market.subscription_message()),
                    AdapterAction::SendText(market.bbo_subscription_message()),
                    AdapterAction::SendText(market.trade_subscription_message()),
                    AdapterAction::MarketSubscribed(key),
                ]
            })
            .collect()
    }

    fn on_heartbeat(&mut self) -> Vec<AdapterAction> {
        vec![AdapterAction::SendText(APPLICATION_PING.into())]
    }

    fn on_text(
        &mut self,
        text: &str,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Vec<AdapterAction> {
        let Ok(envelope) = serde_json::from_str::<WireEnvelope>(text) else {
            return Vec::new();
        };
        if envelope.channel == "trades" {
            let Ok(trades) = serde_json::from_value::<Vec<WireTrade>>(envelope.data) else {
                return Vec::new();
            };
            let mut actions = Vec::new();
            for wire in trades {
                let index = self
                    .markets
                    .iter()
                    .position(|market| market.market_coin().as_str() == wire.coin());
                let Some(index) = index else {
                    continue;
                };
                let market_coin = self.markets[index].market_coin().clone();
                match self.markets[index].apply_trade(wire, local_receive, clock) {
                    Ok(TradeApplied::New { trade, resumed }) => {
                        if resumed {
                            actions.push(AdapterAction::Publish(
                                NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
                                    Venue::Hyperliquid,
                                    market_coin.clone(),
                                    local_receive,
                                )),
                            ));
                        }
                        actions.push(AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(
                            trade,
                        )));
                    }
                    Ok(TradeApplied::Duplicate) => {
                        actions.push(AdapterAction::TradeDeduplicated(MarketKey::new(index)));
                    }
                    Err(AdapterError::TradeDedupCapacityExceeded { capacity }) => {
                        actions.push(AdapterAction::Stop {
                            reason: format!(
                                "Hyperliquid {market_coin} trade dedup capacity {capacity} exceeded"
                            ),
                        });
                        break;
                    }
                    Err(error) => {
                        self.markets[index].mark_trade_unavailable();
                        actions.push(AdapterAction::Publish(
                            NormalizedMarketEvent::TradeStreamUnavailable(
                                MarketDataUnavailable::new(
                                    Venue::Hyperliquid,
                                    market_coin,
                                    local_receive,
                                    UnavailabilityCategory::InvalidMarketData,
                                    error.to_string(),
                                ),
                            ),
                        ));
                    }
                }
            }
            return actions;
        }
        if envelope.channel == "bbo" {
            let Ok(bbo) = serde_json::from_value::<WireBbo>(envelope.data) else {
                return Vec::new();
            };
            let Some(index) = self
                .markets
                .iter()
                .position(|market| market.market_coin().as_str() == bbo.coin)
            else {
                return Vec::new();
            };
            match decode_bbo(bbo, &self.markets[index].market_coin, local_receive) {
                Ok(bbo) => {
                    return vec![AdapterAction::Publish(
                        NormalizedMarketEvent::BestBidOfferUpdated(bbo),
                    )];
                }
                Err(_) => return Vec::new(),
            }
        }
        if envelope.channel != "l2Book" {
            return Vec::new();
        }
        let Some(coin) = envelope.data.get("coin").and_then(|coin| coin.as_str()) else {
            return Vec::new();
        };
        let Some(index) = self
            .markets
            .iter()
            .position(|market| market.market_coin().as_str() == coin)
        else {
            return Vec::new();
        };
        match self.markets[index].apply_book_payload(envelope.data, local_receive, clock) {
            Ok(()) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(
                    self.markets[index]
                        .book()
                        .current()
                        .expect("accepted update installs a snapshot")
                        .clone(),
                ),
            )],
            Err(error) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookUnavailable(MarketDataUnavailable::new(
                    Venue::Hyperliquid,
                    self.markets[index].market_coin().clone(),
                    local_receive,
                    UnavailabilityCategory::InvalidMarketData,
                    error.to_string(),
                )),
            )],
        }
    }

    fn on_disconnected(
        &mut self,
        reason: &str,
        observed_at: LocalObservationTime,
    ) -> Vec<AdapterAction> {
        self.markets
            .iter_mut()
            .flat_map(|market| {
                market.disconnected();
                let coin = market.market_coin().clone();
                [
                    AdapterAction::Publish(NormalizedMarketEvent::OrderBookUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Hyperliquid,
                            coin.clone(),
                            observed_at,
                            UnavailabilityCategory::Disconnected,
                            reason,
                        ),
                    )),
                    AdapterAction::Publish(NormalizedMarketEvent::TradeStreamUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Hyperliquid,
                            coin,
                            observed_at,
                            UnavailabilityCategory::Disconnected,
                            reason,
                        ),
                    )),
                ]
            })
            .collect()
    }

    fn market_coin(&self, market: MarketKey) -> Option<&MarketCoin> {
        self.markets
            .get(market.index())
            .map(MarketOrderBook::market_coin)
    }
}

#[derive(Debug)]
struct MarketOrderBook {
    market_coin: MarketCoin,
    book: OrderBook,
    trade_dedup: BoundedDeduplicator<(u64, u64)>,
    trade_available: bool,
}

impl MarketOrderBook {
    fn new(market_coin: MarketCoin, trade_dedup_capacity: NonZeroUsize) -> Self {
        let symbol = Symbol::perpetual(market_coin.clone());
        Self {
            market_coin,
            book: OrderBook::new(Venue::Hyperliquid, symbol),
            trade_dedup: BoundedDeduplicator::new(trade_dedup_capacity),
            trade_available: false,
        }
    }

    fn subscription_message(&self) -> String {
        serde_json::json!({"method":"subscribe","subscription":{"type":"l2Book","coin":self.market_coin.as_str()}}).to_string()
    }

    fn bbo_subscription_message(&self) -> String {
        serde_json::json!({"method":"subscribe","subscription":{"type":"bbo","coin":self.market_coin.as_str()}}).to_string()
    }

    fn trade_subscription_message(&self) -> String {
        serde_json::json!({"method":"subscribe","subscription":{"type":"trades","coin":self.market_coin.as_str()}}).to_string()
    }

    fn apply_book_payload(
        &mut self,
        payload: Value,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<(), AdapterError> {
        let wire_book =
            serde_json::from_value(payload).map_err(AdapterError::InvalidBookPayload)?;
        let snapshot = decode_snapshot(wire_book, &self.market_coin, local_receive)?;
        self.book
            .replace_at_processing_completion(snapshot, || clock.now())
            .map_err(AdapterError::IdentityMismatch)
    }

    fn disconnected(&mut self) {
        self.book.mark_unhealthy();
        self.trade_available = false;
    }

    fn mark_trade_unavailable(&mut self) {
        self.trade_available = false;
    }

    fn apply_trade(
        &mut self,
        wire: WireTrade,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<TradeApplied, AdapterError> {
        let trade = decode_trade(wire, &self.market_coin, local_receive, clock.now())
            .map_err(AdapterError::InvalidTrade)?;
        let market_data::MarketTradeIdentity::Hyperliquid {
            block_time,
            trade_id,
            ..
        } = trade.identity()
        else {
            unreachable!("Hyperliquid decoder returns Hyperliquid identity")
        };
        match self.trade_dedup.observe((*block_time, *trade_id)) {
            DeduplicationResult::New => {
                let resumed = !self.trade_available;
                self.trade_available = true;
                Ok(TradeApplied::New { trade, resumed })
            }
            DeduplicationResult::Duplicate => Ok(TradeApplied::Duplicate),
            DeduplicationResult::CapacityExceeded => {
                Err(AdapterError::TradeDedupCapacityExceeded {
                    capacity: self.trade_dedup.capacity().get(),
                })
            }
        }
    }
    const fn market_coin(&self) -> &MarketCoin {
        &self.market_coin
    }
    const fn book(&self) -> &OrderBook {
        &self.book
    }
}

enum TradeApplied {
    New {
        trade: market_data::MarketTrade,
        resumed: bool,
    },
    Duplicate,
}

#[derive(Debug)]
enum AdapterError {
    InvalidBookPayload(serde_json::Error),
    UnexpectedCoin {
        expected: MarketCoin,
        actual: String,
    },
    InvalidPrice {
        side: BookSide,
        level: usize,
        source: DecimalParseError,
    },
    InvalidQuantity {
        side: BookSide,
        level: usize,
        source: DecimalParseError,
    },
    ZeroOrderCount {
        side: BookSide,
        level: usize,
    },
    TooManyLevels {
        side: BookSide,
        actual: usize,
        maximum: usize,
    },
    InvalidSnapshot(SnapshotValidationError),
    IdentityMismatch(OrderBookIdentityError),
    InvalidTrade(crate::trade::TradeDecodeError),
    TradeDedupCapacityExceeded {
        capacity: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Deserialize)]
struct WireEnvelope {
    channel: String,
    #[serde(default)]
    data: Value,
}

#[derive(Deserialize)]
struct WireBook {
    coin: String,
    levels: [Vec<WireLevel>; 2],
    time: u64,
}

#[derive(Deserialize)]
struct WireBbo {
    coin: String,
    bbo: [Option<WireLevel>; 2],
    time: u64,
}

#[derive(Deserialize)]
struct WireLevel {
    px: String,
    sz: String,
    n: u32,
}

fn decode_bbo(
    wire_bbo: WireBbo,
    expected_coin: &MarketCoin,
    local_receive: LocalObservationTime,
) -> Result<BestBidOffer, AdapterError> {
    if wire_bbo.coin != expected_coin.as_str() {
        return Err(AdapterError::UnexpectedCoin {
            expected: expected_coin.clone(),
            actual: wire_bbo.coin,
        });
    }
    let [bid, ask] = wire_bbo.bbo;
    let bid = bid
        .map(|level| decode_level(level, BookSide::Bid, 0))
        .transpose()?;
    let ask = ask
        .map(|level| decode_level(level, BookSide::Ask, 0))
        .transpose()?;
    Ok(BestBidOffer::new(
        Venue::Hyperliquid,
        Symbol::perpetual(expected_coin.clone()),
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                wire_bbo.time,
                ExchangeTimeUnit::Unknown,
            )],
            local_receive,
            local_receive,
        ),
        bid,
        ask,
    ))
}

fn decode_snapshot(
    wire_book: WireBook,
    expected_coin: &MarketCoin,
    local_receive: LocalObservationTime,
) -> Result<OrderBookSnapshot, AdapterError> {
    if wire_book.coin != expected_coin.as_str() {
        return Err(AdapterError::UnexpectedCoin {
            expected: expected_coin.clone(),
            actual: wire_book.coin,
        });
    }
    let [wire_bids, wire_asks] = wire_book.levels;
    let bids = decode_levels(wire_bids, BookSide::Bid)?;
    let asks = decode_levels(wire_asks, BookSide::Ask)?;
    OrderBookSnapshot::try_new(
        Venue::Hyperliquid,
        Symbol::perpetual(expected_coin.clone()),
        None,
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                wire_book.time,
                ExchangeTimeUnit::Unknown,
            )],
            local_receive,
            local_receive,
        ),
        bids,
        asks,
    )
    .map_err(AdapterError::InvalidSnapshot)
}

fn decode_levels(
    wire_levels: Vec<WireLevel>,
    side: BookSide,
) -> Result<Vec<BookLevel>, AdapterError> {
    if wire_levels.len() > MAX_LEVELS_PER_SIDE {
        return Err(AdapterError::TooManyLevels {
            side,
            actual: wire_levels.len(),
            maximum: MAX_LEVELS_PER_SIDE,
        });
    }
    wire_levels
        .into_iter()
        .enumerate()
        .map(|(level, wire)| {
            let price = Price::from_str(&wire.px).map_err(|source| AdapterError::InvalidPrice {
                side,
                level,
                source,
            })?;
            let quantity =
                Quantity::from_str(&wire.sz).map_err(|source| AdapterError::InvalidQuantity {
                    side,
                    level,
                    source,
                })?;
            let order_count =
                NonZeroU32::new(wire.n).ok_or(AdapterError::ZeroOrderCount { side, level })?;
            Ok(BookLevel::new(price, quantity, Some(order_count)))
        })
        .collect()
}

fn decode_level(wire: WireLevel, side: BookSide, level: usize) -> Result<BookLevel, AdapterError> {
    let price = Price::from_str(&wire.px).map_err(|source| AdapterError::InvalidPrice {
        side,
        level,
        source,
    })?;
    let quantity =
        Quantity::from_str(&wire.sz).map_err(|source| AdapterError::InvalidQuantity {
            side,
            level,
            source,
        })?;
    let order_count =
        NonZeroU32::new(wire.n).ok_or(AdapterError::ZeroOrderCount { side, level })?;
    Ok(BookLevel::new(price, quantity, Some(order_count)))
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBookPayload(error) => write!(formatter, "invalid l2Book payload: {error}"),
            Self::UnexpectedCoin { expected, actual } => {
                write!(formatter, "expected {expected} book, received {actual}")
            }
            Self::InvalidPrice {
                side,
                level,
                source,
            } => write!(formatter, "invalid {side} price at level {level}: {source}"),
            Self::InvalidQuantity {
                side,
                level,
                source,
            } => write!(
                formatter,
                "invalid {side} quantity at level {level}: {source}"
            ),
            Self::ZeroOrderCount { side, level } => {
                write!(formatter, "zero {side} order count at level {level}")
            }
            Self::TooManyLevels {
                side,
                actual,
                maximum,
            } => write!(
                formatter,
                "received {actual} {side} levels, maximum is {maximum}"
            ),
            Self::InvalidSnapshot(error) => Display::fmt(error, formatter),
            Self::IdentityMismatch(error) => Display::fmt(error, formatter),
            Self::InvalidTrade(error) => Display::fmt(error, formatter),
            Self::TradeDedupCapacityExceeded { capacity } => {
                write!(formatter, "trade dedup capacity {capacity} exceeded")
            }
        }
    }
}

impl Error for AdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBookPayload(error) => Some(error),
            Self::InvalidPrice { source, .. } | Self::InvalidQuantity { source, .. } => {
                Some(source)
            }
            Self::InvalidSnapshot(error) => Some(error),
            Self::IdentityMismatch(error) => Some(error),
            Self::InvalidTrade(error) => Some(error),
            _ => None,
        }
    }
}

impl Display for BookSide {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bid => formatter.write_str("bid"),
            Self::Ask => formatter.write_str("ask"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::cell::Cell;

    const SNAPSHOT: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":10,"levels":[[{"px":"100","sz":"1","n":2}],[{"px":"101","sz":"2","n":1}]]}}"#;
    const BBO: &str = r#"{"channel":"bbo","data":{"coin":"BTC","time":11,"bbo":[{"px":"100","sz":"1","n":2},{"px":"101","sz":"2","n":1}]}}"#;
    const TRADES: &str = r#"{"channel":"trades","data":[{"coin":"BTC","side":"B","px":"100","sz":"1","hash":"0x1","time":10,"tid":1},{"coin":"BTC","side":"A","px":"101","sz":"2","hash":"0x2","time":11,"tid":2}]}"#;

    struct Clock(Cell<u64>);
    impl ObservationClock for Clock {
        fn now(&self) -> LocalObservationTime {
            let next = self.0.get() + 1;
            self.0.set(next);
            LocalObservationTime::from_nanos_since_start(next)
        }
    }

    #[test]
    fn routes_through_whole_market_adapter_and_builds_subscriptions() {
        let mut adapter = HyperliquidAdapter::new(
            vec![
                MarketCoin::try_new("BTC").unwrap(),
                MarketCoin::try_new("ETH").unwrap(),
            ],
            NonZeroUsize::new(10).unwrap(),
        );
        let connected = adapter.on_connected();
        let AdapterAction::SendText(message) = &connected[0] else {
            panic!("first action sends subscription");
        };
        let value: Value = serde_json::from_str(message).unwrap();
        assert_eq!(value["subscription"]["coin"], "BTC");
        assert!(value["subscription"].get("fast").is_none());
        let actions = adapter.on_text(
            SNAPSHOT,
            LocalObservationTime::from_nanos_since_start(1),
            &Clock(Cell::new(1)),
        );
        assert!(matches!(
            actions.as_slice(),
            [AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(_)
            )]
        ));
        let snapshot = adapter.markets[0].book().current().unwrap();
        assert_eq!(snapshot.timestamps().local_receive().nanos_since_start(), 1);
        assert_eq!(
            snapshot
                .timestamps()
                .processing_completed()
                .nanos_since_start(),
            2
        );
        assert!(adapter.markets[1].book().current().is_none());
    }

    #[test]
    fn decodes_bbo_as_an_independent_normalized_event() {
        let mut adapter = HyperliquidAdapter::new(
            vec![MarketCoin::try_new("BTC").unwrap()],
            NonZeroUsize::new(10).unwrap(),
        );
        let actions = adapter.on_text(
            BBO,
            LocalObservationTime::from_nanos_since_start(42),
            &Clock(Cell::new(42)),
        );
        let AdapterAction::Publish(NormalizedMarketEvent::BestBidOfferUpdated(bbo)) = &actions[0]
        else {
            panic!("expected normalized BBO event")
        };
        assert_eq!(bbo.bid().unwrap().price().to_string(), "100");
        assert_eq!(bbo.ask().unwrap().quantity().to_string(), "2");
        assert_eq!(bbo.timestamps().local_receive().nanos_since_start(), 42);
    }

    #[test]
    fn decodes_one_sided_bbo_without_fabricating_the_missing_side() {
        let mut adapter = HyperliquidAdapter::new(
            vec![MarketCoin::try_new("BTC").unwrap()],
            NonZeroUsize::new(10).unwrap(),
        );
        let actions = adapter.on_text(
            &BBO.replace(r#"{"px":"100","sz":"1","n":2}"#, "null"),
            LocalObservationTime::from_nanos_since_start(42),
            &Clock(Cell::new(42)),
        );
        let AdapterAction::Publish(NormalizedMarketEvent::BestBidOfferUpdated(bbo)) = &actions[0]
        else {
            panic!("expected normalized BBO event")
        };
        assert!(bbo.bid().is_none());
        assert!(bbo.ask().is_some());
    }

    #[test]
    fn invalid_identified_book_isolated_and_disconnect_invalidates_all() {
        let mut adapter = HyperliquidAdapter::new(
            vec![
                MarketCoin::try_new("BTC").unwrap(),
                MarketCoin::try_new("ETH").unwrap(),
            ],
            NonZeroUsize::new(10).unwrap(),
        );
        let clock = Clock(Cell::new(1));
        adapter.on_text(
            SNAPSHOT,
            LocalObservationTime::from_nanos_since_start(1),
            &clock,
        );
        let invalid = SNAPSHOT.replace(r#""sz":"1""#, r#""sz":"0""#);
        assert!(matches!(
            adapter.on_text(
                &invalid,
                LocalObservationTime::from_nanos_since_start(3),
                &clock
            )[0],
            AdapterAction::Publish(NormalizedMarketEvent::OrderBookUnavailable(_))
        ));
        assert_eq!(
            adapter
                .on_disconnected("lost", LocalObservationTime::from_nanos_since_start(4))
                .len(),
            4
        );
    }

    #[test]
    fn preserves_trade_batch_order_as_owned_events() {
        let mut adapter = HyperliquidAdapter::new(
            vec![MarketCoin::try_new("BTC").unwrap()],
            NonZeroUsize::new(10).unwrap(),
        );
        let actions = adapter.on_text(
            TRADES,
            LocalObservationTime::from_nanos_since_start(1),
            &Clock(Cell::new(1)),
        );
        let trade_ids = actions
            .iter()
            .filter_map(|action| match action {
                AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(trade)) => {
                    let market_data::MarketTradeIdentity::Hyperliquid { trade_id, .. } =
                        trade.identity()
                    else {
                        panic!("wrong venue identity")
                    };
                    Some(*trade_id)
                }
                AdapterAction::Publish(NormalizedMarketEvent::TradeStreamResumed(_)) => None,
                _ => panic!("unexpected action"),
            })
            .collect::<Vec<_>>();
        assert_eq!(trade_ids, [1, 2]);
    }

    #[test]
    fn deduplicates_trade_batch_and_stops_at_capacity() {
        let coin = MarketCoin::try_new("BTC").unwrap();
        let clock = Clock(Cell::new(1));
        let mut adapter =
            HyperliquidAdapter::new(vec![coin.clone()], NonZeroUsize::new(10).unwrap());
        adapter.on_text(
            TRADES,
            LocalObservationTime::from_nanos_since_start(1),
            &clock,
        );
        assert!(
            adapter
                .on_text(
                    TRADES,
                    LocalObservationTime::from_nanos_since_start(2),
                    &clock
                )
                .iter()
                .all(|action| matches!(action, AdapterAction::TradeDeduplicated(_)))
        );

        let mut full = HyperliquidAdapter::new(vec![coin], NonZeroUsize::new(1).unwrap());
        let actions = full.on_text(
            TRADES,
            LocalObservationTime::from_nanos_since_start(1),
            &clock,
        );
        assert!(matches!(actions.last(), Some(AdapterAction::Stop { .. })));
    }
}
