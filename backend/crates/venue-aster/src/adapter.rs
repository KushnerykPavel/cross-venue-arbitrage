use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::str::FromStr;

use domain::{DecimalParseError, MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    BookLevel, EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit,
    LocalObservationTime, MarketDataUnavailable, NormalizedMarketEvent, OrderBook,
    OrderBookIdentityError, OrderBookSnapshot, SnapshotValidationError, TradeStreamResumed,
    UnavailabilityCategory,
};
use serde::Deserialize;
use serde_json::Value;
use venue::{
    AdapterAction, BoundedDeduplicator, DeduplicationResult, MarketDataAdapter, MarketKey,
    ObservationClock,
};

use crate::metadata::{AsterMarket, MetadataError, resolve_markets};
use crate::trade::decode_trade;

const ASTER_WS_BASE_URL: &str = "wss://fstream.asterdex.com/stream?streams=";
const MAX_LEVELS_PER_SIDE: usize = 10;

#[derive(Debug)]
pub struct AsterAdapter {
    markets: Vec<MarketOrderBook>,
    endpoint: String,
}

impl AsterAdapter {
    pub async fn bootstrap(
        market_coins: Vec<MarketCoin>,
        trade_dedup_capacity: NonZeroUsize,
    ) -> Result<Self, MetadataError> {
        Ok(Self::from_markets(
            resolve_markets(&market_coins).await?,
            trade_dedup_capacity,
        ))
    }

    fn from_markets(markets: Vec<AsterMarket>, trade_dedup_capacity: NonZeroUsize) -> Self {
        let streams = markets
            .iter()
            .flat_map(|market| {
                let symbol = market.symbol().to_lowercase();
                [
                    format!("{symbol}@depth10@100ms"),
                    format!("{symbol}@aggTrade"),
                ]
            })
            .collect::<Vec<_>>()
            .join("/");
        Self {
            markets: markets
                .into_iter()
                .map(|market| MarketOrderBook::new(market, trade_dedup_capacity))
                .collect(),
            endpoint: format!("{ASTER_WS_BASE_URL}{streams}"),
        }
    }

    pub fn resolved_markets(&self) -> Vec<(MarketCoin, String)> {
        self.markets
            .iter()
            .map(|market| {
                (
                    market.market().market_coin().clone(),
                    market.market().symbol().to_owned(),
                )
            })
            .collect()
    }
}

impl MarketDataAdapter for AsterAdapter {
    fn venue(&self) -> Venue {
        Venue::Aster
    }
    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn on_connected(&mut self) -> Vec<AdapterAction> {
        (0..self.markets.len())
            .map(|index| AdapterAction::MarketSubscribed(MarketKey::new(index)))
            .collect()
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
        let Some(symbol) = envelope.data.get("s").and_then(|symbol| symbol.as_str()) else {
            return Vec::new();
        };
        let Some(index) = self
            .markets
            .iter()
            .position(|market| market.market().symbol() == symbol)
        else {
            return Vec::new();
        };
        if envelope.data.get("e").and_then(Value::as_str) == Some("aggTrade") {
            return match self.markets[index].apply_trade_payload(
                envelope.data,
                local_receive,
                clock,
            ) {
                Ok(TradeApplied::New { trade, resumed }) => {
                    let mut actions = Vec::with_capacity(2);
                    if resumed {
                        actions.push(AdapterAction::Publish(
                            NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
                                Venue::Aster,
                                self.markets[index].market().market_coin().clone(),
                                local_receive,
                            )),
                        ));
                    }
                    actions.push(AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(
                        trade,
                    )));
                    actions
                }
                Ok(TradeApplied::Duplicate) => {
                    vec![AdapterAction::TradeDeduplicated(MarketKey::new(index))]
                }
                Err(AdapterError::TradeDedupCapacityExceeded { capacity }) => {
                    vec![AdapterAction::Stop {
                        reason: format!(
                            "Aster {} trade dedup capacity {capacity} exceeded",
                            self.markets[index].market().market_coin()
                        ),
                    }]
                }
                Err(error) => {
                    self.markets[index].mark_trade_unavailable();
                    vec![AdapterAction::Publish(
                        NormalizedMarketEvent::TradeStreamUnavailable(MarketDataUnavailable::new(
                            Venue::Aster,
                            self.markets[index].market().market_coin().clone(),
                            local_receive,
                            UnavailabilityCategory::InvalidMarketData,
                            error.to_string(),
                        )),
                    )]
                }
            };
        }
        match self.markets[index].apply_book_payload(envelope.data, local_receive, clock) {
            Ok(Applied::Yes) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(
                    self.markets[index]
                        .book()
                        .current()
                        .expect("accepted update installs a snapshot")
                        .clone(),
                ),
            )],
            Ok(Applied::No) => Vec::new(),
            Err(error) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookUnavailable(MarketDataUnavailable::new(
                    Venue::Aster,
                    self.markets[index].market().market_coin().clone(),
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
                let coin = market.market().market_coin().clone();
                [
                    AdapterAction::Publish(NormalizedMarketEvent::OrderBookUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Aster,
                            coin.clone(),
                            observed_at,
                            UnavailabilityCategory::Disconnected,
                            reason,
                        ),
                    )),
                    AdapterAction::Publish(NormalizedMarketEvent::TradeStreamUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Aster,
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
            .map(|market| market.market().market_coin())
    }
}

#[derive(Debug)]
struct MarketOrderBook {
    market: AsterMarket,
    book: OrderBook,
    trade_dedup: BoundedDeduplicator<u64>,
    trade_available: bool,
}

impl MarketOrderBook {
    fn new(market: AsterMarket, trade_dedup_capacity: NonZeroUsize) -> Self {
        let symbol = Symbol::perpetual(market.market_coin().clone());
        Self {
            market,
            book: OrderBook::new(Venue::Aster, symbol),
            trade_dedup: BoundedDeduplicator::new(trade_dedup_capacity),
            trade_available: false,
        }
    }

    fn apply_book_payload(
        &mut self,
        payload: Value,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<Applied, AdapterError> {
        let wire: WireBook =
            serde_json::from_value(payload).map_err(AdapterError::InvalidBookPayload)?;
        if wire.event_type != "depthUpdate" {
            return Ok(Applied::No);
        }
        if wire.symbol != self.market.symbol() {
            self.book.mark_unhealthy();
            return Err(AdapterError::UnexpectedSymbol {
                expected: self.market.symbol().into(),
                actual: wire.symbol,
            });
        }
        let snapshot = decode_snapshot(wire, &self.market, local_receive)?;
        self.book
            .replace_at_processing_completion(snapshot, || clock.now())
            .map_err(AdapterError::IdentityMismatch)?;
        Ok(Applied::Yes)
    }

    fn disconnected(&mut self) {
        self.book.mark_unhealthy();
        self.trade_available = false;
    }

    fn mark_trade_unavailable(&mut self) {
        self.trade_available = false;
    }

    fn apply_trade_payload(
        &mut self,
        payload: Value,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<TradeApplied, AdapterError> {
        let trade = decode_trade(
            payload,
            self.market.symbol(),
            self.market.market_coin(),
            local_receive,
            clock.now(),
        )
        .map_err(AdapterError::InvalidTrade)?;
        let market_data::MarketTradeIdentity::Aster {
            aggregate_trade_id, ..
        } = trade.identity()
        else {
            unreachable!("Aster decoder returns Aster identity")
        };
        match self.trade_dedup.observe(*aggregate_trade_id) {
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
    const fn market(&self) -> &AsterMarket {
        &self.market
    }
    const fn book(&self) -> &OrderBook {
        &self.book
    }
}

#[derive(Clone, Copy)]
enum Applied {
    Yes,
    No,
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
    UnexpectedSymbol {
        expected: String,
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

#[derive(Clone, Copy, Debug)]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Deserialize)]
struct WireEnvelope {
    #[serde(default)]
    data: Value,
}

#[derive(Deserialize)]
struct WireBook {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

fn decode_snapshot(
    wire: WireBook,
    market: &AsterMarket,
    local_receive: LocalObservationTime,
) -> Result<OrderBookSnapshot, AdapterError> {
    let bids = decode_levels(wire.bids, BookSide::Bid)?;
    let asks = decode_levels(wire.asks, BookSide::Ask)?;
    OrderBookSnapshot::try_new(
        Venue::Aster,
        Symbol::perpetual(market.market_coin().clone()),
        Some(wire.final_update_id),
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                wire.event_time,
                ExchangeTimeUnit::Milliseconds,
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
    wire_levels: Vec<[String; 2]>,
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
        .map(|(level, [price, quantity])| {
            let price = Price::from_str(&price).map_err(|source| AdapterError::InvalidPrice {
                side,
                level,
                source,
            })?;
            let quantity =
                Quantity::from_str(&quantity).map_err(|source| AdapterError::InvalidQuantity {
                    side,
                    level,
                    source,
                })?;
            Ok(BookLevel::new(price, quantity, None))
        })
        .collect()
}

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBookPayload(error) => {
                write!(formatter, "invalid Aster depth payload: {error}")
            }
            Self::UnexpectedSymbol { expected, actual } => write!(
                formatter,
                "expected Aster symbol {expected}, received {actual}"
            ),
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
    use std::cell::Cell;

    const SNAPSHOT: &str = r#"{"stream":"btcusdt@depth10@100ms","data":{"e":"depthUpdate","E":10,"T":9,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[["100","1"]],"a":[["101","1"]]}}"#;
    const TRADE: &str = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":10,"s":"BTCUSDT","a":91,"p":"100","q":"1","f":700,"l":703,"T":9,"m":false}}"#;

    struct Clock(Cell<u64>);
    impl ObservationClock for Clock {
        fn now(&self) -> LocalObservationTime {
            let next = self.0.get() + 1;
            self.0.set(next);
            LocalObservationTime::from_nanos_since_start(next)
        }
    }

    fn adapter() -> AsterAdapter {
        AsterAdapter::from_markets(
            vec![
                AsterMarket::new(MarketCoin::try_new("BTC").unwrap(), "BTCUSDT".into()),
                AsterMarket::new(MarketCoin::try_new("ETH").unwrap(), "ETHUSDT".into()),
            ],
            NonZeroUsize::new(10).unwrap(),
        )
    }

    #[test]
    fn combined_endpoint_and_routing_are_one_adapter_responsibility() {
        let mut adapter = adapter();
        assert_eq!(
            adapter.endpoint(),
            "wss://fstream.asterdex.com/stream?streams=btcusdt@depth10@100ms/btcusdt@aggTrade/ethusdt@depth10@100ms/ethusdt@aggTrade"
        );
        assert_eq!(adapter.on_connected().len(), 2);
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
        assert!(adapter.markets[0].book().current().is_some());
        assert!(adapter.markets[1].book().current().is_none());
    }

    #[test]
    fn invalid_market_is_isolated_and_disconnect_invalidates_all() {
        let mut adapter = adapter();
        let clock = Clock(Cell::new(1));
        adapter.on_text(
            SNAPSHOT,
            LocalObservationTime::from_nanos_since_start(1),
            &clock,
        );
        let invalid = SNAPSHOT.replace("\"1\"]],\"a\"", "\"0\"]],\"a\"");
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
    fn routes_aggregate_trade_as_owned_normalized_event() {
        let mut adapter = adapter();
        let actions = adapter.on_text(
            TRADE,
            LocalObservationTime::from_nanos_since_start(1),
            &Clock(Cell::new(1)),
        );
        assert!(matches!(
            actions.as_slice(),
            [
                AdapterAction::Publish(NormalizedMarketEvent::TradeStreamResumed(_)),
                AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(trade))
            ] if trade.symbol().market_coin().as_str() == "BTC"
        ));
    }

    #[test]
    fn drops_duplicate_and_stops_when_dedup_capacity_is_exhausted() {
        let markets = vec![AsterMarket::new(
            MarketCoin::try_new("BTC").unwrap(),
            "BTCUSDT".into(),
        )];
        let mut adapter = AsterAdapter::from_markets(markets, NonZeroUsize::new(1).unwrap());
        let clock = Clock(Cell::new(1));
        assert_eq!(
            adapter
                .on_text(
                    TRADE,
                    LocalObservationTime::from_nanos_since_start(1),
                    &clock
                )
                .len(),
            2
        );
        assert!(matches!(
            adapter
                .on_text(
                    TRADE,
                    LocalObservationTime::from_nanos_since_start(2),
                    &clock
                )
                .as_slice(),
            [AdapterAction::TradeDeduplicated(_)]
        ));
        let next = TRADE.replace("\"a\":91", "\"a\":92");
        assert!(matches!(
            adapter
                .on_text(
                    &next,
                    LocalObservationTime::from_nanos_since_start(3),
                    &clock
                )
                .as_slice(),
            [AdapterAction::Stop { .. }]
        ));
    }
}
