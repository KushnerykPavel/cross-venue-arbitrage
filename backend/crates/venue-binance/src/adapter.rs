use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::str::FromStr;

use domain::{DecimalParseError, MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    BookLevel, EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit,
    LocalObservationTime, MarketDataUnavailable, MarketTrade, MarketTradeIdentity, MarketTradeKind,
    MarketTradeReportingKind, NormalizedMarketEvent, OrderBook, OrderBookIdentityError,
    OrderBookSnapshot, SnapshotValidationError, TradeStreamResumed, UnavailabilityCategory,
};
use serde::Deserialize;
use serde_json::Value;
use venue::{
    AdapterAction, BoundedDeduplicator, DeduplicationResult, MarketDataAdapter, MarketKey,
    ObservationClock,
};

use crate::metadata::{BinanceMarket, MetadataError, resolve_markets};

const BINANCE_WS_BASE_URL: &str = "wss://fstream.binance.com/stream?streams=";

#[derive(Debug)]
pub struct BinanceAdapter {
    markets: Vec<MarketOrderBook>,
    endpoint: String,
}

impl BinanceAdapter {
    pub async fn bootstrap(
        market_coins: Vec<MarketCoin>,
        trade_dedup_capacity: NonZeroUsize,
    ) -> Result<Self, MetadataError> {
        Ok(Self::from_markets(
            resolve_markets(&market_coins).await?,
            trade_dedup_capacity,
        ))
    }

    fn from_markets(markets: Vec<BinanceMarket>, trade_dedup_capacity: NonZeroUsize) -> Self {
        let streams = markets
            .iter()
            .flat_map(|market| {
                let symbol = market.symbol().to_lowercase();
                [
                    format!("{symbol}@depth20@100ms"),
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
            endpoint: format!("{BINANCE_WS_BASE_URL}{streams}"),
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

impl MarketDataAdapter for BinanceAdapter {
    fn venue(&self) -> Venue {
        Venue::Binance
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
        let Some(symbol) = envelope.data.get("s").and_then(Value::as_str) else {
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
            return self.handle_trade(index, envelope.data, local_receive, clock);
        }
        match self.markets[index].apply_book_payload(envelope.data, local_receive, clock) {
            Ok(true) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(
                    self.markets[index]
                        .book()
                        .current()
                        .expect("accepted update installs a snapshot")
                        .clone(),
                ),
            )],
            Ok(false) => Vec::new(),
            Err(error) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookUnavailable(MarketDataUnavailable::new(
                    Venue::Binance,
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
                            Venue::Binance,
                            coin.clone(),
                            observed_at,
                            UnavailabilityCategory::Disconnected,
                            reason,
                        ),
                    )),
                    AdapterAction::Publish(NormalizedMarketEvent::TradeStreamUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Binance,
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

impl BinanceAdapter {
    fn handle_trade(
        &mut self,
        index: usize,
        payload: Value,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Vec<AdapterAction> {
        match self.markets[index].apply_trade_payload(payload, local_receive, clock) {
            Ok(TradeApplied::New { trade, resumed }) => {
                let mut actions = Vec::with_capacity(2);
                if resumed {
                    actions.push(AdapterAction::Publish(
                        NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
                            Venue::Binance,
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
                        "Binance {} trade dedup capacity {capacity} exceeded",
                        self.markets[index].market().market_coin()
                    ),
                }]
            }
            Err(error) => {
                self.markets[index].mark_trade_unavailable();
                vec![AdapterAction::Publish(
                    NormalizedMarketEvent::TradeStreamUnavailable(MarketDataUnavailable::new(
                        Venue::Binance,
                        self.markets[index].market().market_coin().clone(),
                        local_receive,
                        UnavailabilityCategory::InvalidMarketData,
                        error.to_string(),
                    )),
                )]
            }
        }
    }
}

#[derive(Debug)]
struct MarketOrderBook {
    market: BinanceMarket,
    book: OrderBook,
    trade_dedup: BoundedDeduplicator<u64>,
    trade_available: bool,
}

impl MarketOrderBook {
    fn new(market: BinanceMarket, trade_dedup_capacity: NonZeroUsize) -> Self {
        let symbol = Symbol::perpetual(market.market_coin().clone());
        Self {
            market,
            book: OrderBook::new(Venue::Binance, symbol),
            trade_dedup: BoundedDeduplicator::new(trade_dedup_capacity),
            trade_available: false,
        }
    }

    fn apply_book_payload(
        &mut self,
        payload: Value,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<bool, AdapterError> {
        let wire: WireBook =
            serde_json::from_value(payload).map_err(AdapterError::InvalidBookPayload)?;
        if wire.event_type != "depthUpdate" {
            return Ok(false);
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
        Ok(true)
    }

    fn apply_trade_payload(
        &mut self,
        payload: Value,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<TradeApplied, AdapterError> {
        let wire: WireTrade =
            serde_json::from_value(payload).map_err(AdapterError::InvalidTradePayload)?;
        if wire.event_type != "aggTrade" {
            return Err(AdapterError::UnexpectedEventType(wire.event_type));
        }
        if wire.symbol != self.market.symbol() {
            return Err(AdapterError::UnexpectedSymbol {
                expected: self.market.symbol().into(),
                actual: wire.symbol,
            });
        }
        let price = Price::from_str(&wire.price).map_err(AdapterError::InvalidPrice)?;
        let quantity = Quantity::from_str(&wire.quantity).map_err(AdapterError::InvalidQuantity)?;
        let trade = MarketTrade::new(
            Venue::Binance,
            Symbol::perpetual(self.market.market_coin().clone()),
            EventTimestamps::new(
                vec![
                    ExchangeTimeObservation::new(
                        ExchangeTimeKind::EventTime,
                        wire.event_time,
                        ExchangeTimeUnit::Milliseconds,
                    ),
                    ExchangeTimeObservation::new(
                        ExchangeTimeKind::TradeTime,
                        wire.trade_time,
                        ExchangeTimeUnit::Milliseconds,
                    ),
                ],
                local_receive,
                clock.now(),
            ),
            price,
            quantity,
            MarketTradeReportingKind::TakerOrderAggregate,
            MarketTradeKind::Regular,
            if wire.buyer_is_maker {
                market_data::AggressorSide::Sell
            } else {
                market_data::AggressorSide::Buy
            },
            market_data::AggressorSideClassification::DerivedFromMakerSide,
            MarketTradeIdentity::Binance {
                aggregate_trade_id: wire.aggregate_trade_id,
                first_trade_id: wire.first_trade_id,
                last_trade_id: wire.last_trade_id,
            },
        );
        match self.trade_dedup.observe(wire.aggregate_trade_id) {
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

    fn disconnected(&mut self) {
        self.book.mark_unhealthy();
        self.trade_available = false;
    }

    fn mark_trade_unavailable(&mut self) {
        self.trade_available = false;
    }

    const fn market(&self) -> &BinanceMarket {
        &self.market
    }
    const fn book(&self) -> &OrderBook {
        &self.book
    }
}

enum TradeApplied {
    New { trade: MarketTrade, resumed: bool },
    Duplicate,
}

#[derive(Debug)]
enum AdapterError {
    InvalidBookPayload(serde_json::Error),
    InvalidTradePayload(serde_json::Error),
    UnexpectedEventType(String),
    UnexpectedSymbol { expected: String, actual: String },
    InvalidPrice(DecimalParseError),
    InvalidQuantity(DecimalParseError),
    InvalidSnapshot(SnapshotValidationError),
    IdentityMismatch(OrderBookIdentityError),
    TradeDedupCapacityExceeded { capacity: usize },
}

#[derive(Deserialize)]
struct WireEnvelope {
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

#[derive(Deserialize)]
struct WireTrade {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "a")]
    aggregate_trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "f")]
    first_trade_id: u64,
    #[serde(rename = "l")]
    last_trade_id: u64,
    #[serde(rename = "T")]
    trade_time: u64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

fn decode_snapshot(
    wire: WireBook,
    market: &BinanceMarket,
    local_receive: LocalObservationTime,
) -> Result<OrderBookSnapshot, AdapterError> {
    let decode = |levels: Vec<[String; 2]>| {
        levels
            .into_iter()
            .map(|[price, quantity]| {
                Ok(BookLevel::new(
                    Price::from_str(&price).map_err(AdapterError::InvalidPrice)?,
                    Quantity::from_str(&quantity).map_err(AdapterError::InvalidQuantity)?,
                    None,
                ))
            })
            .collect::<Result<Vec<_>, AdapterError>>()
    };
    let bids = decode(wire.bids)?;
    let asks = decode(wire.asks)?;
    OrderBookSnapshot::try_new(
        Venue::Binance,
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

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBookPayload(error) => {
                write!(formatter, "invalid Binance depth payload: {error}")
            }
            Self::InvalidTradePayload(error) => {
                write!(formatter, "invalid Binance aggTrade payload: {error}")
            }
            Self::UnexpectedEventType(actual) => {
                write!(formatter, "unexpected Binance event type {actual}")
            }
            Self::UnexpectedSymbol { expected, actual } => write!(
                formatter,
                "expected Binance symbol {expected}, received {actual}"
            ),
            Self::InvalidPrice(error) => write!(formatter, "invalid Binance price: {error}"),
            Self::InvalidQuantity(error) => write!(formatter, "invalid Binance quantity: {error}"),
            Self::InvalidSnapshot(error) => Display::fmt(error, formatter),
            Self::IdentityMismatch(error) => Display::fmt(error, formatter),
            Self::TradeDedupCapacityExceeded { capacity } => write!(
                formatter,
                "Binance trade dedup capacity {capacity} exceeded"
            ),
        }
    }
}

impl Error for AdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidBookPayload(error) | Self::InvalidTradePayload(error) => Some(error),
            Self::InvalidPrice(error) | Self::InvalidQuantity(error) => Some(error),
            Self::InvalidSnapshot(error) => Some(error),
            Self::IdentityMismatch(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    struct Clock(Cell<u64>);
    impl ObservationClock for Clock {
        fn now(&self) -> LocalObservationTime {
            let value = self.0.get() + 1;
            self.0.set(value);
            LocalObservationTime::from_nanos_since_start(value)
        }
    }

    fn adapter() -> BinanceAdapter {
        BinanceAdapter::from_markets(
            vec![BinanceMarket::new(
                MarketCoin::try_new("BTC").unwrap(),
                "BTCUSDT".into(),
            )],
            NonZeroUsize::new(10).unwrap(),
        )
    }

    #[test]
    fn routes_partial_depth_and_aggregate_trade() {
        let mut adapter = adapter();
        assert_eq!(
            adapter.endpoint(),
            "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms/btcusdt@aggTrade"
        );
        let clock = Clock(Cell::new(1));
        let depth = r#"{"stream":"btcusdt@depth20@100ms","data":{"e":"depthUpdate","E":10,"s":"BTCUSDT","u":2,"b":[["100","1"]],"a":[["101","1"]]}}"#;
        assert!(matches!(
            adapter
                .on_text(
                    depth,
                    LocalObservationTime::from_nanos_since_start(1),
                    &clock
                )
                .as_slice(),
            [AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(_)
            )]
        ));
        let trade = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":10,"s":"BTCUSDT","a":91,"p":"100","q":"1","f":700,"l":703,"T":9,"m":false}}"#;
        assert!(matches!(
            adapter
                .on_text(
                    trade,
                    LocalObservationTime::from_nanos_since_start(2),
                    &clock
                )
                .as_slice(),
            [
                AdapterAction::Publish(NormalizedMarketEvent::TradeStreamResumed(_)),
                AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(_))
            ]
        ));
    }
}
