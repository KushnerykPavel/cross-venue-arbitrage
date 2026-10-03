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

const BINANCE_PUBLIC_WS_BASE_URL: &str = "wss://fstream.binance.com/public/stream?streams=";
const BINANCE_MARKET_WS_BASE_URL: &str = "wss://fstream.binance.com/market/stream?streams=";
const MAX_LEVELS_PER_SIDE: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BinanceFeed {
    Depth,
    Trades,
}

#[derive(Debug)]
pub struct BinanceAdapters {
    pub depth: BinanceAdapter,
    pub trades: BinanceAdapter,
}

#[derive(Debug)]
pub struct BinanceAdapter {
    markets: Vec<BinanceMarketState>,
    endpoint: String,
    feed: BinanceFeed,
}

impl BinanceAdapter {
    pub async fn bootstrap(
        market_coins: Vec<MarketCoin>,
        trade_dedup_capacity: NonZeroUsize,
    ) -> Result<BinanceAdapters, MetadataError> {
        let markets = resolve_markets(&market_coins).await?;
        Ok(BinanceAdapters {
            depth: Self::from_markets(markets.clone(), trade_dedup_capacity, BinanceFeed::Depth),
            trades: Self::from_markets(markets, trade_dedup_capacity, BinanceFeed::Trades),
        })
    }

    fn from_markets(
        markets: Vec<BinanceMarket>,
        trade_dedup_capacity: NonZeroUsize,
        feed: BinanceFeed,
    ) -> Self {
        let streams = markets
            .iter()
            .map(|market| {
                let symbol = market.symbol().to_lowercase();
                match feed {
                    BinanceFeed::Depth => format!("{symbol}@depth10@100ms"),
                    BinanceFeed::Trades => format!("{symbol}@aggTrade"),
                }
            })
            .collect::<Vec<_>>()
            .join("/");
        Self {
            markets: markets
                .into_iter()
                .map(|market| BinanceMarketState::new(market, trade_dedup_capacity))
                .collect(),
            endpoint: format!(
                "{}{streams}",
                match feed {
                    BinanceFeed::Depth => BINANCE_PUBLIC_WS_BASE_URL,
                    BinanceFeed::Trades => BINANCE_MARKET_WS_BASE_URL,
                }
            ),
            feed,
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

impl BinanceAdapters {
    pub fn resolved_markets(&self) -> Vec<(MarketCoin, String)> {
        self.depth.resolved_markets()
    }
}

#[cfg(feature = "dev-fixtures")]
impl BinanceAdapters {
    #[doc(hidden)]
    pub fn dev_fixture() -> Self {
        let market = BinanceMarket::new(
            MarketCoin::try_new("BTC").expect("static test market is valid"),
            "BTCUSDT".into(),
        );
        let capacity = NonZeroUsize::new(1_000_000).expect("fixture capacity is nonzero");
        Self {
            depth: BinanceAdapter::from_markets(vec![market.clone()], capacity, BinanceFeed::Depth),
            trades: BinanceAdapter::from_markets(vec![market], capacity, BinanceFeed::Trades),
        }
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
        let envelope = match serde_json::from_str::<WireEnvelope>(text) {
            Ok(envelope) => envelope,
            Err(error) => return invalid_message(format!("undecodable Binance frame: {error}")),
        };
        let Some(symbol) = envelope.data.get("s").and_then(Value::as_str) else {
            return invalid_message("Binance frame has no symbol".into());
        };
        let Some(index) = self
            .markets
            .iter()
            .position(|market| market.market().symbol() == symbol)
        else {
            return invalid_message(format!("Binance frame for unconfigured symbol {symbol}"));
        };
        match self.feed {
            BinanceFeed::Trades => {
                if envelope.data.get("e").and_then(Value::as_str) == Some("aggTrade") {
                    self.handle_trade(index, envelope.data, local_receive, clock)
                } else {
                    invalid_message("unexpected event type on Binance trade stream".into())
                }
            }
            BinanceFeed::Depth => {
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
                    Ok(false) => {
                        invalid_message("unexpected event type on Binance depth stream".into())
                    }
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
        }
    }

    fn on_disconnected(
        &mut self,
        reason: &str,
        observed_at: LocalObservationTime,
    ) -> Vec<AdapterAction> {
        self.markets
            .iter_mut()
            .map(|market| {
                let coin = market.market().market_coin().clone();
                match self.feed {
                    BinanceFeed::Depth => {
                        market.disconnected(BinanceFeed::Depth);
                        AdapterAction::Publish(NormalizedMarketEvent::OrderBookUnavailable(
                            MarketDataUnavailable::new(
                                Venue::Binance,
                                coin,
                                observed_at,
                                UnavailabilityCategory::Disconnected,
                                reason,
                            ),
                        ))
                    }
                    BinanceFeed::Trades => {
                        market.disconnected(BinanceFeed::Trades);
                        AdapterAction::Publish(NormalizedMarketEvent::TradeStreamUnavailable(
                            MarketDataUnavailable::new(
                                Venue::Binance,
                                coin,
                                observed_at,
                                UnavailabilityCategory::Disconnected,
                                reason,
                            ),
                        ))
                    }
                }
            })
            .collect()
    }

    fn market_coin(&self, market: MarketKey) -> Option<&MarketCoin> {
        self.markets
            .get(market.index())
            .map(|market| market.market().market_coin())
    }
}

fn invalid_message(reason: String) -> Vec<AdapterAction> {
    vec![AdapterAction::InvalidMessage { reason }]
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
struct BinanceMarketState {
    market: BinanceMarket,
    book: OrderBook,
    trade_dedup: BoundedDeduplicator<u64>,
    trade_available: bool,
}

impl BinanceMarketState {
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

    fn disconnected(&mut self, feed: BinanceFeed) {
        match feed {
            BinanceFeed::Depth => self.book.mark_unhealthy(),
            BinanceFeed::Trades => self.trade_available = false,
        }
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
    TooManyLevels { actual: usize, maximum: usize },
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
    #[serde(rename = "T")]
    transaction_time: Option<u64>,
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
    for side in [&wire.bids, &wire.asks] {
        if side.len() > MAX_LEVELS_PER_SIDE {
            return Err(AdapterError::TooManyLevels {
                actual: side.len(),
                maximum: MAX_LEVELS_PER_SIDE,
            });
        }
    }
    let mut exchange_times = vec![ExchangeTimeObservation::new(
        ExchangeTimeKind::EventTime,
        wire.event_time,
        ExchangeTimeUnit::Milliseconds,
    )];
    if let Some(transaction_time) = wire.transaction_time {
        exchange_times.push(ExchangeTimeObservation::new(
            ExchangeTimeKind::TransactionTime,
            transaction_time,
            ExchangeTimeUnit::Milliseconds,
        ));
    }
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
        EventTimestamps::new(exchange_times, local_receive, local_receive),
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
            Self::TooManyLevels { actual, maximum } => write!(
                formatter,
                "received {actual} Binance depth levels on one side, maximum is {maximum}"
            ),
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

    fn adapters() -> BinanceAdapters {
        let markets = vec![BinanceMarket::new(
            MarketCoin::try_new("BTC").unwrap(),
            "BTCUSDT".into(),
        )];
        let capacity = NonZeroUsize::new(10).unwrap();
        BinanceAdapters {
            depth: BinanceAdapter::from_markets(markets.clone(), capacity, BinanceFeed::Depth),
            trades: BinanceAdapter::from_markets(markets, capacity, BinanceFeed::Trades),
        }
    }

    #[test]
    fn routes_depth_and_aggregate_trades_to_their_endpoint_categories() {
        let mut adapters = adapters();
        assert_eq!(
            adapters.depth.endpoint(),
            "wss://fstream.binance.com/public/stream?streams=btcusdt@depth10@100ms"
        );
        assert_eq!(
            adapters.trades.endpoint(),
            "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade"
        );
        let clock = Clock(Cell::new(1));
        let depth = r#"{"stream":"btcusdt@depth10@100ms","data":{"e":"depthUpdate","E":10,"s":"BTCUSDT","u":2,"b":[["100","1"]],"a":[["101","1"]]}}"#;
        assert!(matches!(
            adapters
                .depth
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
            adapters
                .trades
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

    fn trade_side(adapter: &mut BinanceAdapter, id: u64, buyer_is_maker: bool) -> MarketTrade {
        let frame = format!(
            r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":10,"s":"BTCUSDT","a":{id},"p":"100","q":"1","f":700,"l":703,"T":9,"m":{buyer_is_maker}}}}}"#
        );
        let actions = adapter.on_text(
            &frame,
            LocalObservationTime::from_nanos_since_start(1),
            &Clock(Cell::new(1)),
        );
        match actions.last() {
            Some(AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(trade))) => {
                trade.clone()
            }
            _ => panic!("expected a Market Trade"),
        }
    }

    #[test]
    fn maps_buyer_maker_flag_to_aggressor_side_and_keeps_identity() {
        let mut trades = adapters().trades;
        let seller_initiated = trade_side(&mut trades, 91, true);
        assert_eq!(
            seller_initiated.aggressor_side(),
            market_data::AggressorSide::Sell
        );
        assert_eq!(
            seller_initiated.aggressor_side_classification(),
            market_data::AggressorSideClassification::DerivedFromMakerSide
        );
        assert_eq!(
            seller_initiated.identity(),
            &MarketTradeIdentity::Binance {
                aggregate_trade_id: 91,
                first_trade_id: 700,
                last_trade_id: 703,
            }
        );
        let buyer_initiated = trade_side(&mut trades, 92, false);
        assert_eq!(
            buyer_initiated.aggressor_side(),
            market_data::AggressorSide::Buy
        );
    }

    #[test]
    fn depth_preserves_transaction_time_and_rejects_more_than_ten_levels() {
        let mut depth = adapters().depth;
        let clock = Clock(Cell::new(1));
        let frame = r#"{"stream":"btcusdt@depth10@100ms","data":{"e":"depthUpdate","E":10,"T":9,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[["100","1"]],"a":[["101","1"]]}}"#;
        let actions = depth.on_text(
            frame,
            LocalObservationTime::from_nanos_since_start(1),
            &clock,
        );
        let [AdapterAction::Publish(NormalizedMarketEvent::OrderBookSnapshot(snapshot))] =
            actions.as_slice()
        else {
            panic!("expected one snapshot");
        };
        assert_eq!(
            snapshot.timestamps().exchange_times(),
            [
                ExchangeTimeObservation::new(
                    ExchangeTimeKind::EventTime,
                    10,
                    ExchangeTimeUnit::Milliseconds
                ),
                ExchangeTimeObservation::new(
                    ExchangeTimeKind::TransactionTime,
                    9,
                    ExchangeTimeUnit::Milliseconds
                ),
            ]
        );

        let bids = (0..11)
            .map(|level| format!(r#"["{}","1"]"#, 100 - level))
            .collect::<Vec<_>>()
            .join(",");
        let oversized = frame.replace(r#"[["100","1"]]"#, &format!("[{bids}]"));
        assert!(matches!(
            depth
                .on_text(
                    &oversized,
                    LocalObservationTime::from_nanos_since_start(2),
                    &clock
                )
                .as_slice(),
            [AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookUnavailable(_)
            )]
        ));
    }

    #[test]
    fn reports_undecodable_and_unroutable_frames_as_invalid_messages() {
        let mut adapters = adapters();
        let clock = Clock(Cell::new(1));
        let now = LocalObservationTime::from_nanos_since_start(1);
        for frame in [
            "{not json",
            r#"{"stream":"x","data":{"e":"depthUpdate"}}"#,
            r#"{"stream":"x","data":{"e":"depthUpdate","s":"ETHUSDT"}}"#,
        ] {
            assert!(matches!(
                adapters.depth.on_text(frame, now, &clock).as_slice(),
                [AdapterAction::InvalidMessage { .. }]
            ));
        }
        assert!(matches!(
            adapters
                .trades
                .on_text(
                    r#"{"stream":"x","data":{"e":"trade","s":"BTCUSDT"}}"#,
                    now,
                    &clock
                )
                .as_slice(),
            [AdapterAction::InvalidMessage { .. }]
        ));
    }
}
