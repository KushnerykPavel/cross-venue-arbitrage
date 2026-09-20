use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;

use domain::{DecimalParseError, MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    BookLevel, EventTimestamps, ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit,
    LocalObservationTime, MarketDataUnavailable, NormalizedMarketEvent, OrderBook,
    OrderBookIdentityError, OrderBookSnapshot, SnapshotValidationError, TradeStreamResumed,
    UnavailabilityCategory,
};
use serde::Deserialize;
use venue::{
    AdapterAction, BoundedDeduplicator, DeduplicationResult, MarketDataAdapter, MarketKey,
    ObservationClock,
};

use crate::metadata::{LighterMarket, MetadataError, resolve_markets};
use crate::trade::{WireTrade, decode_trade};

const LIGHTER_WS_URL: &str = "wss://mainnet.zklighter.elliot.ai/stream";
const PONG: &str = r#"{"type":"pong"}"#;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub struct LighterAdapter {
    markets: Vec<LighterOrderBookAdapter>,
}

impl LighterAdapter {
    pub async fn bootstrap(
        market_coins: Vec<MarketCoin>,
        trade_dedup_capacity: NonZeroUsize,
    ) -> Result<Self, MetadataError> {
        Ok(Self::from_markets(
            resolve_markets(&market_coins).await?,
            trade_dedup_capacity,
        ))
    }

    fn from_markets(markets: Vec<LighterMarket>, trade_dedup_capacity: NonZeroUsize) -> Self {
        Self {
            markets: markets
                .into_iter()
                .map(|market| LighterOrderBookAdapter::new(market, trade_dedup_capacity))
                .collect(),
        }
    }

    pub fn resolved_markets(&self) -> Vec<(MarketCoin, u32)> {
        self.markets
            .iter()
            .map(|market| {
                (
                    market.market().market_coin().clone(),
                    market.market().market_id(),
                )
            })
            .collect()
    }
}

impl MarketDataAdapter for LighterAdapter {
    fn venue(&self) -> Venue {
        Venue::Lighter
    }

    fn endpoint(&self) -> &str {
        LIGHTER_WS_URL
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(HEARTBEAT_INTERVAL)
    }

    fn on_heartbeat(&mut self) -> Vec<AdapterAction> {
        vec![AdapterAction::SendPing(Vec::new())]
    }

    fn on_connected(&mut self) -> Vec<AdapterAction> {
        Vec::new()
    }

    fn on_text(
        &mut self,
        text: &str,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Vec<AdapterAction> {
        let Ok(envelope) = decode_envelope(text) else {
            return Vec::new();
        };
        match envelope.kind.as_str() {
            "connected" => {
                return self
                    .markets
                    .iter()
                    .enumerate()
                    .flat_map(|(index, market)| {
                        [
                            AdapterAction::SendText(market.subscription_message()),
                            AdapterAction::SendText(market.trade_subscription_message()),
                            AdapterAction::MarketSubscribed(MarketKey::new(index)),
                        ]
                    })
                    .collect();
            }
            "ping" => return vec![AdapterAction::SendText(PONG.into())],
            "subscribed/trade" | "update/trade" => {
                return self.handle_trades(envelope, local_receive, clock);
            }
            "subscribed/order_book" | "update/order_book" => {}
            _ => return Vec::new(),
        }
        let Ok(market_id) = market_id_from_channel(&envelope.channel) else {
            return Vec::new();
        };
        let Some(index) = self
            .markets
            .iter()
            .position(|market| market.market().market_id() == market_id)
        else {
            return Vec::new();
        };
        let key = MarketKey::new(index);
        match self.markets[index].apply_envelope(envelope, local_receive, clock) {
            Ok(HandleOutcome::Applied) => vec![AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(
                    self.markets[index]
                        .book()
                        .current()
                        .expect("accepted update installs a snapshot")
                        .clone(),
                ),
            )],
            Ok(HandleOutcome::Ignored) => Vec::new(),
            Err(error) => {
                let recovery_required = !matches!(error, AdapterError::UpdateBeforeSnapshot);
                let category = if matches!(error, AdapterError::NonceGap { .. }) {
                    UnavailabilityCategory::SequenceGap
                } else {
                    UnavailabilityCategory::InvalidMarketData
                };
                let mut actions = vec![AdapterAction::Publish(
                    NormalizedMarketEvent::OrderBookUnavailable(MarketDataUnavailable::new(
                        Venue::Lighter,
                        self.markets[index].market().market_coin().clone(),
                        local_receive,
                        category,
                        error.to_string(),
                    )),
                )];
                if recovery_required {
                    actions.push(AdapterAction::SendText(
                        self.markets[index].unsubscription_message(),
                    ));
                    actions.push(AdapterAction::SendText(
                        self.markets[index].subscription_message(),
                    ));
                    actions.push(AdapterAction::MarketSubscribed(key));
                }
                actions
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
            .flat_map(|market| {
                market.disconnected();
                let coin = market.market().market_coin().clone();
                [
                    AdapterAction::Publish(NormalizedMarketEvent::OrderBookUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Lighter,
                            coin.clone(),
                            observed_at,
                            UnavailabilityCategory::Disconnected,
                            reason,
                        ),
                    )),
                    AdapterAction::Publish(NormalizedMarketEvent::TradeStreamUnavailable(
                        MarketDataUnavailable::new(
                            Venue::Lighter,
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

impl LighterAdapter {
    fn handle_trades(
        &mut self,
        envelope: WireEnvelope,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Vec<AdapterAction> {
        let Ok(market_id) = market_id_from_trade_channel(&envelope.channel) else {
            return Vec::new();
        };
        let Some(index) = self
            .markets
            .iter()
            .position(|market| market.market().market_id() == market_id)
        else {
            return Vec::new();
        };
        let mut actions = Vec::new();
        for wire in envelope
            .trades
            .into_iter()
            .chain(envelope.liquidation_trades)
        {
            debug_assert_eq!(wire.market_id(), market_id);
            let market_coin = self.markets[index].market().market_coin().clone();
            match self.markets[index].apply_trade(wire, envelope.nonce, local_receive, clock) {
                Ok(TradeApplied::New { trade, resumed }) => {
                    if resumed {
                        actions.push(AdapterAction::Publish(
                            NormalizedMarketEvent::TradeStreamResumed(TradeStreamResumed::new(
                                Venue::Lighter,
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
                            "Lighter {market_coin} trade dedup capacity {capacity} exceeded"
                        ),
                    });
                    break;
                }
                Err(error) => {
                    self.markets[index].mark_trade_unavailable();
                    actions.push(AdapterAction::Publish(
                        NormalizedMarketEvent::TradeStreamUnavailable(MarketDataUnavailable::new(
                            Venue::Lighter,
                            market_coin,
                            local_receive,
                            UnavailabilityCategory::InvalidMarketData,
                            error.to_string(),
                        )),
                    ));
                }
            }
        }
        actions
    }
}

#[derive(Debug)]
struct LighterOrderBookAdapter {
    market: LighterMarket,
    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,
    nonce: Option<u64>,
    book: OrderBook,
    trade_dedup: BoundedDeduplicator<(u32, String)>,
    trade_available: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HandleOutcome {
    Applied,
    Ignored,
}

#[derive(Debug)]
enum AdapterError {
    InvalidJson(serde_json::Error),
    MissingBook,
    InvalidChannel(String),
    UnexpectedMarketId {
        expected: u32,
        actual: u32,
    },
    UpdateBeforeSnapshot,
    NonceGap {
        expected: u64,
        actual: u64,
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
    DuplicatePrice {
        side: BookSide,
        price: Price,
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

impl LighterOrderBookAdapter {
    fn new(market: LighterMarket, trade_dedup_capacity: NonZeroUsize) -> Self {
        let symbol = Symbol::perpetual(market.market_coin().clone());
        Self {
            market,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            nonce: None,
            book: OrderBook::new(Venue::Lighter, symbol),
            trade_dedup: BoundedDeduplicator::new(trade_dedup_capacity),
            trade_available: false,
        }
    }

    fn subscription_message(&self) -> String {
        command_message("subscribe", self.market.market_id())
    }

    fn unsubscription_message(&self) -> String {
        command_message("unsubscribe", self.market.market_id())
    }

    fn trade_subscription_message(&self) -> String {
        trade_command_message("subscribe", self.market.market_id())
    }

    #[cfg(test)]
    fn handle_message(
        &mut self,
        message: &str,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<HandleOutcome, AdapterError> {
        let envelope: WireEnvelope = match serde_json::from_str(message) {
            Ok(envelope) => envelope,
            Err(error) => return self.reject(AdapterError::InvalidJson(error)),
        };
        self.apply_envelope(envelope, local_receive, clock)
    }

    fn disconnected(&mut self) {
        self.nonce = None;
        self.book.mark_unhealthy();
        self.trade_available = false;
    }

    fn mark_trade_unavailable(&mut self) {
        self.trade_available = false;
    }

    fn apply_trade(
        &mut self,
        wire: WireTrade,
        message_nonce: Option<u64>,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<TradeApplied, AdapterError> {
        let trade = decode_trade(
            wire,
            self.market.market_id(),
            self.market.market_coin(),
            message_nonce,
            local_receive,
            clock.now(),
        )
        .map_err(AdapterError::InvalidTrade)?;
        let market_data::MarketTradeIdentity::Lighter {
            market_id,
            trade_id,
            ..
        } = trade.identity()
        else {
            unreachable!("Lighter decoder returns Lighter identity")
        };
        match self.trade_dedup.observe((*market_id, trade_id.clone())) {
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

    const fn market(&self) -> &LighterMarket {
        &self.market
    }

    const fn book(&self) -> &OrderBook {
        &self.book
    }

    #[cfg(test)]
    const fn nonce(&self) -> Option<u64> {
        self.nonce
    }

    fn apply_envelope(
        &mut self,
        envelope: WireEnvelope,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<HandleOutcome, AdapterError> {
        let is_snapshot = match envelope.kind.as_str() {
            "subscribed/order_book" => true,
            "update/order_book" => false,
            _ => return Ok(HandleOutcome::Ignored),
        };

        let market_id = match market_id_from_channel(&envelope.channel) {
            Ok(market_id) => market_id,
            Err(error) => return self.reject(error),
        };
        if market_id != self.market.market_id() {
            return self.reject(AdapterError::UnexpectedMarketId {
                expected: self.market.market_id(),
                actual: market_id,
            });
        }
        let Some(wire_book) = envelope.order_book else {
            return self.reject(AdapterError::MissingBook);
        };

        let result = if is_snapshot {
            self.apply_snapshot(wire_book, local_receive, clock)
        } else {
            self.apply_update(wire_book, local_receive, clock)
        };
        if let Err(error) = result {
            return self.reject(error);
        }
        Ok(HandleOutcome::Applied)
    }

    fn apply_snapshot(
        &mut self,
        wire_book: WireBook,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<(), AdapterError> {
        let bids = decode_snapshot_side(wire_book.bids, BookSide::Bid)?;
        let asks = decode_snapshot_side(wire_book.asks, BookSide::Ask)?;
        let snapshot = normalized_snapshot(
            self.market.market_coin(),
            &bids,
            &asks,
            wire_book.nonce,
            wire_book.last_updated_at,
            local_receive,
        )?;
        self.bids = bids;
        self.asks = asks;
        self.nonce = Some(wire_book.nonce);
        self.book
            .replace_at_processing_completion(snapshot, || clock.now())
            .map_err(AdapterError::IdentityMismatch)?;
        Ok(())
    }

    fn apply_update(
        &mut self,
        wire_book: WireBook,
        local_receive: LocalObservationTime,
        clock: &dyn ObservationClock,
    ) -> Result<(), AdapterError> {
        let current_nonce = self.nonce.ok_or(AdapterError::UpdateBeforeSnapshot)?;
        if wire_book.begin_nonce != current_nonce {
            return Err(AdapterError::NonceGap {
                expected: current_nonce,
                actual: wire_book.begin_nonce,
            });
        }

        let bid_changes = decode_changes(wire_book.bids, BookSide::Bid)?;
        let ask_changes = decode_changes(wire_book.asks, BookSide::Ask)?;
        apply_changes(&mut self.bids, bid_changes);
        apply_changes(&mut self.asks, ask_changes);

        let snapshot = normalized_snapshot(
            self.market.market_coin(),
            &self.bids,
            &self.asks,
            wire_book.nonce,
            wire_book.last_updated_at,
            local_receive,
        )?;
        self.nonce = Some(wire_book.nonce);
        self.book
            .replace_at_processing_completion(snapshot, || clock.now())
            .map_err(AdapterError::IdentityMismatch)?;
        Ok(())
    }

    fn reject(&mut self, error: AdapterError) -> Result<HandleOutcome, AdapterError> {
        self.nonce = None;
        self.book.mark_unhealthy();
        Err(error)
    }
}

enum TradeApplied {
    New {
        trade: market_data::MarketTrade,
        resumed: bool,
    },
    Duplicate,
}

fn decode_envelope(message: &str) -> Result<WireEnvelope, AdapterError> {
    serde_json::from_str(message).map_err(AdapterError::InvalidJson)
}

fn market_id_from_channel(channel: &str) -> Result<u32, AdapterError> {
    channel
        .strip_prefix("order_book:")
        .ok_or_else(|| AdapterError::InvalidChannel(channel.into()))?
        .parse()
        .map_err(|_| AdapterError::InvalidChannel(channel.into()))
}

fn market_id_from_trade_channel(channel: &str) -> Result<u32, AdapterError> {
    channel
        .strip_prefix("trade:")
        .ok_or_else(|| AdapterError::InvalidChannel(channel.into()))?
        .parse()
        .map_err(|_| AdapterError::InvalidChannel(channel.into()))
}

fn command_message(command: &str, market_id: u32) -> String {
    serde_json::json!({
        "type": command,
        "channel": format!("order_book/{market_id}")
    })
    .to_string()
}

fn trade_command_message(command: &str, market_id: u32) -> String {
    serde_json::json!({
        "type": command,
        "channel": format!("trade/{market_id}")
    })
    .to_string()
}

fn decode_snapshot_side(
    levels: Vec<WireLevel>,
    side: BookSide,
) -> Result<BTreeMap<Price, Quantity>, AdapterError> {
    let mut decoded = BTreeMap::new();
    for (level, wire) in levels.into_iter().enumerate() {
        let price = decode_price(&wire.price, side, level)?;
        let quantity =
            Quantity::from_str(&wire.size).map_err(|source| AdapterError::InvalidQuantity {
                side,
                level,
                source,
            })?;
        if decoded.insert(price, quantity).is_some() {
            return Err(AdapterError::DuplicatePrice { side, price });
        }
    }
    Ok(decoded)
}

fn decode_changes(
    levels: Vec<WireLevel>,
    side: BookSide,
) -> Result<Vec<(Price, Option<Quantity>)>, AdapterError> {
    let mut decoded = Vec::with_capacity(levels.len());
    let mut seen = std::collections::BTreeSet::new();
    for (level, wire) in levels.into_iter().enumerate() {
        let price = decode_price(&wire.price, side, level)?;
        if !seen.insert(price) {
            return Err(AdapterError::DuplicatePrice { side, price });
        }
        let quantity = match Quantity::from_str(&wire.size) {
            Ok(quantity) => Some(quantity),
            Err(DecimalParseError::NonPositive) if is_zero_decimal(&wire.size) => None,
            Err(source) => {
                return Err(AdapterError::InvalidQuantity {
                    side,
                    level,
                    source,
                });
            }
        };
        decoded.push((price, quantity));
    }
    Ok(decoded)
}

fn decode_price(value: &str, side: BookSide, level: usize) -> Result<Price, AdapterError> {
    Price::from_str(value).map_err(|source| AdapterError::InvalidPrice {
        side,
        level,
        source,
    })
}

fn is_zero_decimal(value: &str) -> bool {
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fractional = parts.next();
    if parts.next().is_some() || whole.is_empty() || !whole.bytes().all(|byte| byte == b'0') {
        return false;
    }
    match fractional {
        Some(fractional) => !fractional.is_empty() && fractional.bytes().all(|byte| byte == b'0'),
        None => true,
    }
}

fn apply_changes(levels: &mut BTreeMap<Price, Quantity>, changes: Vec<(Price, Option<Quantity>)>) {
    for (price, quantity) in changes {
        match quantity {
            Some(quantity) => {
                levels.insert(price, quantity);
            }
            None => {
                levels.remove(&price);
            }
        }
    }
}

fn normalized_snapshot(
    market_coin: &MarketCoin,
    bids: &BTreeMap<Price, Quantity>,
    asks: &BTreeMap<Price, Quantity>,
    nonce: u64,
    exchange_raw: u64,
    local_receive: LocalObservationTime,
) -> Result<OrderBookSnapshot, AdapterError> {
    let bids = bids
        .iter()
        .map(|(price, quantity)| BookLevel::new(*price, *quantity, None))
        .collect();
    let asks = asks
        .iter()
        .map(|(price, quantity)| BookLevel::new(*price, *quantity, None))
        .collect();
    OrderBookSnapshot::try_new(
        Venue::Lighter,
        Symbol::perpetual(market_coin.clone()),
        Some(nonce),
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::EventTime,
                exchange_raw,
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

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(error) => write!(formatter, "invalid WebSocket JSON: {error}"),
            Self::MissingBook => formatter.write_str("order-book message has no book payload"),
            Self::InvalidChannel(channel) => {
                write!(formatter, "invalid order-book channel {channel}")
            }
            Self::UnexpectedMarketId { expected, actual } => {
                write!(
                    formatter,
                    "expected market ID {expected}, received {actual}"
                )
            }
            Self::UpdateBeforeSnapshot => formatter.write_str("update arrived before a snapshot"),
            Self::NonceGap { expected, actual } => {
                write!(
                    formatter,
                    "nonce gap: expected begin_nonce {expected}, received {actual}"
                )
            }
            Self::InvalidPrice {
                side,
                level,
                source,
            } => {
                write!(formatter, "invalid {side} price at level {level}: {source}")
            }
            Self::InvalidQuantity {
                side,
                level,
                source,
            } => {
                write!(
                    formatter,
                    "invalid {side} quantity at level {level}: {source}"
                )
            }
            Self::DuplicatePrice { side, price } => {
                write!(formatter, "duplicate {side} price {price}")
            }
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
            Self::InvalidJson(error) => Some(error),
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

#[derive(Deserialize)]
struct WireEnvelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    channel: String,
    order_book: Option<WireBook>,
    #[serde(default)]
    trades: Vec<WireTrade>,
    #[serde(default)]
    liquidation_trades: Vec<WireTrade>,
    nonce: Option<u64>,
}

#[derive(Deserialize)]
struct WireBook {
    asks: Vec<WireLevel>,
    bids: Vec<WireLevel>,
    nonce: u64,
    begin_nonce: u64,
    last_updated_at: u64,
}

#[derive(Deserialize)]
struct WireLevel {
    price: String,
    size: String,
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use domain::MarketCoin;
    use market_data::{LocalObservationTime, NormalizedMarketEvent, OrderBookStatus};
    use venue::{AdapterAction, MarketDataAdapter, MarketKey, ObservationClock};

    use super::{AdapterError, HandleOutcome, LighterAdapter, LighterOrderBookAdapter};
    use crate::metadata::LighterMarket;

    struct Clock(Cell<u64>);

    impl ObservationClock for Clock {
        fn now(&self) -> LocalObservationTime {
            let next = self.0.get() + 1;
            self.0.set(next);
            LocalObservationTime::from_nanos_since_start(next)
        }
    }

    fn received(value: u64) -> LocalObservationTime {
        LocalObservationTime::from_nanos_since_start(value)
    }

    const SNAPSHOT: &str = r#"{
        "type":"subscribed/order_book","channel":"order_book:1",
        "order_book":{"asks":[{"price":"102","size":"2"}],
        "bids":[{"price":"100","size":"1"},{"price":"99","size":"4"}],"nonce":10,
        "begin_nonce":0,"last_updated_at":1000}}"#;
    const TRADES: &str = r#"{
        "type":"update/trade","channel":"trade:1","nonce":88,
        "trades":[{"trade_id_str":"1","type":"trade","market_id":1,"size":"1","price":"100","is_maker_ask":true,"timestamp":10,"transaction_time":9}],
        "liquidation_trades":[{"trade_id_str":"2","type":"liquidation","market_id":1,"size":"2","price":"99","is_maker_ask":false,"timestamp":11,"transaction_time":10}]}
    "#;

    fn adapter() -> LighterOrderBookAdapter {
        LighterOrderBookAdapter::new(
            LighterMarket::new(MarketCoin::try_new("BTC").unwrap(), 1),
            NonZeroUsize::new(10).unwrap(),
        )
    }

    fn whole_adapter() -> LighterAdapter {
        LighterAdapter::from_markets(
            vec![
                LighterMarket::new(MarketCoin::try_new("BTC").unwrap(), 1),
                LighterMarket::new(MarketCoin::try_new("ETH").unwrap(), 0),
            ],
            NonZeroUsize::new(10).unwrap(),
        )
    }

    fn update(begin_nonce: u64, nonce: u64, bid_size: &str) -> String {
        format!(
            r#"{{"type":"update/order_book","channel":"order_book:1","order_book":{{"asks":[{{"price":"101","size":"3"}}],"bids":[{{"price":"100","size":"{bid_size}"}}],"nonce":{nonce},"begin_nonce":{begin_nonce},"last_updated_at":2000}}}}"#
        )
    }

    #[test]
    fn builds_snapshot_and_preserves_nonce_without_order_counts() {
        let mut adapter = adapter();
        let clock = Clock(Cell::new(20));
        assert_eq!(
            adapter
                .handle_message(SNAPSHOT, received(20), &clock)
                .unwrap(),
            HandleOutcome::Applied
        );
        let snapshot = adapter.book().current().unwrap();
        assert_eq!(snapshot.source_sequence(), Some(10));
        assert_eq!(snapshot.bids()[0].order_count(), None);
        assert_eq!(snapshot.timestamps().exchange_times()[0].raw_value(), 1000);
    }

    #[test]
    fn applies_delta_and_zero_size_deletion() {
        let mut adapter = adapter();
        let clock = Clock(Cell::new(20));
        adapter
            .handle_message(SNAPSHOT, received(20), &clock)
            .unwrap();
        adapter
            .handle_message(&update(10, 11, "0.000"), received(40), &clock)
            .unwrap();
        let snapshot = adapter.book().current().unwrap();
        assert_eq!(snapshot.source_sequence(), Some(11));
        assert_eq!(snapshot.bids()[0].price().to_string(), "99");
    }

    #[test]
    fn nonce_gap_invalidates_book_until_fresh_snapshot() {
        let mut adapter = adapter();
        let clock = Clock(Cell::new(20));
        adapter
            .handle_message(SNAPSHOT, received(20), &clock)
            .unwrap();
        assert!(matches!(
            adapter.handle_message(&update(9, 11, "2"), received(40), &clock),
            Err(AdapterError::NonceGap { .. })
        ));
        assert_eq!(adapter.book().status(), OrderBookStatus::Unhealthy);
        assert_eq!(adapter.nonce(), None);
        adapter
            .handle_message(SNAPSHOT, received(60), &clock)
            .unwrap();
        assert!(adapter.book().current().is_some());
    }

    #[test]
    fn whole_market_adapter_routes_and_orders_recovery_actions() {
        let mut adapter = whole_adapter();
        let clock = Clock(Cell::new(20));
        assert!(adapter.on_connected().is_empty());
        assert_eq!(
            adapter
                .on_text(r#"{"type":"connected"}"#, received(1), &clock)
                .len(),
            6
        );
        assert!(matches!(
            adapter.on_text(SNAPSHOT, received(20), &clock).as_slice(),
            [AdapterAction::Publish(
                NormalizedMarketEvent::OrderBookSnapshot(_)
            )]
        ));
        let actions = adapter.on_text(&update(9, 11, "2"), received(40), &clock);
        assert!(matches!(
            actions.as_slice(),
            [
                AdapterAction::Publish(NormalizedMarketEvent::OrderBookUnavailable(_)),
                AdapterAction::SendText(_),
                AdapterAction::SendText(_),
                AdapterAction::MarketSubscribed(subscribed)
            ] if *subscribed == MarketKey::new(0)
        ));
        assert!(adapter.markets[1].book().current().is_none());
    }

    #[test]
    fn sends_standard_websocket_heartbeat() {
        let mut adapter = whole_adapter();

        assert_eq!(adapter.heartbeat_interval(), Some(Duration::from_secs(15)));
        assert_eq!(
            adapter.on_heartbeat(),
            vec![AdapterAction::SendPing(Vec::new())]
        );
    }

    #[test]
    fn preserves_regular_then_liquidation_array_order() {
        let mut adapter = whole_adapter();
        let actions = adapter.on_text(TRADES, received(1), &Clock(Cell::new(1)));
        let kinds = actions
            .iter()
            .filter_map(|action| match action {
                AdapterAction::Publish(NormalizedMarketEvent::MarketTrade(trade)) => {
                    Some(trade.trade_kind())
                }
                AdapterAction::Publish(NormalizedMarketEvent::TradeStreamResumed(_)) => None,
                _ => panic!("unexpected action"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                market_data::MarketTradeKind::Regular,
                market_data::MarketTradeKind::Liquidation
            ]
        );
    }

    #[test]
    fn deduplicates_trade_arrays_and_stops_at_capacity() {
        let clock = Clock(Cell::new(1));
        let mut adapter = whole_adapter();
        adapter.on_text(TRADES, received(1), &clock);
        assert!(
            adapter
                .on_text(TRADES, received(2), &clock)
                .iter()
                .all(|action| matches!(action, AdapterAction::TradeDeduplicated(_)))
        );

        let mut full = LighterAdapter::from_markets(
            vec![LighterMarket::new(MarketCoin::try_new("BTC").unwrap(), 1)],
            NonZeroUsize::new(1).unwrap(),
        );
        let actions = full.on_text(TRADES, received(1), &clock);
        assert!(matches!(actions.last(), Some(AdapterAction::Stop { .. })));
    }
}
