use domain::{MarketCoin, Price, Quantity, Symbol, Venue};

use crate::{BookLevel, OrderBookSnapshot};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LocalObservationTime(u64);

impl LocalObservationTime {
    pub const fn from_nanos_since_start(value: u64) -> Self {
        Self(value)
    }

    pub const fn nanos_since_start(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeTimeKind {
    EventTime,
    TradeTime,
    BlockTime,
    TransactionTime,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchangeTimeUnit {
    Milliseconds,
    Microseconds,
    Nanoseconds,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExchangeTimeObservation {
    kind: ExchangeTimeKind,
    raw_value: u64,
    declared_unit: ExchangeTimeUnit,
}

impl ExchangeTimeObservation {
    pub const fn new(
        kind: ExchangeTimeKind,
        raw_value: u64,
        declared_unit: ExchangeTimeUnit,
    ) -> Self {
        Self {
            kind,
            raw_value,
            declared_unit,
        }
    }

    pub const fn kind(self) -> ExchangeTimeKind {
        self.kind
    }

    pub const fn raw_value(self) -> u64 {
        self.raw_value
    }

    pub const fn declared_unit(self) -> ExchangeTimeUnit {
        self.declared_unit
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTimestamps {
    exchange_times: Vec<ExchangeTimeObservation>,
    local_receive: LocalObservationTime,
    processing_completed: LocalObservationTime,
}

impl EventTimestamps {
    pub fn new(
        exchange_times: Vec<ExchangeTimeObservation>,
        local_receive: LocalObservationTime,
        processing_completed: LocalObservationTime,
    ) -> Self {
        Self {
            exchange_times,
            local_receive,
            processing_completed,
        }
    }

    pub fn exchange_times(&self) -> &[ExchangeTimeObservation] {
        &self.exchange_times
    }

    pub const fn local_receive(&self) -> LocalObservationTime {
        self.local_receive
    }

    pub const fn processing_completed(&self) -> LocalObservationTime {
        self.processing_completed
    }

    pub(crate) fn set_processing_completed(&mut self, value: LocalObservationTime) {
        self.processing_completed = value;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketTradeReportingKind {
    Individual,
    TakerOrderAggregate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketTradeKind {
    Regular,
    Liquidation,
    Deleverage,
    MarketSettlement,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggressorSideClassification {
    VenueProvided,
    DerivedFromMakerSide,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketTradeIdentity {
    Aster {
        aggregate_trade_id: u64,
        first_trade_id: u64,
        last_trade_id: u64,
    },
    Hyperliquid {
        block_time: u64,
        trade_id: u64,
        transaction_hash: String,
    },
    Lighter {
        market_id: u32,
        trade_id: String,
        message_nonce: Option<u64>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketTrade {
    venue: Venue,
    symbol: Symbol,
    timestamps: EventTimestamps,
    price: Price,
    quantity: Quantity,
    reporting_kind: MarketTradeReportingKind,
    trade_kind: MarketTradeKind,
    aggressor_side: AggressorSide,
    aggressor_side_classification: AggressorSideClassification,
    identity: MarketTradeIdentity,
}

impl MarketTrade {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        venue: Venue,
        symbol: Symbol,
        timestamps: EventTimestamps,
        price: Price,
        quantity: Quantity,
        reporting_kind: MarketTradeReportingKind,
        trade_kind: MarketTradeKind,
        aggressor_side: AggressorSide,
        aggressor_side_classification: AggressorSideClassification,
        identity: MarketTradeIdentity,
    ) -> Self {
        Self {
            venue,
            symbol,
            timestamps,
            price,
            quantity,
            reporting_kind,
            trade_kind,
            aggressor_side,
            aggressor_side_classification,
            identity,
        }
    }

    pub const fn venue(&self) -> Venue {
        self.venue
    }

    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub const fn timestamps(&self) -> &EventTimestamps {
        &self.timestamps
    }

    pub const fn price(&self) -> Price {
        self.price
    }

    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }

    pub const fn reporting_kind(&self) -> MarketTradeReportingKind {
        self.reporting_kind
    }

    pub const fn trade_kind(&self) -> MarketTradeKind {
        self.trade_kind
    }

    pub const fn aggressor_side(&self) -> AggressorSide {
        self.aggressor_side
    }

    pub const fn aggressor_side_classification(&self) -> AggressorSideClassification {
        self.aggressor_side_classification
    }

    pub const fn identity(&self) -> &MarketTradeIdentity {
        &self.identity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailabilityCategory {
    Disconnected,
    InvalidMarketData,
    SequenceGap,
    RecorderShutdown,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataUnavailable {
    venue: Venue,
    market_coin: MarketCoin,
    observed_at: LocalObservationTime,
    category: UnavailabilityCategory,
    diagnostic: String,
}

impl MarketDataUnavailable {
    pub fn new(
        venue: Venue,
        market_coin: MarketCoin,
        observed_at: LocalObservationTime,
        category: UnavailabilityCategory,
        diagnostic: impl Into<String>,
    ) -> Self {
        Self {
            venue,
            market_coin,
            observed_at,
            category,
            diagnostic: diagnostic.into(),
        }
    }

    pub const fn venue(&self) -> Venue {
        self.venue
    }

    pub const fn market_coin(&self) -> &MarketCoin {
        &self.market_coin
    }

    pub const fn observed_at(&self) -> LocalObservationTime {
        self.observed_at
    }

    pub const fn category(&self) -> UnavailabilityCategory {
        self.category
    }

    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TradeStreamResumed {
    venue: Venue,
    market_coin: MarketCoin,
    observed_at: LocalObservationTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BestBidOffer {
    venue: Venue,
    symbol: Symbol,
    timestamps: EventTimestamps,
    bid: Option<BookLevel>,
    ask: Option<BookLevel>,
}

impl BestBidOffer {
    pub fn new(
        venue: Venue,
        symbol: Symbol,
        timestamps: EventTimestamps,
        bid: Option<BookLevel>,
        ask: Option<BookLevel>,
    ) -> Self {
        Self {
            venue,
            symbol,
            timestamps,
            bid,
            ask,
        }
    }

    pub const fn venue(&self) -> Venue {
        self.venue
    }
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }
    pub const fn timestamps(&self) -> &EventTimestamps {
        &self.timestamps
    }
    pub const fn bid(&self) -> Option<&BookLevel> {
        self.bid.as_ref()
    }
    pub const fn ask(&self) -> Option<&BookLevel> {
        self.ask.as_ref()
    }
}

impl TradeStreamResumed {
    pub const fn new(
        venue: Venue,
        market_coin: MarketCoin,
        observed_at: LocalObservationTime,
    ) -> Self {
        Self {
            venue,
            market_coin,
            observed_at,
        }
    }

    pub const fn venue(&self) -> Venue {
        self.venue
    }

    pub const fn market_coin(&self) -> &MarketCoin {
        &self.market_coin
    }

    pub const fn observed_at(&self) -> LocalObservationTime {
        self.observed_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NormalizedMarketEvent {
    OrderBookSnapshot(OrderBookSnapshot),
    BestBidOfferUpdated(BestBidOffer),
    OrderBookUnavailable(MarketDataUnavailable),
    MarketTrade(MarketTrade),
    TradeStreamUnavailable(MarketDataUnavailable),
    TradeStreamResumed(TradeStreamResumed),
}

impl NormalizedMarketEvent {
    pub const fn venue(&self) -> Venue {
        match self {
            Self::OrderBookSnapshot(snapshot) => snapshot.venue(),
            Self::BestBidOfferUpdated(bbo) => bbo.venue(),
            Self::OrderBookUnavailable(event) | Self::TradeStreamUnavailable(event) => {
                event.venue()
            }
            Self::MarketTrade(trade) => trade.venue(),
            Self::TradeStreamResumed(event) => event.venue(),
        }
    }

    pub const fn market_coin(&self) -> &MarketCoin {
        match self {
            Self::OrderBookSnapshot(snapshot) => snapshot.symbol().market_coin(),
            Self::BestBidOfferUpdated(bbo) => bbo.symbol().market_coin(),
            Self::OrderBookUnavailable(event) | Self::TradeStreamUnavailable(event) => {
                event.market_coin()
            }
            Self::MarketTrade(trade) => trade.symbol().market_coin(),
            Self::TradeStreamResumed(event) => event.market_coin(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_unknown_exchange_time_without_conversion() {
        let observation = ExchangeTimeObservation::new(
            ExchangeTimeKind::TransactionTime,
            1_234_567,
            ExchangeTimeUnit::Unknown,
        );
        let timestamps = EventTimestamps::new(
            vec![observation],
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(20),
        );

        assert_eq!(timestamps.exchange_times(), [observation]);
        assert_eq!(
            timestamps.exchange_times()[0].declared_unit(),
            ExchangeTimeUnit::Unknown
        );
    }
}
