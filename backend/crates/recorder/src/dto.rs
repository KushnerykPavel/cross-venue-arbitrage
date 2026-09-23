use std::error::Error;
use std::fmt::{self, Display, Formatter};

use std::num::NonZeroU32;
use std::str::FromStr;

use domain::{MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    AggressorSide, AggressorSideClassification, BestBidOffer, BookLevel, EventTimestamps,
    ExchangeTimeKind, ExchangeTimeObservation, ExchangeTimeUnit, LocalObservationTime,
    MarketDataUnavailable, MarketTrade, MarketTradeIdentity, MarketTradeKind,
    MarketTradeReportingKind, NormalizedMarketEvent, OrderBookSnapshot, TradeStreamResumed,
    UnavailabilityCategory,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StoredEventV1 {
    capture_sequence: u64,
    venue: StoredVenueV1,
    market_coin: String,
    local_receive_time: u64,
    processing_completion_time: u64,
    exchange_times: Vec<StoredExchangeTimeObservationV1>,
    source_id: Option<StoredSourceIdV1>,
    payload: StoredPayloadV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredVenueV1 {
    Aster,
    Hyperliquid,
    Lighter,
    Binance,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StoredExchangeTimeObservationV1 {
    pub kind: StoredExchangeTimeKindV1,
    pub raw_value: u64,
    pub declared_unit: StoredExchangeTimeUnitV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredExchangeTimeKindV1 {
    EventTime,
    TradeTime,
    BlockTime,
    TransactionTime,
    Other,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredExchangeTimeUnitV1 {
    Milliseconds,
    Microseconds,
    Nanoseconds,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredSourceIdV1 {
    OrderBookSequence(u64),
    AsterTrade {
        aggregate_trade_id: u64,
        first_trade_id: u64,
        last_trade_id: u64,
    },
    HyperliquidTrade {
        block_time: u64,
        trade_id: u64,
        transaction_hash: String,
    },
    LighterTrade {
        market_id: u32,
        trade_id_string: String,
        message_nonce: Option<u64>,
    },
    BinanceTrade {
        aggregate_trade_id: u64,
        first_trade_id: u64,
        last_trade_id: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredPayloadV1 {
    OrderBookSnapshot {
        bids: Vec<StoredBookLevelV1>,
        asks: Vec<StoredBookLevelV1>,
    },
    OrderBookUnavailable {
        category: StoredUnavailabilityCategoryV1,
        diagnostic: String,
    },
    MarketTrade {
        price: StoredDecimalV1,
        quantity: StoredDecimalV1,
        reporting_kind: StoredMarketTradeReportingKindV1,
        trade_kind: StoredMarketTradeKindV1,
        aggressor_side: StoredAggressorSideV1,
        aggressor_side_classification: StoredAggressorSideClassificationV1,
    },
    TradeStreamUnavailable {
        category: StoredUnavailabilityCategoryV1,
        diagnostic: String,
    },
    TradeStreamResumed,
    BestBidOfferUpdated {
        bid: Option<StoredBookLevelV1>,
        ask: Option<StoredBookLevelV1>,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StoredBookLevelV1 {
    pub price: StoredDecimalV1,
    pub quantity: StoredDecimalV1,
    pub order_count: Option<u32>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StoredDecimalV1 {
    pub coefficient: i128,
    pub scale: u8,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredMarketTradeReportingKindV1 {
    Individual,
    TakerOrderAggregate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredMarketTradeKindV1 {
    Regular,
    Liquidation,
    Deleverage,
    MarketSettlement,
    Other,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredAggressorSideV1 {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredAggressorSideClassificationV1 {
    VenueProvided,
    DerivedFromMakerSide,
    Unknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum StoredUnavailabilityCategoryV1 {
    Disconnected,
    InvalidMarketData,
    SequenceGap,
    RecorderShutdown,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageConversionError {
    ZeroCaptureSequence,
    TradeIdentityVenueMismatch,
    InvalidMarketCoin,
    InvalidPrice,
    InvalidQuantity,
    InvalidOrderCount,
    InvalidOrderBook,
    MissingOrderBookSequence,
    MissingTradeIdentity,
}

impl StoredEventV1 {
    pub const fn capture_sequence(&self) -> u64 {
        self.capture_sequence
    }

    pub const fn venue(&self) -> StoredVenueV1 {
        self.venue
    }

    pub fn market_coin(&self) -> &str {
        &self.market_coin
    }

    pub const fn local_receive_time(&self) -> u64 {
        self.local_receive_time
    }

    pub const fn processing_completion_time(&self) -> u64 {
        self.processing_completion_time
    }

    pub fn exchange_times(&self) -> &[StoredExchangeTimeObservationV1] {
        &self.exchange_times
    }

    pub const fn source_id(&self) -> Option<&StoredSourceIdV1> {
        self.source_id.as_ref()
    }

    pub const fn payload(&self) -> &StoredPayloadV1 {
        &self.payload
    }

    pub fn approximate_owned_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>()
            .saturating_add(self.market_coin.capacity())
            .saturating_add(
                self.exchange_times
                    .capacity()
                    .saturating_mul(std::mem::size_of::<StoredExchangeTimeObservationV1>()),
            );
        bytes = bytes.saturating_add(match &self.source_id {
            Some(StoredSourceIdV1::HyperliquidTrade {
                transaction_hash, ..
            }) => transaction_hash.capacity(),
            Some(StoredSourceIdV1::LighterTrade {
                trade_id_string, ..
            }) => trade_id_string.capacity(),
            _ => 0,
        });
        bytes.saturating_add(match &self.payload {
            StoredPayloadV1::OrderBookSnapshot { bids, asks } => bids
                .capacity()
                .saturating_add(asks.capacity())
                .saturating_mul(std::mem::size_of::<StoredBookLevelV1>()),
            StoredPayloadV1::BestBidOfferUpdated { .. } => 0,
            StoredPayloadV1::OrderBookUnavailable { diagnostic, .. }
            | StoredPayloadV1::TradeStreamUnavailable { diagnostic, .. } => diagnostic.capacity(),
            StoredPayloadV1::MarketTrade { .. } | StoredPayloadV1::TradeStreamResumed => 0,
        })
    }

    pub fn from_normalized(
        capture_sequence: u64,
        event: &NormalizedMarketEvent,
    ) -> Result<Self, StorageConversionError> {
        if capture_sequence == 0 {
            return Err(StorageConversionError::ZeroCaptureSequence);
        }
        let venue = event.venue().into();
        let market_coin = event.market_coin().as_str().to_owned();
        match event {
            NormalizedMarketEvent::OrderBookSnapshot(snapshot) => {
                let timestamps = snapshot.timestamps();
                Ok(Self {
                    capture_sequence,
                    venue,
                    market_coin,
                    local_receive_time: timestamps.local_receive().nanos_since_start(),
                    processing_completion_time: timestamps
                        .processing_completed()
                        .nanos_since_start(),
                    exchange_times: stored_exchange_times(timestamps),
                    source_id: snapshot
                        .source_sequence()
                        .map(StoredSourceIdV1::OrderBookSequence),
                    payload: StoredPayloadV1::OrderBookSnapshot {
                        bids: snapshot
                            .bids()
                            .iter()
                            .copied()
                            .map(StoredBookLevelV1::from)
                            .collect(),
                        asks: snapshot
                            .asks()
                            .iter()
                            .copied()
                            .map(StoredBookLevelV1::from)
                            .collect(),
                    },
                })
            }
            NormalizedMarketEvent::BestBidOfferUpdated(bbo) => {
                let timestamps = bbo.timestamps();
                Ok(Self {
                    capture_sequence,
                    venue,
                    market_coin,
                    local_receive_time: timestamps.local_receive().nanos_since_start(),
                    processing_completion_time: timestamps
                        .processing_completed()
                        .nanos_since_start(),
                    exchange_times: stored_exchange_times(timestamps),
                    source_id: None,
                    payload: StoredPayloadV1::BestBidOfferUpdated {
                        bid: bbo.bid().copied().map(StoredBookLevelV1::from),
                        ask: bbo.ask().copied().map(StoredBookLevelV1::from),
                    },
                })
            }
            NormalizedMarketEvent::OrderBookUnavailable(unavailable) => Ok(Self {
                capture_sequence,
                venue,
                market_coin,
                local_receive_time: unavailable.observed_at().nanos_since_start(),
                processing_completion_time: unavailable.observed_at().nanos_since_start(),
                exchange_times: Vec::new(),
                source_id: None,
                payload: StoredPayloadV1::OrderBookUnavailable {
                    category: unavailable.category().into(),
                    diagnostic: unavailable.diagnostic().into(),
                },
            }),
            NormalizedMarketEvent::MarketTrade(trade) => {
                stored_trade(capture_sequence, venue, market_coin, trade)
            }
            NormalizedMarketEvent::TradeStreamUnavailable(unavailable) => Ok(Self {
                capture_sequence,
                venue,
                market_coin,
                local_receive_time: unavailable.observed_at().nanos_since_start(),
                processing_completion_time: unavailable.observed_at().nanos_since_start(),
                exchange_times: Vec::new(),
                source_id: None,
                payload: StoredPayloadV1::TradeStreamUnavailable {
                    category: unavailable.category().into(),
                    diagnostic: unavailable.diagnostic().into(),
                },
            }),
            NormalizedMarketEvent::TradeStreamResumed(resumed) => Ok(Self {
                capture_sequence,
                venue,
                market_coin,
                local_receive_time: resumed.observed_at().nanos_since_start(),
                processing_completion_time: resumed.observed_at().nanos_since_start(),
                exchange_times: Vec::new(),
                source_id: None,
                payload: StoredPayloadV1::TradeStreamResumed,
            }),
        }
    }

    pub fn to_normalized(&self) -> Result<NormalizedMarketEvent, StorageConversionError> {
        let venue: Venue = self.venue.into();
        let market_coin = MarketCoin::try_new(&self.market_coin)
            .map_err(|_| StorageConversionError::InvalidMarketCoin)?;
        let observed_at = LocalObservationTime::from_nanos_since_start(self.local_receive_time);
        let timestamps = EventTimestamps::new(
            self.exchange_times
                .iter()
                .copied()
                .map(ExchangeTimeObservation::from)
                .collect(),
            observed_at,
            LocalObservationTime::from_nanos_since_start(self.processing_completion_time),
        );
        match &self.payload {
            StoredPayloadV1::OrderBookSnapshot { bids, asks } => {
                let source_sequence = match &self.source_id {
                    Some(StoredSourceIdV1::OrderBookSequence(sequence)) => Some(*sequence),
                    None => None,
                    Some(_) => return Err(StorageConversionError::MissingOrderBookSequence),
                };
                let snapshot = OrderBookSnapshot::try_new(
                    venue,
                    Symbol::perpetual(market_coin),
                    source_sequence,
                    timestamps,
                    bids.iter()
                        .map(stored_book_level)
                        .collect::<Result<Vec<_>, _>>()?,
                    asks.iter()
                        .map(stored_book_level)
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(|_| StorageConversionError::InvalidOrderBook)?;
                Ok(NormalizedMarketEvent::OrderBookSnapshot(snapshot))
            }
            StoredPayloadV1::BestBidOfferUpdated { bid, ask } => Ok(
                NormalizedMarketEvent::BestBidOfferUpdated(BestBidOffer::new(
                    venue,
                    Symbol::perpetual(market_coin),
                    timestamps,
                    bid.as_ref().map(stored_book_level).transpose()?,
                    ask.as_ref().map(stored_book_level).transpose()?,
                )),
            ),
            StoredPayloadV1::OrderBookUnavailable {
                category,
                diagnostic,
            } => Ok(NormalizedMarketEvent::OrderBookUnavailable(
                MarketDataUnavailable::new(
                    venue,
                    market_coin,
                    observed_at,
                    (*category).into(),
                    diagnostic,
                ),
            )),
            StoredPayloadV1::MarketTrade {
                price,
                quantity,
                reporting_kind,
                trade_kind,
                aggressor_side,
                aggressor_side_classification,
            } => Ok(NormalizedMarketEvent::MarketTrade(MarketTrade::new(
                venue,
                Symbol::perpetual(market_coin),
                timestamps,
                stored_price(*price)?,
                stored_quantity(*quantity)?,
                (*reporting_kind).into(),
                (*trade_kind).into(),
                (*aggressor_side).into(),
                (*aggressor_side_classification).into(),
                stored_trade_identity(
                    venue,
                    self.source_id
                        .as_ref()
                        .ok_or(StorageConversionError::MissingTradeIdentity)?,
                )?,
            ))),
            StoredPayloadV1::TradeStreamUnavailable {
                category,
                diagnostic,
            } => Ok(NormalizedMarketEvent::TradeStreamUnavailable(
                MarketDataUnavailable::new(
                    venue,
                    market_coin,
                    observed_at,
                    (*category).into(),
                    diagnostic,
                ),
            )),
            StoredPayloadV1::TradeStreamResumed => Ok(NormalizedMarketEvent::TradeStreamResumed(
                TradeStreamResumed::new(venue, market_coin, observed_at),
            )),
        }
    }
}

fn stored_book_level(level: &StoredBookLevelV1) -> Result<BookLevel, StorageConversionError> {
    let order_count = level
        .order_count
        .map(|count| NonZeroU32::new(count).ok_or(StorageConversionError::InvalidOrderCount))
        .transpose()?;
    Ok(BookLevel::new(
        stored_price(level.price)?,
        stored_quantity(level.quantity)?,
        order_count,
    ))
}

fn stored_price(value: StoredDecimalV1) -> Result<Price, StorageConversionError> {
    Price::from_str(&stored_decimal_string(value)).map_err(|_| StorageConversionError::InvalidPrice)
}

fn stored_quantity(value: StoredDecimalV1) -> Result<Quantity, StorageConversionError> {
    Quantity::from_str(&stored_decimal_string(value))
        .map_err(|_| StorageConversionError::InvalidQuantity)
}

fn stored_decimal_string(value: StoredDecimalV1) -> String {
    if value.scale == 0 {
        return value.coefficient.to_string();
    }
    let negative = value.coefficient.is_negative();
    let digits = value.coefficient.unsigned_abs().to_string();
    let scale = usize::from(value.scale);
    let unsigned = if digits.len() <= scale {
        format!("0.{digits:0>scale$}")
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative {
        format!("-{unsigned}")
    } else {
        unsigned
    }
}

fn stored_trade_identity(
    venue: Venue,
    identity: &StoredSourceIdV1,
) -> Result<MarketTradeIdentity, StorageConversionError> {
    match (venue, identity) {
        (
            Venue::Aster,
            StoredSourceIdV1::AsterTrade {
                aggregate_trade_id,
                first_trade_id,
                last_trade_id,
            },
        ) => Ok(MarketTradeIdentity::Aster {
            aggregate_trade_id: *aggregate_trade_id,
            first_trade_id: *first_trade_id,
            last_trade_id: *last_trade_id,
        }),
        (
            Venue::Hyperliquid,
            StoredSourceIdV1::HyperliquidTrade {
                block_time,
                trade_id,
                transaction_hash,
            },
        ) => Ok(MarketTradeIdentity::Hyperliquid {
            block_time: *block_time,
            trade_id: *trade_id,
            transaction_hash: transaction_hash.clone(),
        }),
        (
            Venue::Binance,
            StoredSourceIdV1::BinanceTrade {
                aggregate_trade_id,
                first_trade_id,
                last_trade_id,
            },
        ) => Ok(MarketTradeIdentity::Binance {
            aggregate_trade_id: *aggregate_trade_id,
            first_trade_id: *first_trade_id,
            last_trade_id: *last_trade_id,
        }),
        (
            Venue::Lighter,
            StoredSourceIdV1::LighterTrade {
                market_id,
                trade_id_string,
                message_nonce,
            },
        ) => Ok(MarketTradeIdentity::Lighter {
            market_id: *market_id,
            trade_id: trade_id_string.clone(),
            message_nonce: *message_nonce,
        }),
        _ => Err(StorageConversionError::TradeIdentityVenueMismatch),
    }
}

fn stored_trade(
    capture_sequence: u64,
    venue: StoredVenueV1,
    market_coin: String,
    trade: &MarketTrade,
) -> Result<StoredEventV1, StorageConversionError> {
    let source_id = match trade.identity() {
        MarketTradeIdentity::Aster {
            aggregate_trade_id,
            first_trade_id,
            last_trade_id,
        } if trade.venue() == Venue::Aster => StoredSourceIdV1::AsterTrade {
            aggregate_trade_id: *aggregate_trade_id,
            first_trade_id: *first_trade_id,
            last_trade_id: *last_trade_id,
        },
        MarketTradeIdentity::Hyperliquid {
            block_time,
            trade_id,
            transaction_hash,
        } if trade.venue() == Venue::Hyperliquid => StoredSourceIdV1::HyperliquidTrade {
            block_time: *block_time,
            trade_id: *trade_id,
            transaction_hash: transaction_hash.clone(),
        },
        MarketTradeIdentity::Binance {
            aggregate_trade_id,
            first_trade_id,
            last_trade_id,
        } if trade.venue() == Venue::Binance => StoredSourceIdV1::BinanceTrade {
            aggregate_trade_id: *aggregate_trade_id,
            first_trade_id: *first_trade_id,
            last_trade_id: *last_trade_id,
        },
        MarketTradeIdentity::Lighter {
            market_id,
            trade_id,
            message_nonce,
        } if trade.venue() == Venue::Lighter => StoredSourceIdV1::LighterTrade {
            market_id: *market_id,
            trade_id_string: trade_id.clone(),
            message_nonce: *message_nonce,
        },
        _ => return Err(StorageConversionError::TradeIdentityVenueMismatch),
    };
    let timestamps = trade.timestamps();
    Ok(StoredEventV1 {
        capture_sequence,
        venue,
        market_coin,
        local_receive_time: timestamps.local_receive().nanos_since_start(),
        processing_completion_time: timestamps.processing_completed().nanos_since_start(),
        exchange_times: stored_exchange_times(timestamps),
        source_id: Some(source_id),
        payload: StoredPayloadV1::MarketTrade {
            price: StoredDecimalV1 {
                coefficient: trade.price().coefficient(),
                scale: trade.price().scale(),
            },
            quantity: StoredDecimalV1 {
                coefficient: trade.quantity().coefficient(),
                scale: trade.quantity().scale(),
            },
            reporting_kind: trade.reporting_kind().into(),
            trade_kind: trade.trade_kind().into(),
            aggressor_side: trade.aggressor_side().into(),
            aggressor_side_classification: trade.aggressor_side_classification().into(),
        },
    })
}

fn stored_exchange_times(timestamps: &EventTimestamps) -> Vec<StoredExchangeTimeObservationV1> {
    timestamps
        .exchange_times()
        .iter()
        .copied()
        .map(StoredExchangeTimeObservationV1::from)
        .collect()
}

impl From<Venue> for StoredVenueV1 {
    fn from(value: Venue) -> Self {
        match value {
            Venue::Aster => Self::Aster,
            Venue::Binance => Self::Binance,
            Venue::Hyperliquid => Self::Hyperliquid,
            Venue::Lighter => Self::Lighter,
        }
    }
}

impl From<StoredVenueV1> for Venue {
    fn from(value: StoredVenueV1) -> Self {
        match value {
            StoredVenueV1::Aster => Self::Aster,
            StoredVenueV1::Binance => Self::Binance,
            StoredVenueV1::Hyperliquid => Self::Hyperliquid,
            StoredVenueV1::Lighter => Self::Lighter,
        }
    }
}

impl From<ExchangeTimeObservation> for StoredExchangeTimeObservationV1 {
    fn from(value: ExchangeTimeObservation) -> Self {
        Self {
            kind: value.kind().into(),
            raw_value: value.raw_value(),
            declared_unit: value.declared_unit().into(),
        }
    }
}

impl From<StoredExchangeTimeObservationV1> for ExchangeTimeObservation {
    fn from(value: StoredExchangeTimeObservationV1) -> Self {
        Self::new(
            value.kind.into(),
            value.raw_value,
            value.declared_unit.into(),
        )
    }
}

impl From<ExchangeTimeKind> for StoredExchangeTimeKindV1 {
    fn from(value: ExchangeTimeKind) -> Self {
        match value {
            ExchangeTimeKind::EventTime => Self::EventTime,
            ExchangeTimeKind::TradeTime => Self::TradeTime,
            ExchangeTimeKind::BlockTime => Self::BlockTime,
            ExchangeTimeKind::TransactionTime => Self::TransactionTime,
            ExchangeTimeKind::Other => Self::Other,
        }
    }
}

impl From<StoredExchangeTimeKindV1> for ExchangeTimeKind {
    fn from(value: StoredExchangeTimeKindV1) -> Self {
        match value {
            StoredExchangeTimeKindV1::EventTime => Self::EventTime,
            StoredExchangeTimeKindV1::TradeTime => Self::TradeTime,
            StoredExchangeTimeKindV1::BlockTime => Self::BlockTime,
            StoredExchangeTimeKindV1::TransactionTime => Self::TransactionTime,
            StoredExchangeTimeKindV1::Other => Self::Other,
        }
    }
}

impl From<ExchangeTimeUnit> for StoredExchangeTimeUnitV1 {
    fn from(value: ExchangeTimeUnit) -> Self {
        match value {
            ExchangeTimeUnit::Milliseconds => Self::Milliseconds,
            ExchangeTimeUnit::Microseconds => Self::Microseconds,
            ExchangeTimeUnit::Nanoseconds => Self::Nanoseconds,
            ExchangeTimeUnit::Unknown => Self::Unknown,
        }
    }
}

impl From<StoredExchangeTimeUnitV1> for ExchangeTimeUnit {
    fn from(value: StoredExchangeTimeUnitV1) -> Self {
        match value {
            StoredExchangeTimeUnitV1::Milliseconds => Self::Milliseconds,
            StoredExchangeTimeUnitV1::Microseconds => Self::Microseconds,
            StoredExchangeTimeUnitV1::Nanoseconds => Self::Nanoseconds,
            StoredExchangeTimeUnitV1::Unknown => Self::Unknown,
        }
    }
}

impl From<market_data::BookLevel> for StoredBookLevelV1 {
    fn from(value: market_data::BookLevel) -> Self {
        Self {
            price: StoredDecimalV1 {
                coefficient: value.price().coefficient(),
                scale: value.price().scale(),
            },
            quantity: StoredDecimalV1 {
                coefficient: value.quantity().coefficient(),
                scale: value.quantity().scale(),
            },
            order_count: value.order_count().map(std::num::NonZeroU32::get),
        }
    }
}

macro_rules! map_enum {
    ($from:ty => $to:ty { $($variant:ident),+ $(,)? }) => {
        impl From<$from> for $to {
            fn from(value: $from) -> Self {
                match value {
                    $(<$from>::$variant => Self::$variant,)+
                }
            }
        }
    };
}

map_enum!(MarketTradeReportingKind => StoredMarketTradeReportingKindV1 {
    Individual,
    TakerOrderAggregate,
});
map_enum!(MarketTradeKind => StoredMarketTradeKindV1 {
    Regular,
    Liquidation,
    Deleverage,
    MarketSettlement,
    Other,
});
map_enum!(AggressorSide => StoredAggressorSideV1 { Buy, Sell, Unknown });
map_enum!(AggressorSideClassification => StoredAggressorSideClassificationV1 {
    VenueProvided,
    DerivedFromMakerSide,
    Unknown,
});
map_enum!(UnavailabilityCategory => StoredUnavailabilityCategoryV1 {
    Disconnected,
    InvalidMarketData,
    SequenceGap,
    RecorderShutdown,
    Other,
});

macro_rules! map_stored_enum {
    ($from:ty => $to:ty { $($variant:ident),+ $(,)? }) => {
        impl From<$from> for $to {
            fn from(value: $from) -> Self {
                match value {
                    $(<$from>::$variant => Self::$variant,)+
                }
            }
        }
    };
}

map_stored_enum!(StoredMarketTradeReportingKindV1 => MarketTradeReportingKind {
    Individual,
    TakerOrderAggregate,
});
map_stored_enum!(StoredMarketTradeKindV1 => MarketTradeKind {
    Regular,
    Liquidation,
    Deleverage,
    MarketSettlement,
    Other,
});
map_stored_enum!(StoredAggressorSideV1 => AggressorSide { Buy, Sell, Unknown });
map_stored_enum!(StoredAggressorSideClassificationV1 => AggressorSideClassification {
    VenueProvided,
    DerivedFromMakerSide,
    Unknown,
});
map_stored_enum!(StoredUnavailabilityCategoryV1 => UnavailabilityCategory {
    Disconnected,
    InvalidMarketData,
    SequenceGap,
    RecorderShutdown,
    Other,
});

impl Display for StorageConversionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCaptureSequence => formatter.write_str("Capture Sequence must start at one"),
            Self::TradeIdentityVenueMismatch => {
                formatter.write_str("trade identity does not match its venue")
            }
            Self::InvalidMarketCoin => formatter.write_str("stored Market Coin is invalid"),
            Self::InvalidPrice => formatter.write_str("stored Price is invalid"),
            Self::InvalidQuantity => formatter.write_str("stored Quantity is invalid"),
            Self::InvalidOrderCount => formatter.write_str("stored order count is zero"),
            Self::InvalidOrderBook => formatter.write_str("stored Order Book is invalid"),
            Self::MissingOrderBookSequence => {
                formatter.write_str("Order Book has a non-book source identity")
            }
            Self::MissingTradeIdentity => formatter.write_str("Market Trade identity is missing"),
        }
    }
}

impl Error for StorageConversionError {}
