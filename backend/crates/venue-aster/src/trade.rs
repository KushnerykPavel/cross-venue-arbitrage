use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use domain::{DecimalParseError, MarketCoin, Price, Quantity, Symbol, Venue};
use market_data::{
    AggressorSide, AggressorSideClassification, EventTimestamps, ExchangeTimeKind,
    ExchangeTimeObservation, ExchangeTimeUnit, LocalObservationTime, MarketTrade,
    MarketTradeIdentity, MarketTradeKind, MarketTradeReportingKind,
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
struct WireAggregateTrade {
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

#[derive(Debug)]
pub(crate) enum TradeDecodeError {
    InvalidPayload(serde_json::Error),
    UnexpectedEventType(String),
    UnexpectedSymbol { expected: String, actual: String },
    InvalidPrice(DecimalParseError),
    InvalidQuantity(DecimalParseError),
}

pub(crate) fn decode_trade(
    payload: Value,
    expected_symbol: &str,
    market_coin: &MarketCoin,
    local_receive: LocalObservationTime,
    processing_completed: LocalObservationTime,
) -> Result<MarketTrade, TradeDecodeError> {
    let wire: WireAggregateTrade =
        serde_json::from_value(payload).map_err(TradeDecodeError::InvalidPayload)?;
    if wire.event_type != "aggTrade" {
        return Err(TradeDecodeError::UnexpectedEventType(wire.event_type));
    }
    if wire.symbol != expected_symbol {
        return Err(TradeDecodeError::UnexpectedSymbol {
            expected: expected_symbol.into(),
            actual: wire.symbol,
        });
    }
    let price = Price::from_str(&wire.price).map_err(TradeDecodeError::InvalidPrice)?;
    let quantity = Quantity::from_str(&wire.quantity).map_err(TradeDecodeError::InvalidQuantity)?;
    let aggressor_side = if wire.buyer_is_maker {
        AggressorSide::Sell
    } else {
        AggressorSide::Buy
    };

    Ok(MarketTrade::new(
        Venue::Aster,
        Symbol::perpetual(market_coin.clone()),
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
            processing_completed,
        ),
        price,
        quantity,
        MarketTradeReportingKind::TakerOrderAggregate,
        MarketTradeKind::Regular,
        aggressor_side,
        AggressorSideClassification::DerivedFromMakerSide,
        MarketTradeIdentity::Aster {
            aggregate_trade_id: wire.aggregate_trade_id,
            first_trade_id: wire.first_trade_id,
            last_trade_id: wire.last_trade_id,
        },
    ))
}

impl Display for TradeDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayload(error) => {
                write!(formatter, "invalid Aster aggTrade payload: {error}")
            }
            Self::UnexpectedEventType(actual) => {
                write!(
                    formatter,
                    "expected Aster aggTrade event, received {actual}"
                )
            }
            Self::UnexpectedSymbol { expected, actual } => {
                write!(
                    formatter,
                    "expected Aster symbol {expected}, received {actual}"
                )
            }
            Self::InvalidPrice(error) => write!(formatter, "invalid Aster trade price: {error}"),
            Self::InvalidQuantity(error) => {
                write!(formatter, "invalid Aster trade quantity: {error}")
            }
        }
    }
}

impl Error for TradeDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPayload(error) => Some(error),
            Self::InvalidPrice(error) | Self::InvalidQuantity(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_aggregate_identity_times_and_maker_side() {
        let payload = serde_json::json!({
            "e": "aggTrade", "E": 1_725_000_000_100_u64, "s": "BTCUSDT",
            "a": 91_u64, "p": "63250.125", "q": "0.004",
            "f": 700_u64, "l": 703_u64, "T": 1_725_000_000_099_u64, "m": true
        });
        let trade = decode_trade(
            payload,
            "BTCUSDT",
            &MarketCoin::try_new("BTC").unwrap(),
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(11),
        )
        .unwrap();

        assert_eq!(
            trade.reporting_kind(),
            MarketTradeReportingKind::TakerOrderAggregate
        );
        assert_eq!(trade.aggressor_side(), AggressorSide::Sell);
        assert_eq!(
            trade.aggressor_side_classification(),
            AggressorSideClassification::DerivedFromMakerSide
        );
        assert_eq!(trade.timestamps().exchange_times().len(), 2);
        assert_eq!(
            trade.identity(),
            &MarketTradeIdentity::Aster {
                aggregate_trade_id: 91,
                first_trade_id: 700,
                last_trade_id: 703,
            }
        );
    }
}
