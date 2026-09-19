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

#[derive(Deserialize)]
pub(crate) struct WireTrade {
    trade_id_str: String,
    #[serde(rename = "type")]
    kind: String,
    market_id: u32,
    size: String,
    price: String,
    is_maker_ask: bool,
    timestamp: u64,
    transaction_time: u64,
}

impl WireTrade {
    pub(crate) const fn market_id(&self) -> u32 {
        self.market_id
    }
}

#[derive(Debug)]
pub(crate) enum TradeDecodeError {
    UnexpectedMarketId { expected: u32, actual: u32 },
    InvalidPrice(DecimalParseError),
    InvalidQuantity(DecimalParseError),
}

pub(crate) fn decode_trade(
    wire: WireTrade,
    expected_market_id: u32,
    market_coin: &MarketCoin,
    message_nonce: Option<u64>,
    local_receive: LocalObservationTime,
    processing_completed: LocalObservationTime,
) -> Result<MarketTrade, TradeDecodeError> {
    if wire.market_id != expected_market_id {
        return Err(TradeDecodeError::UnexpectedMarketId {
            expected: expected_market_id,
            actual: wire.market_id,
        });
    }
    let price = Price::from_str(&wire.price).map_err(TradeDecodeError::InvalidPrice)?;
    let quantity = Quantity::from_str(&wire.size).map_err(TradeDecodeError::InvalidQuantity)?;
    let aggressor_side = if wire.is_maker_ask {
        AggressorSide::Buy
    } else {
        AggressorSide::Sell
    };

    Ok(MarketTrade::new(
        Venue::Lighter,
        Symbol::perpetual(market_coin.clone()),
        EventTimestamps::new(
            vec![
                ExchangeTimeObservation::new(
                    ExchangeTimeKind::EventTime,
                    wire.timestamp,
                    ExchangeTimeUnit::Unknown,
                ),
                ExchangeTimeObservation::new(
                    ExchangeTimeKind::TransactionTime,
                    wire.transaction_time,
                    ExchangeTimeUnit::Unknown,
                ),
            ],
            local_receive,
            processing_completed,
        ),
        price,
        quantity,
        MarketTradeReportingKind::Individual,
        decode_trade_kind(&wire.kind),
        aggressor_side,
        AggressorSideClassification::DerivedFromMakerSide,
        MarketTradeIdentity::Lighter {
            market_id: wire.market_id,
            trade_id: wire.trade_id_str,
            message_nonce,
        },
    ))
}

fn decode_trade_kind(value: &str) -> MarketTradeKind {
    match value.to_ascii_lowercase().as_str() {
        "trade" | "regular" => MarketTradeKind::Regular,
        "liquidation" => MarketTradeKind::Liquidation,
        "deleverage" => MarketTradeKind::Deleverage,
        "market_settlement" | "market-settlement" => MarketTradeKind::MarketSettlement,
        _ => MarketTradeKind::Other,
    }
}

impl Display for TradeDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedMarketId { expected, actual } => {
                write!(
                    formatter,
                    "expected Lighter market ID {expected}, received {actual}"
                )
            }
            Self::InvalidPrice(error) => write!(formatter, "invalid Lighter trade price: {error}"),
            Self::InvalidQuantity(error) => {
                write!(formatter, "invalid Lighter trade quantity: {error}")
            }
        }
    }
}

impl Error for TradeDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPrice(error) | Self::InvalidQuantity(error) => Some(error),
            Self::UnexpectedMarketId { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(kind: &str, is_maker_ask: bool) -> WireTrade {
        serde_json::from_value(serde_json::json!({
            "trade_id_str": "9007199254740993",
            "type": kind,
            "market_id": 1,
            "size": "0.004",
            "price": "63250.125",
            "is_maker_ask": is_maker_ask,
            "timestamp": 1_725_000_000_100_u64,
            "transaction_time": 1_725_000_000_099_u64
        }))
        .unwrap()
    }

    #[test]
    fn decodes_string_identity_both_times_and_maker_side() {
        let trade = decode_trade(
            fixture("trade", true),
            1,
            &MarketCoin::try_new("BTC").unwrap(),
            Some(88),
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(11),
        )
        .unwrap();

        assert_eq!(trade.aggressor_side(), AggressorSide::Buy);
        assert_eq!(trade.trade_kind(), MarketTradeKind::Regular);
        assert_eq!(trade.timestamps().exchange_times().len(), 2);
        assert_eq!(
            trade.identity(),
            &MarketTradeIdentity::Lighter {
                market_id: 1,
                trade_id: "9007199254740993".into(),
                message_nonce: Some(88),
            }
        );
    }

    #[test]
    fn preserves_liquidation_and_unknown_trade_kinds() {
        let coin = MarketCoin::try_new("BTC").unwrap();
        let decode = |wire| {
            decode_trade(
                wire,
                1,
                &coin,
                None,
                LocalObservationTime::from_nanos_since_start(10),
                LocalObservationTime::from_nanos_since_start(11),
            )
            .unwrap()
        };
        assert_eq!(
            decode(fixture("liquidation", false)).trade_kind(),
            MarketTradeKind::Liquidation
        );
        assert_eq!(
            decode(fixture("future_kind", false)).trade_kind(),
            MarketTradeKind::Other
        );
    }
}
