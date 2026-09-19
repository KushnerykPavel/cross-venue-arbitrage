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
    coin: String,
    side: String,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
}

impl WireTrade {
    pub(crate) fn coin(&self) -> &str {
        &self.coin
    }
}

#[derive(Debug)]
pub(crate) enum TradeDecodeError {
    UnexpectedCoin {
        expected: MarketCoin,
        actual: String,
    },
    InvalidPrice(DecimalParseError),
    InvalidQuantity(DecimalParseError),
}

pub(crate) fn decode_trade(
    wire: WireTrade,
    expected_coin: &MarketCoin,
    local_receive: LocalObservationTime,
    processing_completed: LocalObservationTime,
) -> Result<MarketTrade, TradeDecodeError> {
    if wire.coin != expected_coin.as_str() {
        return Err(TradeDecodeError::UnexpectedCoin {
            expected: expected_coin.clone(),
            actual: wire.coin,
        });
    }
    let price = Price::from_str(&wire.px).map_err(TradeDecodeError::InvalidPrice)?;
    let quantity = Quantity::from_str(&wire.sz).map_err(TradeDecodeError::InvalidQuantity)?;
    let (aggressor_side, classification) = match wire.side.as_str() {
        "B" => (
            AggressorSide::Buy,
            AggressorSideClassification::VenueProvided,
        ),
        "A" => (
            AggressorSide::Sell,
            AggressorSideClassification::VenueProvided,
        ),
        _ => (AggressorSide::Unknown, AggressorSideClassification::Unknown),
    };

    Ok(MarketTrade::new(
        Venue::Hyperliquid,
        Symbol::perpetual(expected_coin.clone()),
        EventTimestamps::new(
            vec![ExchangeTimeObservation::new(
                ExchangeTimeKind::BlockTime,
                wire.time,
                ExchangeTimeUnit::Unknown,
            )],
            local_receive,
            processing_completed,
        ),
        price,
        quantity,
        MarketTradeReportingKind::Individual,
        MarketTradeKind::Regular,
        aggressor_side,
        classification,
        MarketTradeIdentity::Hyperliquid {
            block_time: wire.time,
            trade_id: wire.tid,
            transaction_hash: wire.hash,
        },
    ))
}

impl Display for TradeDecodeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedCoin { expected, actual } => {
                write!(formatter, "expected {expected} trade, received {actual}")
            }
            Self::InvalidPrice(error) => {
                write!(formatter, "invalid Hyperliquid trade price: {error}")
            }
            Self::InvalidQuantity(error) => {
                write!(formatter, "invalid Hyperliquid trade quantity: {error}")
            }
        }
    }
}

impl Error for TradeDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPrice(error) | Self::InvalidQuantity(error) => Some(error),
            Self::UnexpectedCoin { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_fixture_without_inventing_a_time_unit() {
        let wire: WireTrade = serde_json::from_str(
            r#"{"coin":"BTC","side":"B","px":"63250.125","sz":"0.004","hash":"0xabc","time":1725000000100,"tid":981894269326506}"#,
        )
        .unwrap();
        let trade = decode_trade(
            wire,
            &MarketCoin::try_new("BTC").unwrap(),
            LocalObservationTime::from_nanos_since_start(10),
            LocalObservationTime::from_nanos_since_start(11),
        )
        .unwrap();

        assert_eq!(trade.aggressor_side(), AggressorSide::Buy);
        assert_eq!(
            trade.timestamps().exchange_times()[0].declared_unit(),
            ExchangeTimeUnit::Unknown
        );
        assert_eq!(
            trade.identity(),
            &MarketTradeIdentity::Hyperliquid {
                block_time: 1_725_000_000_100,
                trade_id: 981_894_269_326_506,
                transaction_hash: "0xabc".into(),
            }
        );
    }

    #[test]
    fn preserves_unknown_side_instead_of_guessing() {
        let wire: WireTrade = serde_json::from_str(
            r#"{"coin":"BTC","side":"?","px":"1","sz":"1","hash":"0x1","time":1,"tid":2}"#,
        )
        .unwrap();
        let trade = decode_trade(
            wire,
            &MarketCoin::try_new("BTC").unwrap(),
            LocalObservationTime::from_nanos_since_start(1),
            LocalObservationTime::from_nanos_since_start(2),
        )
        .unwrap();
        assert_eq!(trade.aggressor_side(), AggressorSide::Unknown);
        assert_eq!(
            trade.aggressor_side_classification(),
            AggressorSideClassification::Unknown
        );
    }
}
