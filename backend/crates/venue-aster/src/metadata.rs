use std::error::Error;
use std::fmt::{self, Display, Formatter};

use domain::MarketCoin;
use serde::Deserialize;

const ASTER_EXCHANGE_INFO_URL: &str = "https://fapi.asterdex.com/fapi/v3/exchangeInfo";
const MAX_STREAMS_PER_CONNECTION: usize = 200;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AsterMarket {
    market_coin: MarketCoin,
    symbol: String,
}

impl AsterMarket {
    pub(crate) fn new(market_coin: MarketCoin, symbol: String) -> Self {
        Self {
            market_coin,
            symbol,
        }
    }

    pub(crate) const fn market_coin(&self) -> &MarketCoin {
        &self.market_coin
    }

    pub(crate) fn symbol(&self) -> &str {
        &self.symbol
    }
}

pub(crate) async fn resolve_markets(
    market_coins: &[MarketCoin],
) -> Result<Vec<AsterMarket>, MetadataError> {
    if market_coins.len() > MAX_STREAMS_PER_CONNECTION {
        return Err(MetadataError::TooManyMarkets {
            actual: market_coins.len(),
            maximum: MAX_STREAMS_PER_CONNECTION,
        });
    }

    let response = reqwest::get(ASTER_EXCHANGE_INFO_URL)
        .await
        .map_err(MetadataError::Request)?
        .error_for_status()
        .map_err(MetadataError::Request)?
        .json::<WireExchangeInfo>()
        .await
        .map_err(MetadataError::Request)?;

    market_coins
        .iter()
        .map(|market_coin| resolve_market(market_coin, &response.symbols))
        .collect()
}

fn resolve_market(
    market_coin: &MarketCoin,
    markets: &[WireMarket],
) -> Result<AsterMarket, MetadataError> {
    let market = markets
        .iter()
        .find(|market| market.base_asset == market_coin.as_str() && market.quote_asset == "USDT")
        .ok_or_else(|| MetadataError::UnknownMarket(market_coin.clone()))?;

    if market.contract_type != "PERPETUAL" {
        return Err(MetadataError::NotPerpetual(market_coin.clone()));
    }
    if market.status != "TRADING" {
        return Err(MetadataError::Inactive(market_coin.clone()));
    }

    Ok(AsterMarket::new(market_coin.clone(), market.symbol.clone()))
}

#[derive(Debug)]
pub enum MetadataError {
    Request(reqwest::Error),
    UnknownMarket(MarketCoin),
    NotPerpetual(MarketCoin),
    Inactive(MarketCoin),
    TooManyMarkets { actual: usize, maximum: usize },
}

impl Display for MetadataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => write!(formatter, "failed to load Aster markets: {error}"),
            Self::UnknownMarket(coin) => write!(formatter, "unknown Aster USDT market coin {coin}"),
            Self::NotPerpetual(coin) => write!(formatter, "Aster market {coin} is not perpetual"),
            Self::Inactive(coin) => write!(formatter, "Aster market {coin} is not trading"),
            Self::TooManyMarkets { actual, maximum } => write!(
                formatter,
                "configured {actual} Aster markets, maximum per connection is {maximum}"
            ),
        }
    }
}

impl Error for MetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Deserialize)]
struct WireExchangeInfo {
    symbols: Vec<WireMarket>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireMarket {
    symbol: String,
    contract_type: String,
    status: String,
    base_asset: String,
    quote_asset: String,
}

#[cfg(test)]
mod tests {
    use domain::MarketCoin;

    use super::{MetadataError, WireMarket, resolve_market};

    fn coin(value: &str) -> MarketCoin {
        MarketCoin::try_new(value).unwrap()
    }

    fn market(contract_type: &str, status: &str) -> WireMarket {
        WireMarket {
            symbol: "BTCUSDT".into(),
            contract_type: contract_type.into(),
            status: status.into(),
            base_asset: "BTC".into(),
            quote_asset: "USDT".into(),
        }
    }

    #[test]
    fn resolves_exact_active_usdt_perpetual() {
        let markets = vec![market("PERPETUAL", "TRADING")];
        let resolved = resolve_market(&coin("BTC"), &markets).unwrap();
        assert_eq!(resolved.symbol(), "BTCUSDT");
        assert!(matches!(
            resolve_market(&coin("btc"), &markets),
            Err(MetadataError::UnknownMarket(_))
        ));
    }

    #[test]
    fn rejects_inactive_and_non_perpetual_markets() {
        assert!(matches!(
            resolve_market(&coin("BTC"), &[market("CURRENT_QUARTER", "TRADING")]),
            Err(MetadataError::NotPerpetual(_))
        ));
        assert!(matches!(
            resolve_market(&coin("BTC"), &[market("PERPETUAL", "BREAK")]),
            Err(MetadataError::Inactive(_))
        ));
    }
}
