use std::error::Error;
use std::fmt::{self, Display, Formatter};

use domain::MarketCoin;
use serde::Deserialize;

const LIGHTER_MARKETS_URL: &str = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LighterMarket {
    market_coin: MarketCoin,
    market_id: u32,
}

impl LighterMarket {
    pub(crate) const fn new(market_coin: MarketCoin, market_id: u32) -> Self {
        Self {
            market_coin,
            market_id,
        }
    }

    pub(crate) const fn market_coin(&self) -> &MarketCoin {
        &self.market_coin
    }

    pub(crate) const fn market_id(&self) -> u32 {
        self.market_id
    }
}

pub(crate) async fn resolve_markets(
    market_coins: &[MarketCoin],
) -> Result<Vec<LighterMarket>, MetadataError> {
    let response = reqwest::get(LIGHTER_MARKETS_URL)
        .await
        .map_err(MetadataError::Request)?
        .error_for_status()
        .map_err(MetadataError::Request)?
        .json::<WireMarkets>()
        .await
        .map_err(MetadataError::Request)?;

    market_coins
        .iter()
        .map(|market_coin| resolve_market(market_coin, &response.order_books))
        .collect()
}

fn resolve_market(
    market_coin: &MarketCoin,
    markets: &[WireMarket],
) -> Result<LighterMarket, MetadataError> {
    let market = markets
        .iter()
        .find(|market| market.symbol == market_coin.as_str())
        .ok_or_else(|| MetadataError::UnknownMarket(market_coin.clone()))?;

    if market.market_type != "perp" {
        return Err(MetadataError::NotPerpetual(market_coin.clone()));
    }
    if market.status != "active" {
        return Err(MetadataError::Inactive(market_coin.clone()));
    }

    Ok(LighterMarket::new(market_coin.clone(), market.market_id))
}

#[derive(Debug)]
pub enum MetadataError {
    Request(reqwest::Error),
    UnknownMarket(MarketCoin),
    NotPerpetual(MarketCoin),
    Inactive(MarketCoin),
}

impl Display for MetadataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => write!(formatter, "failed to load Lighter markets: {error}"),
            Self::UnknownMarket(coin) => write!(formatter, "unknown Lighter market coin {coin}"),
            Self::NotPerpetual(coin) => {
                write!(formatter, "Lighter market {coin} is not a perpetual")
            }
            Self::Inactive(coin) => write!(formatter, "Lighter market {coin} is inactive"),
        }
    }
}

impl Error for MetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            Self::UnknownMarket(_) | Self::NotPerpetual(_) | Self::Inactive(_) => None,
        }
    }
}

#[derive(Deserialize)]
struct WireMarkets {
    order_books: Vec<WireMarket>,
}

#[derive(Deserialize)]
struct WireMarket {
    symbol: String,
    market_id: u32,
    market_type: String,
    status: String,
}

#[cfg(test)]
mod tests {
    use domain::MarketCoin;

    use super::{MetadataError, WireMarket, resolve_market};

    fn coin(value: &str) -> MarketCoin {
        MarketCoin::try_new(value).unwrap()
    }

    #[test]
    fn resolves_only_active_perpetual_with_exact_symbol() {
        let markets = vec![WireMarket {
            symbol: "BTC".into(),
            market_id: 1,
            market_type: "perp".into(),
            status: "active".into(),
        }];
        let market = resolve_market(&coin("BTC"), &markets).unwrap();
        assert_eq!(market.market_id(), 1);
        assert!(matches!(
            resolve_market(&coin("btc"), &markets),
            Err(MetadataError::UnknownMarket(_))
        ));
    }

    #[test]
    fn rejects_inactive_and_non_perpetual_markets() {
        let inactive = vec![WireMarket {
            symbol: "BTC".into(),
            market_id: 1,
            market_type: "perp".into(),
            status: "inactive".into(),
        }];
        assert!(matches!(
            resolve_market(&coin("BTC"), &inactive),
            Err(MetadataError::Inactive(_))
        ));

        let spot = vec![WireMarket {
            symbol: "BTC".into(),
            market_id: 1,
            market_type: "spot".into(),
            status: "active".into(),
        }];
        assert!(matches!(
            resolve_market(&coin("BTC"), &spot),
            Err(MetadataError::NotPerpetual(_))
        ));
    }
}
