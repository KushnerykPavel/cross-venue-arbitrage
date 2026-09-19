use std::error::Error;
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MarketCoin(String);

impl MarketCoin {
    pub fn try_new(value: impl Into<String>) -> Result<Self, MarketCoinError> {
        let value = value.into();
        if value.is_empty() {
            return Err(MarketCoinError::Empty);
        }
        if value.chars().any(char::is_whitespace) {
            return Err(MarketCoinError::Whitespace);
        }
        if value.chars().any(char::is_control) {
            return Err(MarketCoinError::ControlCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for MarketCoin {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketCoinError {
    Empty,
    Whitespace,
    ControlCharacter,
}

impl Display for MarketCoinError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("market coin is empty"),
            Self::Whitespace => formatter.write_str("market coin contains whitespace"),
            Self::ControlCharacter => {
                formatter.write_str("market coin contains a control character")
            }
        }
    }
}

impl Error for MarketCoinError {}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Symbol {
    Perpetual(MarketCoin),
}

impl Symbol {
    pub fn perpetual(market_coin: MarketCoin) -> Self {
        Self::Perpetual(market_coin)
    }

    pub const fn market_coin(&self) -> &MarketCoin {
        match self {
            Self::Perpetual(market_coin) => market_coin,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MarketCoin, MarketCoinError};

    #[test]
    fn preserves_case_and_punctuation() {
        let coin = MarketCoin::try_new("dex-name:Coin_1").unwrap();
        assert_eq!(coin.as_str(), "dex-name:Coin_1");
    }

    #[test]
    fn rejects_empty_whitespace_and_control_characters() {
        assert_eq!(MarketCoin::try_new(""), Err(MarketCoinError::Empty));
        assert_eq!(
            MarketCoin::try_new("BT C"),
            Err(MarketCoinError::Whitespace)
        );
        assert_eq!(
            MarketCoin::try_new("BTC\n"),
            Err(MarketCoinError::Whitespace)
        );
        assert_eq!(
            MarketCoin::try_new("BTC\0"),
            Err(MarketCoinError::ControlCharacter)
        );
    }
}
