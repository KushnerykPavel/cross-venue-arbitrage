use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use domain::{MarketCoin, MarketCoinError};

const MARKET_COINS: &str = "MARKET_COINS";
const ENABLE_HYPERLIQUID: &str = "ENABLE_HYPERLIQUID";
const TRADE_DEDUP_CAPACITY: &str = "TRADE_DEDUP_CAPACITY";
const DATA_DIR: &str = "DATA_DIR";
const RECORDER_QUEUE_CAPACITY: &str = "RECORDER_QUEUE_CAPACITY";
const CAPTURE_DURATION_SECONDS: &str = "CAPTURE_DURATION_SECONDS";
const DEFAULT_TRADE_DEDUP_CAPACITY: usize = 1_000_000;
const DEFAULT_RECORDER_QUEUE_CAPACITY: usize = 4_096;
const DEFAULT_CAPTURE_DURATION_SECONDS: u64 = 2 * 60 * 60;

pub struct TraderConfig {
    pub market_coins: Vec<MarketCoin>,
    pub enable_hyperliquid: bool,
    pub trade_dedup_capacity: NonZeroUsize,
    pub data_dir: PathBuf,
    pub recorder_queue_capacity: NonZeroUsize,
    pub capture_duration: Duration,
}

impl TraderConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let env_file = backend_env_file();
        if env_file.exists() {
            // `from_path` preserves variables already present in the process.
            dotenvy::from_path(&env_file).map_err(ConfigError::EnvFile)?;
        }

        let market_coins =
            env::var(MARKET_COINS).map_err(|_| ConfigError::MissingVariable(MARKET_COINS))?;
        let enable_hyperliquid = optional_bool(ENABLE_HYPERLIQUID, true)?;
        let trade_dedup_capacity = match env::var(TRADE_DEDUP_CAPACITY) {
            Ok(raw) => parse_positive_usize(TRADE_DEDUP_CAPACITY, &raw)?,
            Err(env::VarError::NotPresent) => NonZeroUsize::new(DEFAULT_TRADE_DEDUP_CAPACITY)
                .expect("default dedup capacity is positive"),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(ConfigError::InvalidPositiveInteger {
                    variable: TRADE_DEDUP_CAPACITY,
                    value: "<non-unicode>".into(),
                });
            }
        };
        let data_dir = env::var(DATA_DIR).map_err(|_| ConfigError::MissingVariable(DATA_DIR))?;
        let data_dir = PathBuf::from(data_dir);
        if !data_dir.is_absolute() {
            return Err(ConfigError::DataDirectoryMustBeAbsolute(data_dir));
        }
        let recorder_queue_capacity =
            optional_positive_usize(RECORDER_QUEUE_CAPACITY, DEFAULT_RECORDER_QUEUE_CAPACITY)?;
        let capture_duration_seconds =
            optional_positive_u64(CAPTURE_DURATION_SECONDS, DEFAULT_CAPTURE_DURATION_SECONDS)?;
        Ok(Self {
            market_coins: parse_market_coins(MARKET_COINS, &market_coins)?,
            enable_hyperliquid,
            trade_dedup_capacity,
            data_dir,
            recorder_queue_capacity,
            capture_duration: Duration::from_secs(capture_duration_seconds),
        })
    }
}

fn optional_bool(variable: &'static str, default: bool) -> Result<bool, ConfigError> {
    match env::var(variable) {
        Ok(raw) => raw
            .parse::<bool>()
            .map_err(|_| ConfigError::InvalidBoolean {
                variable,
                value: raw,
            }),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidBoolean {
            variable,
            value: "<non-unicode>".into(),
        }),
    }
}

fn optional_positive_usize(
    variable: &'static str,
    default: usize,
) -> Result<NonZeroUsize, ConfigError> {
    match env::var(variable) {
        Ok(raw) => parse_positive_usize(variable, &raw),
        Err(env::VarError::NotPresent) => {
            Ok(NonZeroUsize::new(default).expect("configuration default must be positive"))
        }
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidPositiveInteger {
            variable,
            value: "<non-unicode>".into(),
        }),
    }
}

fn optional_positive_u64(variable: &'static str, default: u64) -> Result<u64, ConfigError> {
    match env::var(variable) {
        Ok(raw) => raw.parse::<u64>().ok().filter(|value| *value > 0).ok_or(
            ConfigError::InvalidPositiveInteger {
                variable,
                value: raw,
            },
        ),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidPositiveInteger {
            variable,
            value: "<non-unicode>".into(),
        }),
    }
}

fn parse_positive_usize(variable: &'static str, raw: &str) -> Result<NonZeroUsize, ConfigError> {
    let value = raw
        .parse::<usize>()
        .ok()
        .and_then(NonZeroUsize::new)
        .ok_or_else(|| ConfigError::InvalidPositiveInteger {
            variable,
            value: raw.into(),
        })?;
    Ok(value)
}

fn backend_env_file() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env")
}

fn parse_market_coins(variable: &'static str, raw: &str) -> Result<Vec<MarketCoin>, ConfigError> {
    let mut seen = HashSet::new();
    raw.split(',')
        .enumerate()
        .map(|(index, entry)| {
            let entry = entry.trim();
            let coin = MarketCoin::try_new(entry).map_err(|source| ConfigError::InvalidCoin {
                variable,
                position: index + 1,
                source,
            })?;
            if !seen.insert(coin.clone()) {
                return Err(ConfigError::DuplicateCoin { variable, coin });
            }
            Ok(coin)
        })
        .collect()
}

#[derive(Debug)]
pub enum ConfigError {
    EnvFile(dotenvy::Error),
    MissingVariable(&'static str),
    InvalidCoin {
        variable: &'static str,
        position: usize,
        source: MarketCoinError,
    },
    DuplicateCoin {
        variable: &'static str,
        coin: MarketCoin,
    },
    InvalidPositiveInteger {
        variable: &'static str,
        value: String,
    },
    InvalidBoolean {
        variable: &'static str,
        value: String,
    },
    DataDirectoryMustBeAbsolute(PathBuf),
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnvFile(error) => write!(formatter, "failed to load backend/.env: {error}"),
            Self::MissingVariable(variable) => write!(
                formatter,
                "{variable} is not set; copy backend/.env.example to backend/.env"
            ),
            Self::InvalidCoin {
                variable,
                position,
                source,
            } => {
                write!(formatter, "invalid {variable} entry {position}: {source}")
            }
            Self::DuplicateCoin { variable, coin } => {
                write!(formatter, "duplicate {variable} entry: {coin}")
            }
            Self::InvalidPositiveInteger { variable, value } => {
                write!(
                    formatter,
                    "{variable} must be a positive integer, received {value:?}"
                )
            }
            Self::InvalidBoolean { variable, value } => {
                write!(
                    formatter,
                    "{variable} must be true or false, received {value:?}"
                )
            }
            Self::DataDirectoryMustBeAbsolute(path) => write!(
                formatter,
                "DATA_DIR must be an absolute path outside the repository, received {:?}",
                path
            ),
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EnvFile(error) => Some(error),
            Self::InvalidCoin { source, .. } => Some(source),
            Self::MissingVariable(_)
            | Self::DuplicateCoin { .. }
            | Self::InvalidPositiveInteger { .. }
            | Self::InvalidBoolean { .. }
            | Self::DataDirectoryMustBeAbsolute(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConfigError, parse_market_coins, parse_positive_usize};

    #[test]
    fn trims_entries_while_preserving_case_and_punctuation() {
        let coins = parse_market_coins("TEST_COINS", " BTC, eth, dex-name:Coin_1 ").unwrap();
        let values = coins.iter().map(|coin| coin.as_str()).collect::<Vec<_>>();
        assert_eq!(values, ["BTC", "eth", "dex-name:Coin_1"]);
    }

    #[test]
    fn rejects_empty_entries() {
        assert!(matches!(
            parse_market_coins("TEST_COINS", "BTC,,ETH"),
            Err(ConfigError::InvalidCoin { position: 2, .. })
        ));
        assert!(parse_market_coins("TEST_COINS", "   ").is_err());
    }

    #[test]
    fn rejects_internal_whitespace_control_characters_and_exact_duplicates() {
        assert!(parse_market_coins("TEST_COINS", "BTC,ET H").is_err());
        assert!(parse_market_coins("TEST_COINS", "BTC,ET\0H").is_err());
        assert!(matches!(
            parse_market_coins("TEST_COINS", "BTC,ETH,BTC"),
            Err(ConfigError::DuplicateCoin { .. })
        ));
        assert!(parse_market_coins("TEST_COINS", "BTC,btc").is_ok());
    }

    #[test]
    fn dedup_capacity_must_be_a_positive_integer() {
        assert_eq!(
            parse_positive_usize("TEST", "1000000").unwrap().get(),
            1_000_000
        );
        assert!(matches!(
            parse_positive_usize("TEST", "0"),
            Err(ConfigError::InvalidPositiveInteger { .. })
        ));
        assert!(parse_positive_usize("TEST", "many").is_err());
    }
}
