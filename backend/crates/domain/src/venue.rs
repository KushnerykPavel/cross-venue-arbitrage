use std::fmt::{self, Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Venue {
    Aster,
    Hyperliquid,
    Lighter,
}

impl Display for Venue {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Aster => "Aster",
            Self::Hyperliquid => "Hyperliquid",
            Self::Lighter => "Lighter",
        })
    }
}
