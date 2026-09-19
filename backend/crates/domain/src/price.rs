use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use crate::DecimalParseError;
use crate::decimal::ExactDecimal;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Price(ExactDecimal);

impl Price {
    pub const fn coefficient(self) -> i128 {
        self.0.coefficient()
    }

    pub const fn scale(self) -> u8 {
        self.0.scale()
    }
}

impl FromStr for Price {
    type Err = DecimalParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        ExactDecimal::parse_positive(value).map(Self)
    }
}

impl Display for Price {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}
