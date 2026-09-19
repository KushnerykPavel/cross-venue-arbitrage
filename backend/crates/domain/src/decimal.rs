use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

const MAX_SCALE: u8 = 18;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ExactDecimal {
    coefficient: i128,
    scale: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalParseError {
    InvalidFormat,
    NonPositive,
    ScaleTooLarge,
    OutOfRange,
}

impl ExactDecimal {
    pub(crate) fn parse_positive(value: &str) -> Result<Self, DecimalParseError> {
        if value.is_empty() || value.starts_with(['+', '-']) {
            return Err(DecimalParseError::InvalidFormat);
        }

        let mut parts = value.split('.');
        let whole = parts.next().ok_or(DecimalParseError::InvalidFormat)?;
        let fractional = parts.next();
        if parts.next().is_some()
            || whole.is_empty()
            || !whole.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(DecimalParseError::InvalidFormat);
        }

        let fractional = fractional.unwrap_or("");
        if value.contains('.')
            && (fractional.is_empty() || !fractional.bytes().all(|byte| byte.is_ascii_digit()))
        {
            return Err(DecimalParseError::InvalidFormat);
        }

        let mut scale =
            u8::try_from(fractional.len()).map_err(|_| DecimalParseError::ScaleTooLarge)?;
        if scale > MAX_SCALE {
            return Err(DecimalParseError::ScaleTooLarge);
        }

        let digits = format!("{whole}{fractional}");
        let mut coefficient = digits
            .parse::<i128>()
            .map_err(|_| DecimalParseError::OutOfRange)?;

        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }

        if coefficient == 0 {
            return Err(DecimalParseError::NonPositive);
        }

        coefficient
            .checked_mul(power_of_ten(MAX_SCALE - scale))
            .ok_or(DecimalParseError::OutOfRange)?;

        Ok(Self { coefficient, scale })
    }

    pub(crate) const fn coefficient(self) -> i128 {
        self.coefficient
    }

    pub(crate) const fn scale(self) -> u8 {
        self.scale
    }

    fn comparison_units(self) -> i128 {
        self.coefficient
            .checked_mul(power_of_ten(MAX_SCALE - self.scale))
            .expect("exact decimal constructor guarantees comparable range")
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.comparison_units().cmp(&other.comparison_units())
    }
}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for ExactDecimal {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(formatter, "{}", self.coefficient);
        }

        let digits = self.coefficient.to_string();
        let scale = usize::from(self.scale);
        if digits.len() <= scale {
            write!(formatter, "0.{:0>width$}", digits, width = scale)
        } else {
            let split = digits.len() - scale;
            write!(formatter, "{}.{}", &digits[..split], &digits[split..])
        }
    }
}

impl Display for DecimalParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat => formatter.write_str("invalid decimal format"),
            Self::NonPositive => formatter.write_str("value must be positive"),
            Self::ScaleTooLarge => formatter.write_str("decimal scale exceeds 18 digits"),
            Self::OutOfRange => formatter.write_str("decimal value exceeds supported range"),
        }
    }
}

impl Error for DecimalParseError {}

const fn power_of_ten(exponent: u8) -> i128 {
    10_i128.pow(exponent as u32)
}

#[cfg(test)]
mod tests {
    use super::{DecimalParseError, ExactDecimal};

    #[test]
    fn normalizes_leading_and_trailing_zeroes() {
        let value = ExactDecimal::parse_positive("00012.3400").unwrap();

        assert_eq!(value.coefficient(), 1234);
        assert_eq!(value.scale(), 2);
        assert_eq!(value.to_string(), "12.34");
    }

    #[test]
    fn compares_values_with_different_scales_exactly() {
        let smaller = ExactDecimal::parse_positive("1.09").unwrap();
        let larger = ExactDecimal::parse_positive("1.1").unwrap();

        assert!(smaller < larger);
    }

    #[test]
    fn rejects_zero_negative_exponents_and_excess_scale() {
        assert_eq!(
            ExactDecimal::parse_positive("0"),
            Err(DecimalParseError::NonPositive)
        );
        assert_eq!(
            ExactDecimal::parse_positive("-1"),
            Err(DecimalParseError::InvalidFormat)
        );
        assert_eq!(
            ExactDecimal::parse_positive("1e2"),
            Err(DecimalParseError::InvalidFormat)
        );
        assert_eq!(
            ExactDecimal::parse_positive("0.1234567890123456789"),
            Err(DecimalParseError::ScaleTooLarge)
        );
    }
}
