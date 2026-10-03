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

    /// Adds two positive decimals exactly, returning `None` if the result
    /// cannot be represented in the supported 18-decimal comparison range.
    pub(crate) fn checked_add(self, other: Self) -> Option<Self> {
        let mut coefficient = self
            .comparison_units()
            .checked_add(other.comparison_units())?;
        let mut scale = MAX_SCALE;

        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }

        Some(Self { coefficient, scale })
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
    use proptest::prelude::*;

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
    fn checked_add_handles_different_scales_exactly() {
        let left = ExactDecimal::parse_positive("1.20").unwrap();
        let right = ExactDecimal::parse_positive("2.003").unwrap();

        assert_eq!(left.checked_add(right).unwrap().to_string(), "3.203");
    }

    #[test]
    fn checked_add_reports_comparison_range_overflow() {
        let value = ExactDecimal::parse_positive("100000000000000000000").unwrap();

        assert!(value.checked_add(value).is_none());
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

    proptest! {
        #[test]
        fn positive_decimal_parse_display_round_trips(value in "[0-9]{1,10}(\\.[0-9]{1,18})?") {
            let Ok(parsed) = ExactDecimal::parse_positive(&value) else {
                prop_assume!(false);
                return Ok(());
            };

            let formatted = parsed.to_string();
            prop_assert_eq!(ExactDecimal::parse_positive(&formatted).unwrap(), parsed);
        }

        #[test]
        fn decimal_comparison_is_scale_independent(
            whole in 1_u32..1_000_000,
            fractional in 0_u32..1_000,
            scale in 1_usize..=3,
        ) {
            let value = format!("{whole}.{fractional:0width$}", width = scale);
            let same_value_with_extra_zero = format!("{value}0");
            let left = ExactDecimal::parse_positive(&value).unwrap();
            let right = ExactDecimal::parse_positive(&same_value_with_extra_zero).unwrap();
            prop_assert_eq!(left, right);
            prop_assert_eq!(left.cmp(&right), std::cmp::Ordering::Equal);
        }

        #[test]
        fn checked_add_is_commutative_and_preserves_exact_scale(
            left_whole in 1_u32..1_000_000,
            right_whole in 1_u32..1_000_000,
            left_fraction in 0_u32..1_000_000,
            right_fraction in 0_u32..1_000_000,
            left_scale in 0_u32..=6,
            right_scale in 0_u32..=6,
        ) {
            let left = ExactDecimal::parse_positive(&scaled_decimal(left_whole, left_fraction, left_scale)).unwrap();
            let right = ExactDecimal::parse_positive(&scaled_decimal(right_whole, right_fraction, right_scale)).unwrap();
            let forward = left.checked_add(right).unwrap();
            let reverse = right.checked_add(left).unwrap();

            prop_assert_eq!(forward, reverse);
            prop_assert_eq!(forward.comparison_units(), left.comparison_units() + right.comparison_units());
        }
    }

    fn scaled_decimal(whole: u32, fraction: u32, scale: u32) -> String {
        if scale == 0 {
            return whole.to_string();
        }

        let divisor = 10_u32.pow(scale);
        format!(
            "{whole}.{:0width$}",
            fraction % divisor,
            width = scale as usize
        )
    }
}
