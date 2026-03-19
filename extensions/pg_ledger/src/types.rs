use pgrx::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

const SCALE: i128 = 10_000;
const MAX_VALUE: i128 = 999_999_999_999_999 * SCALE + 9999; // 999_999_999_999_999.9999

/// Fixed-point decimal type for financial amounts.
///
/// Stored as i128 scaled by 10,000 for exact 4-decimal-place arithmetic.
/// Range: -999,999,999,999,999.9999 to 999,999,999,999,999.9999
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    PostgresType,
    PostgresEq,
    PostgresOrd,
    PostgresHash,
)]
#[inoutfuncs]
pub struct LedgerAmount {
    value: i128,
}

impl Serialize for LedgerAmount {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.format())
    }
}

impl<'de> Deserialize<'de> for LedgerAmount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl LedgerAmount {
    /// Create from raw scaled value (value * 10000).
    pub fn from_raw(value: i128) -> Self {
        Self { value }
    }

    /// Get the raw scaled value.
    pub fn raw(&self) -> i128 {
        self.value
    }

    /// Parse a decimal string like "1500.00" into a LedgerAmount.
    /// Rejects more than 4 decimal places.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("empty input".to_string());
        }

        let negative = s.starts_with('-');
        let s = if negative || s.starts_with('+') {
            &s[1..]
        } else {
            s
        };

        let (integer_part, decimal_part) = if let Some(dot_pos) = s.find('.') {
            (&s[..dot_pos], &s[dot_pos + 1..])
        } else {
            (s, "")
        };

        if decimal_part.len() > 4 {
            return Err(format!(
                "too many decimal places ({}), maximum is 4",
                decimal_part.len()
            ));
        }

        // Validate characters
        if !integer_part.chars().all(|c| c.is_ascii_digit()) || integer_part.is_empty() {
            return Err(format!("invalid integer part: '{}'", integer_part));
        }
        if !decimal_part.is_empty() && !decimal_part.chars().all(|c| c.is_ascii_digit()) {
            return Err(format!("invalid decimal part: '{}'", decimal_part));
        }

        let integer: i128 = integer_part
            .parse()
            .map_err(|e| format!("integer overflow: {}", e))?;

        // Pad decimal to 4 places
        let decimal_str = format!("{:0<4}", decimal_part);
        let decimal: i128 = decimal_str[..4]
            .parse()
            .map_err(|e| format!("decimal parse error: {}", e))?;

        let value = integer * SCALE + decimal;

        if value > MAX_VALUE {
            return Err("value out of range".to_string());
        }

        Ok(Self {
            value: if negative { -value } else { value },
        })
    }

    /// Format as a decimal string with exactly 4 decimal places.
    pub fn format(&self) -> String {
        let abs = self.value.unsigned_abs();
        let integer = abs / SCALE as u128;
        let decimal = abs % SCALE as u128;
        let sign = if self.value < 0 { "-" } else { "" };
        format!("{}{}.{:04}", sign, integer, decimal)
    }

    pub fn is_zero(&self) -> bool {
        self.value == 0
    }

    pub fn negate(&self) -> Self {
        Self { value: -self.value }
    }

    pub fn checked_add(&self, other: &Self) -> Result<Self, String> {
        let result = self.value.checked_add(other.value).ok_or("overflow")?;
        Ok(Self { value: result })
    }

    pub fn checked_sub(&self, other: &Self) -> Result<Self, String> {
        let result = self.value.checked_sub(other.value).ok_or("overflow")?;
        Ok(Self { value: result })
    }

    /// Multiply by an integer scalar.
    pub fn checked_mul_int(&self, scalar: i64) -> Result<Self, String> {
        let result = self.value.checked_mul(scalar as i128).ok_or("overflow")?;
        Ok(Self { value: result })
    }

    /// Multiply by a numeric scalar (f64). Result truncated to 4dp.
    pub fn mul_numeric(&self, scalar: f64) -> Result<Self, String> {
        if !scalar.is_finite() {
            return Err("non-finite multiplier".to_string());
        }
        // Compute in f64, then round to i128
        let result = (self.value as f64) * scalar;
        if result.abs() > MAX_VALUE as f64 {
            return Err("overflow".to_string());
        }
        Ok(Self {
            value: result.round() as i128,
        })
    }

    /// Divide by an integer scalar.
    pub fn checked_div_int(&self, divisor: i64) -> Result<Self, String> {
        if divisor == 0 {
            return Err("division by zero".to_string());
        }
        // Use rounding division to maintain precision
        let d = divisor as i128;
        let result = if (self.value >= 0) == (d >= 0) {
            (self.value.abs() + d.abs() / 2) / d.abs()
        } else {
            -((self.value.abs() + d.abs() / 2) / d.abs())
        };
        Ok(Self { value: result })
    }

    /// Divide by a numeric scalar (f64). Result truncated to 4dp.
    pub fn div_numeric(&self, divisor: f64) -> Result<Self, String> {
        if divisor == 0.0 {
            return Err("division by zero".to_string());
        }
        if !divisor.is_finite() {
            return Err("non-finite divisor".to_string());
        }
        let result = (self.value as f64) / divisor;
        if result.abs() > MAX_VALUE as f64 {
            return Err("overflow".to_string());
        }
        Ok(Self {
            value: result.round() as i128,
        })
    }
}

impl fmt::Display for LedgerAmount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format())
    }
}

impl InOutFuncs for LedgerAmount {
    fn input(input: &core::ffi::CStr) -> Self
    where
        Self: Sized,
    {
        let s = input
            .to_str()
            .expect("invalid UTF-8 in ledger_amount input");
        Self::parse(s).unwrap_or_else(|e| {
            pgrx::error!("invalid input for ledger_amount: {}", e);
        })
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        buffer.push_str(&self.format());
    }
}

// =============================================================================
// Arithmetic pg_extern functions
// =============================================================================

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_add(a: LedgerAmount, b: LedgerAmount) -> LedgerAmount {
    a.checked_add(&b)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount overflow: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_sub(a: LedgerAmount, b: LedgerAmount) -> LedgerAmount {
    a.checked_sub(&b)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount overflow: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_neg(a: LedgerAmount) -> LedgerAmount {
    a.negate()
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_mul_int(a: LedgerAmount, b: i32) -> LedgerAmount {
    a.checked_mul_int(b as i64)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount overflow: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn int_mul_ledger(a: i32, b: LedgerAmount) -> LedgerAmount {
    b.checked_mul_int(a as i64)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount overflow: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_mul_numeric(a: LedgerAmount, b: f64) -> LedgerAmount {
    a.mul_numeric(b)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount error: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn numeric_mul_ledger(a: f64, b: LedgerAmount) -> LedgerAmount {
    b.mul_numeric(a)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount error: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_div_int(a: LedgerAmount, b: i32) -> LedgerAmount {
    a.checked_div_int(b as i64)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount error: {}", e))
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_div_numeric(a: LedgerAmount, b: f64) -> LedgerAmount {
    a.div_numeric(b)
        .unwrap_or_else(|e| pgrx::error!("ledger_amount error: {}", e))
}

// =============================================================================
// Cast functions
// =============================================================================

#[pg_extern(immutable, parallel_safe)]
pub fn numeric_to_ledger(val: f64) -> LedgerAmount {
    // Round to 4dp, then convert
    let scaled = (val * SCALE as f64).round() as i128;
    LedgerAmount { value: scaled }
}

#[pg_extern(immutable, parallel_safe)]
pub fn int_to_ledger(val: i32) -> LedgerAmount {
    LedgerAmount {
        value: (val as i128) * SCALE,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn bigint_to_ledger(val: i64) -> LedgerAmount {
    LedgerAmount {
        value: (val as i128) * SCALE,
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_to_numeric(val: LedgerAmount) -> f64 {
    val.value as f64 / SCALE as f64
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_to_text(val: LedgerAmount) -> String {
    val.format()
}

// =============================================================================
// Aggregate state functions
// =============================================================================

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_sum_state(
    state: Option<LedgerAmount>,
    value: Option<LedgerAmount>,
) -> Option<LedgerAmount> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => Some(ledger_add(s, v)),
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_min_state(
    state: Option<LedgerAmount>,
    value: Option<LedgerAmount>,
) -> Option<LedgerAmount> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => {
            if s.value <= v.value {
                Some(s)
            } else {
                Some(v)
            }
        }
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn ledger_max_state(
    state: Option<LedgerAmount>,
    value: Option<LedgerAmount>,
) -> Option<LedgerAmount> {
    match (state, value) {
        (None, None) => None,
        (Some(s), None) => Some(s),
        (None, Some(v)) => Some(v),
        (Some(s), Some(v)) => {
            if s.value >= v.value {
                Some(s)
            } else {
                Some(v)
            }
        }
    }
}

// =============================================================================
// SQL operator and cast definitions
// =============================================================================

pgrx::extension_sql!(
    r#"
-- ledger_amount + ledger_amount
CREATE OPERATOR @extschema@.+ (
    LEFTARG = @extschema@.LedgerAmount,
    RIGHTARG = @extschema@.LedgerAmount,
    FUNCTION = @extschema@.ledger_add,
    COMMUTATOR = +
);

-- ledger_amount - ledger_amount
CREATE OPERATOR @extschema@.- (
    LEFTARG = @extschema@.LedgerAmount,
    RIGHTARG = @extschema@.LedgerAmount,
    FUNCTION = @extschema@.ledger_sub
);

-- Unary minus
CREATE OPERATOR @extschema@.- (
    RIGHTARG = @extschema@.LedgerAmount,
    FUNCTION = @extschema@.ledger_neg
);

-- ledger_amount * int4
CREATE OPERATOR @extschema@.* (
    LEFTARG = @extschema@.LedgerAmount,
    RIGHTARG = int4,
    FUNCTION = @extschema@.ledger_mul_int,
    COMMUTATOR = *
);

-- int4 * ledger_amount
CREATE OPERATOR @extschema@.* (
    LEFTARG = int4,
    RIGHTARG = @extschema@.LedgerAmount,
    FUNCTION = @extschema@.int_mul_ledger,
    COMMUTATOR = *
);

-- ledger_amount * float8
CREATE OPERATOR @extschema@.* (
    LEFTARG = @extschema@.LedgerAmount,
    RIGHTARG = float8,
    FUNCTION = @extschema@.ledger_mul_numeric
);

-- float8 * ledger_amount
CREATE OPERATOR @extschema@.* (
    LEFTARG = float8,
    RIGHTARG = @extschema@.LedgerAmount,
    FUNCTION = @extschema@.numeric_mul_ledger
);

-- ledger_amount / int4
CREATE OPERATOR @extschema@./ (
    LEFTARG = @extschema@.LedgerAmount,
    RIGHTARG = int4,
    FUNCTION = @extschema@.ledger_div_int
);

-- ledger_amount / float8
CREATE OPERATOR @extschema@./ (
    LEFTARG = @extschema@.LedgerAmount,
    RIGHTARG = float8,
    FUNCTION = @extschema@.ledger_div_numeric
);

-- Explicit casts
CREATE CAST (float8 AS @extschema@.LedgerAmount)
    WITH FUNCTION @extschema@.numeric_to_ledger(float8);

CREATE CAST (int4 AS @extschema@.LedgerAmount)
    WITH FUNCTION @extschema@.int_to_ledger(int4);

CREATE CAST (int8 AS @extschema@.LedgerAmount)
    WITH FUNCTION @extschema@.bigint_to_ledger(int8);

CREATE CAST (@extschema@.LedgerAmount AS float8)
    WITH FUNCTION @extschema@.ledger_to_numeric(@extschema@.LedgerAmount);

CREATE CAST (@extschema@.LedgerAmount AS text)
    WITH FUNCTION @extschema@.ledger_to_text(@extschema@.LedgerAmount);

-- Aggregates
CREATE AGGREGATE @extschema@.sum(@extschema@.LedgerAmount) (
    SFUNC = @extschema@.ledger_sum_state,
    STYPE = @extschema@.LedgerAmount
);

CREATE AGGREGATE @extschema@.min(@extschema@.LedgerAmount) (
    SFUNC = @extschema@.ledger_min_state,
    STYPE = @extschema@.LedgerAmount
);

CREATE AGGREGATE @extschema@.max(@extschema@.LedgerAmount) (
    SFUNC = @extschema@.ledger_max_state,
    STYPE = @extschema@.LedgerAmount
);
"#,
    name = "ledger_operators_casts_aggregates",
    requires = [
        ledger_add,
        ledger_sub,
        ledger_neg,
        ledger_mul_int,
        int_mul_ledger,
        ledger_mul_numeric,
        numeric_mul_ledger,
        ledger_div_int,
        ledger_div_numeric,
        numeric_to_ledger,
        int_to_ledger,
        bigint_to_ledger,
        ledger_to_numeric,
        ledger_to_text,
        ledger_sum_state,
        ledger_min_state,
        ledger_max_state
    ]
);

// =============================================================================
// Unit Tests (pure Rust, no Postgres)
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_integer() {
        let a = LedgerAmount::parse("1500").unwrap();
        assert_eq!(a.raw(), 1500 * SCALE);
    }

    #[test]
    fn test_parse_decimal() {
        let a = LedgerAmount::parse("1500.00").unwrap();
        assert_eq!(a.raw(), 1500 * SCALE);
    }

    #[test]
    fn test_parse_four_decimals() {
        let a = LedgerAmount::parse("0.0001").unwrap();
        assert_eq!(a.raw(), 1);
    }

    #[test]
    fn test_parse_negative() {
        let a = LedgerAmount::parse("-42.5000").unwrap();
        assert_eq!(a.raw(), -425000);
    }

    #[test]
    fn test_reject_too_many_decimals() {
        let result = LedgerAmount::parse("100.12345");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too many decimal places"));
    }

    #[test]
    fn test_reject_invalid_input() {
        assert!(LedgerAmount::parse("abc").is_err());
        assert!(LedgerAmount::parse("").is_err());
    }

    #[test]
    fn test_format_positive() {
        let a = LedgerAmount::from_raw(15_000_000);
        assert_eq!(a.format(), "1500.0000");
    }

    #[test]
    fn test_format_zero() {
        let a = LedgerAmount::from_raw(0);
        assert_eq!(a.format(), "0.0000");
    }

    #[test]
    fn test_format_negative() {
        let a = LedgerAmount::from_raw(-425000);
        assert_eq!(a.format(), "-42.5000");
    }

    #[test]
    fn test_format_small_decimal() {
        let a = LedgerAmount::from_raw(1);
        assert_eq!(a.format(), "0.0001");
    }

    #[test]
    fn test_add() {
        let a = LedgerAmount::parse("100.00").unwrap();
        let b = LedgerAmount::parse("50.25").unwrap();
        let c = a.checked_add(&b).unwrap();
        assert_eq!(c.format(), "150.2500");
    }

    #[test]
    fn test_sub() {
        let a = LedgerAmount::parse("100.00").unwrap();
        let b = LedgerAmount::parse("30.50").unwrap();
        let c = a.checked_sub(&b).unwrap();
        assert_eq!(c.format(), "69.5000");
    }

    #[test]
    fn test_mul_int() {
        let a = LedgerAmount::parse("25.00").unwrap();
        let b = a.checked_mul_int(4).unwrap();
        assert_eq!(b.format(), "100.0000");
    }

    #[test]
    fn test_mul_numeric() {
        let a = LedgerAmount::parse("100.00").unwrap();
        let b = a.mul_numeric(0.21).unwrap();
        assert_eq!(b.format(), "21.0000");
    }

    #[test]
    fn test_div_int() {
        let a = LedgerAmount::parse("100.00").unwrap();
        let b = a.checked_div_int(3).unwrap();
        assert_eq!(b.format(), "33.3333");
    }

    #[test]
    fn test_div_zero() {
        let a = LedgerAmount::parse("100.00").unwrap();
        assert!(a.checked_div_int(0).is_err());
    }

    #[test]
    fn test_div_numeric() {
        let a = LedgerAmount::parse("100.00").unwrap();
        let b = a.div_numeric(3.0).unwrap();
        assert_eq!(b.format(), "33.3333");
    }

    #[test]
    fn test_negate() {
        let a = LedgerAmount::parse("42.00").unwrap();
        assert_eq!(a.negate().format(), "-42.0000");
        assert_eq!(a.negate().negate().format(), "42.0000");
    }

    #[test]
    fn test_is_zero() {
        assert!(LedgerAmount::from_raw(0).is_zero());
        assert!(!LedgerAmount::from_raw(1).is_zero());
    }

    #[test]
    fn test_comparison() {
        let a = LedgerAmount::parse("100.00").unwrap();
        let b = LedgerAmount::parse("200.00").unwrap();
        let c = LedgerAmount::parse("100.00").unwrap();
        assert!(a.raw() < b.raw());
        assert!(b.raw() > a.raw());
        assert_eq!(a.raw(), c.raw());
    }

    #[test]
    fn test_roundtrip_serde() {
        let a = LedgerAmount::parse("1234.5678").unwrap();
        let json = serde_json::to_string(&a).unwrap();
        let b: LedgerAmount = serde_json::from_str(&json).unwrap();
        assert_eq!(a.raw(), b.raw());
    }

    #[test]
    fn test_parse_with_plus_sign() {
        let a = LedgerAmount::parse("+100.00").unwrap();
        assert_eq!(a.raw(), 1_000_000);
    }

    #[test]
    fn test_parse_leading_trailing_spaces() {
        let a = LedgerAmount::parse("  100.00  ").unwrap();
        assert_eq!(a.raw(), 1_000_000);
    }
}
