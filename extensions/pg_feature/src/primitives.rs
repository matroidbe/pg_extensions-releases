//! Primitives for feature engineering
//!
//! Primitives are the building blocks for feature generation:
//! - Aggregation primitives: COUNT, SUM, AVG, etc. (applied across relationships)
//! - Transform primitives: YEAR, MONTH, ABS, etc. (applied to single columns)

use crate::schema::PgType;

/// A primitive that can generate SQL for feature calculation
pub trait Primitive: Send + Sync {
    /// Name of the primitive (e.g., "count", "sum", "year")
    fn name(&self) -> &str;

    /// Generate SQL expression for this primitive
    fn to_sql(&self, column: &str) -> String;

    /// Types this primitive can be applied to
    fn applicable_types(&self) -> Vec<PgType>;

    /// Output type of this primitive
    fn output_type(&self) -> PgType;

    /// Whether this is an aggregation primitive (vs transform)
    fn is_aggregation(&self) -> bool;

    /// Human-readable description
    fn description(&self) -> &str;
}

// =============================================================================
// Aggregation Primitives
// =============================================================================

/// COUNT - Count of non-null values
pub struct Count;
impl Primitive for Count {
    fn name(&self) -> &str {
        "count"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("COUNT({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![
            PgType::Integer,
            PgType::Numeric,
            PgType::Boolean,
            PgType::Text,
            PgType::Timestamp,
            PgType::Date,
            PgType::Other,
        ]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Count of non-null values"
    }
}

/// SUM - Sum of numeric values
pub struct Sum;
impl Primitive for Sum {
    fn name(&self) -> &str {
        "sum"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("SUM({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Integer, PgType::Numeric]
    }
    fn output_type(&self) -> PgType {
        PgType::Numeric
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Sum of numeric values"
    }
}

/// MEAN - Average of numeric values
pub struct Mean;
impl Primitive for Mean {
    fn name(&self) -> &str {
        "mean"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("AVG({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Integer, PgType::Numeric]
    }
    fn output_type(&self) -> PgType {
        PgType::Numeric
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Average of numeric values"
    }
}

/// MIN - Minimum value
pub struct Min;
impl Primitive for Min {
    fn name(&self) -> &str {
        "min"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("MIN({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![
            PgType::Integer,
            PgType::Numeric,
            PgType::Timestamp,
            PgType::Date,
            PgType::Text,
        ]
    }
    fn output_type(&self) -> PgType {
        // Returns same type as input
        PgType::Other
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Minimum value"
    }
}

/// MAX - Maximum value
pub struct Max;
impl Primitive for Max {
    fn name(&self) -> &str {
        "max"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("MAX({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![
            PgType::Integer,
            PgType::Numeric,
            PgType::Timestamp,
            PgType::Date,
            PgType::Text,
        ]
    }
    fn output_type(&self) -> PgType {
        PgType::Other
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Maximum value"
    }
}

/// STD - Standard deviation
pub struct Std;
impl Primitive for Std {
    fn name(&self) -> &str {
        "std"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("STDDEV({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Integer, PgType::Numeric]
    }
    fn output_type(&self) -> PgType {
        PgType::Numeric
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Standard deviation"
    }
}

/// MODE - Most common value
pub struct Mode;
impl Primitive for Mode {
    fn name(&self) -> &str {
        "mode"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("MODE() WITHIN GROUP (ORDER BY {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![
            PgType::Integer,
            PgType::Numeric,
            PgType::Boolean,
            PgType::Text,
        ]
    }
    fn output_type(&self) -> PgType {
        PgType::Other
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Most common value"
    }
}

/// NUM_UNIQUE - Count of distinct values
pub struct NumUnique;
impl Primitive for NumUnique {
    fn name(&self) -> &str {
        "num_unique"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("COUNT(DISTINCT {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![
            PgType::Integer,
            PgType::Numeric,
            PgType::Boolean,
            PgType::Text,
            PgType::Timestamp,
            PgType::Date,
        ]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Count of distinct values"
    }
}

/// PERCENT_TRUE - Percentage of true values (for boolean columns)
pub struct PercentTrue;
impl Primitive for PercentTrue {
    fn name(&self) -> &str {
        "percent_true"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("(AVG({}::int) * 100)", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Boolean]
    }
    fn output_type(&self) -> PgType {
        PgType::Numeric
    }
    fn is_aggregation(&self) -> bool {
        true
    }
    fn description(&self) -> &str {
        "Percentage of true values"
    }
}

// =============================================================================
// Transform Primitives
// =============================================================================

/// YEAR - Extract year from timestamp/date
pub struct Year;
impl Primitive for Year {
    fn name(&self) -> &str {
        "year"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("EXTRACT(YEAR FROM {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Timestamp, PgType::Date]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Extract year from date/timestamp"
    }
}

/// MONTH - Extract month from timestamp/date
pub struct Month;
impl Primitive for Month {
    fn name(&self) -> &str {
        "month"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("EXTRACT(MONTH FROM {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Timestamp, PgType::Date]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Extract month from date/timestamp"
    }
}

/// DAY - Extract day from timestamp/date
pub struct Day;
impl Primitive for Day {
    fn name(&self) -> &str {
        "day"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("EXTRACT(DAY FROM {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Timestamp, PgType::Date]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Extract day from date/timestamp"
    }
}

/// WEEKDAY - Extract day of week from timestamp/date
pub struct Weekday;
impl Primitive for Weekday {
    fn name(&self) -> &str {
        "weekday"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("EXTRACT(DOW FROM {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Timestamp, PgType::Date]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Extract day of week (0=Sunday, 6=Saturday)"
    }
}

/// HOUR - Extract hour from timestamp
pub struct Hour;
impl Primitive for Hour {
    fn name(&self) -> &str {
        "hour"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("EXTRACT(HOUR FROM {})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Timestamp, PgType::Time]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Extract hour from timestamp/time"
    }
}

/// IS_WEEKEND - Check if date/timestamp is on weekend
pub struct IsWeekend;
impl Primitive for IsWeekend {
    fn name(&self) -> &str {
        "is_weekend"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("(EXTRACT(DOW FROM {}) IN (0, 6))", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Timestamp, PgType::Date]
    }
    fn output_type(&self) -> PgType {
        PgType::Boolean
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Whether date is on weekend"
    }
}

/// ABSOLUTE - Absolute value of numeric
pub struct Absolute;
impl Primitive for Absolute {
    fn name(&self) -> &str {
        "absolute"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("ABS({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Integer, PgType::Numeric]
    }
    fn output_type(&self) -> PgType {
        PgType::Numeric
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Absolute value"
    }
}

/// NUM_CHARACTERS - Length of text
pub struct NumCharacters;
impl Primitive for NumCharacters {
    fn name(&self) -> &str {
        "num_characters"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("LENGTH({})", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Text]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Number of characters in text"
    }
}

/// NUM_WORDS - Word count in text
pub struct NumWords;
impl Primitive for NumWords {
    fn name(&self) -> &str {
        "num_words"
    }
    fn to_sql(&self, column: &str) -> String {
        format!(
            "COALESCE(array_length(regexp_split_to_array(NULLIF(TRIM({}), ''), '\\s+'), 1), 0)",
            column
        )
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![PgType::Text]
    }
    fn output_type(&self) -> PgType {
        PgType::Integer
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Number of words in text"
    }
}

/// IS_NULL - Check if value is null
pub struct IsNull;
impl Primitive for IsNull {
    fn name(&self) -> &str {
        "is_null"
    }
    fn to_sql(&self, column: &str) -> String {
        format!("({} IS NULL)", column)
    }
    fn applicable_types(&self) -> Vec<PgType> {
        vec![
            PgType::Integer,
            PgType::Numeric,
            PgType::Boolean,
            PgType::Text,
            PgType::Timestamp,
            PgType::Date,
            PgType::Other,
        ]
    }
    fn output_type(&self) -> PgType {
        PgType::Boolean
    }
    fn is_aggregation(&self) -> bool {
        false
    }
    fn description(&self) -> &str {
        "Whether value is null"
    }
}

// =============================================================================
// Primitive Registry
// =============================================================================

/// Get all available aggregation primitives
pub fn all_aggregation_primitives() -> Vec<Box<dyn Primitive>> {
    vec![
        Box::new(Count),
        Box::new(Sum),
        Box::new(Mean),
        Box::new(Min),
        Box::new(Max),
        Box::new(Std),
        Box::new(Mode),
        Box::new(NumUnique),
        Box::new(PercentTrue),
    ]
}

/// Get all available transform primitives
pub fn all_transform_primitives() -> Vec<Box<dyn Primitive>> {
    vec![
        Box::new(Year),
        Box::new(Month),
        Box::new(Day),
        Box::new(Weekday),
        Box::new(Hour),
        Box::new(IsWeekend),
        Box::new(Absolute),
        Box::new(NumCharacters),
        Box::new(NumWords),
        Box::new(IsNull),
    ]
}

/// Get primitives by name from a list of names
pub fn get_primitives_by_name(names: &[String], aggregation: bool) -> Vec<Box<dyn Primitive>> {
    let all = if aggregation {
        all_aggregation_primitives()
    } else {
        all_transform_primitives()
    };

    all.into_iter()
        .filter(|p| names.iter().any(|n| n.to_lowercase() == p.name()))
        .collect()
}

/// Check if a primitive is applicable to a given column type
pub fn is_applicable(primitive: &dyn Primitive, column_type: PgType) -> bool {
    primitive.applicable_types().contains(&column_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_to_sql() {
        let count = Count;
        assert_eq!(count.to_sql("amount"), "COUNT(amount)");
        assert!(count.is_aggregation());
    }

    #[test]
    fn test_sum_to_sql() {
        let sum = Sum;
        assert_eq!(sum.to_sql("total"), "SUM(total)");
    }

    #[test]
    fn test_year_to_sql() {
        let year = Year;
        assert_eq!(year.to_sql("created_at"), "EXTRACT(YEAR FROM created_at)");
        assert!(!year.is_aggregation());
    }

    #[test]
    fn test_is_weekend_to_sql() {
        let is_weekend = IsWeekend;
        assert_eq!(
            is_weekend.to_sql("order_date"),
            "(EXTRACT(DOW FROM order_date) IN (0, 6))"
        );
    }

    #[test]
    fn test_applicable_types() {
        let sum = Sum;
        assert!(sum.applicable_types().contains(&PgType::Integer));
        assert!(sum.applicable_types().contains(&PgType::Numeric));
        assert!(!sum.applicable_types().contains(&PgType::Text));
    }

    #[test]
    fn test_get_primitives_by_name() {
        let names = vec!["count".to_string(), "sum".to_string()];
        let prims = get_primitives_by_name(&names, true);
        assert_eq!(prims.len(), 2);
    }
}
