//! Primitives for feature engineering
//!
//! Primitives are the building blocks for feature generation:
//! - Aggregation primitives: COUNT, SUM, AVG, etc. (applied across relationships)
//! - Transform primitives: YEAR, MONTH, ABS, etc. (applied to single columns)
//!
//! Primitives are stateless unit structs; the registry hands out `&'static`
//! references so feature specs can be copied freely.

use crate::schema::PgType;

/// What a primitive is applied to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Input {
    /// One feature per relationship; `SqlCtx::column` is the child's key column
    Rows,
    /// One feature per relationship, computed from the child's time index
    TimeIndex,
    /// One feature per applicable column
    Column,
}

/// SQL expressions a primitive is rendered against
#[derive(Debug, Clone, Copy)]
pub struct SqlCtx<'a> {
    /// The input column (for `Input::Rows`, the child's key column)
    pub column: &'a str,
    /// The table's time index column, if it has one
    pub time: Option<&'a str>,
    /// The cutoff the feature is computed as of (`now()` when there is none)
    pub cutoff: &'a str,
}

impl<'a> SqlCtx<'a> {
    #[cfg(test)]
    pub fn column(column: &'a str) -> Self {
        SqlCtx {
            column,
            time: None,
            cutoff: "now()",
        }
    }

    fn time(&self) -> &'a str {
        self.time.unwrap_or(self.column)
    }
}

/// A primitive that can generate SQL for feature calculation
pub trait Primitive: Send + Sync {
    /// Name of the primitive (e.g., "count", "sum", "year")
    fn name(&self) -> &'static str;

    /// Generate SQL expression for this primitive
    fn to_sql(&self, ctx: &SqlCtx) -> String;

    /// What the primitive is applied to
    fn input(&self) -> Input {
        Input::Column
    }

    /// Column types this primitive can be applied to (for `Input::Column`)
    fn applicable_types(&self) -> &'static [PgType];

    /// Whether the source table must have a time index
    fn needs_time_index(&self) -> bool {
        self.input() == Input::TimeIndex
    }

    /// Output type given the input column type
    fn output_type(&self, input: PgType) -> PgType;

    /// Whether this is an aggregation primitive (vs transform)
    fn is_aggregation(&self) -> bool;

    /// Counts are 0, not NULL, for an entity without child rows
    fn is_count(&self) -> bool {
        matches!(self.name(), "count" | "num_unique")
    }

    /// Human-readable description
    fn description(&self) -> &'static str;
}

impl std::fmt::Debug for dyn Primitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A shared reference to a registered primitive
pub type Prim = &'static dyn Primitive;

const NUMERIC: &[PgType] = &[PgType::Integer, PgType::Numeric];
const TEMPORAL: &[PgType] = &[PgType::Timestamp, PgType::Date];
const CATEGORICAL: &[PgType] = &[
    PgType::Integer,
    PgType::Numeric,
    PgType::Boolean,
    PgType::Text,
];
const ANY: &[PgType] = &[
    PgType::Integer,
    PgType::Numeric,
    PgType::Boolean,
    PgType::Text,
    PgType::Timestamp,
    PgType::Date,
    PgType::Time,
    PgType::Json,
    PgType::Array,
    PgType::Other,
];

/// Seconds in an interval expression
fn epoch(interval: &str) -> String {
    format!("EXTRACT(EPOCH FROM ({}))", interval)
}

/// Declare a primitive as a unit struct with a fixed SQL template
macro_rules! primitive {
    (
        $ty:ident, $name:literal, $desc:literal, agg: $agg:literal,
        input: $input:expr, types: $types:expr, time: $time:literal,
        out: |$in:ident| $out:expr, sql: |$ctx:ident| $sql:expr
    ) => {
        pub struct $ty;
        impl Primitive for $ty {
            fn name(&self) -> &'static str {
                $name
            }
            fn to_sql(&self, $ctx: &SqlCtx) -> String {
                $sql
            }
            fn input(&self) -> Input {
                $input
            }
            fn applicable_types(&self) -> &'static [PgType] {
                $types
            }
            fn needs_time_index(&self) -> bool {
                $time || $input == Input::TimeIndex
            }
            fn output_type(&self, $in: PgType) -> PgType {
                $out
            }
            fn is_aggregation(&self) -> bool {
                $agg
            }
            fn description(&self) -> &'static str {
                $desc
            }
        }
    };
}

// =============================================================================
// Aggregation Primitives
// =============================================================================

primitive!(Count, "count", "Number of rows", agg: true,
    input: Input::Rows, types: &[], time: false,
    out: |_t| PgType::Integer, sql: |c| format!("COUNT({})", c.column));

primitive!(Sum, "sum", "Sum of numeric values", agg: true,
    input: Input::Column, types: NUMERIC, time: false,
    out: |_t| PgType::Numeric, sql: |c| format!("SUM({})", c.column));

primitive!(Mean, "mean", "Average of numeric values", agg: true,
    input: Input::Column, types: NUMERIC, time: false,
    out: |_t| PgType::Numeric, sql: |c| format!("AVG({})", c.column));

primitive!(Min, "min", "Minimum value", agg: true,
    input: Input::Column, types: NUMERIC, time: false,
    out: |t| t, sql: |c| format!("MIN({})", c.column));

primitive!(Max, "max", "Maximum value", agg: true,
    input: Input::Column, types: NUMERIC, time: false,
    out: |t| t, sql: |c| format!("MAX({})", c.column));

primitive!(Std, "std", "Standard deviation", agg: true,
    input: Input::Column, types: NUMERIC, time: false,
    out: |_t| PgType::Numeric, sql: |c| format!("STDDEV({})", c.column));

primitive!(Median, "median", "Median value", agg: true,
    input: Input::Column, types: NUMERIC, time: false,
    out: |_t| PgType::Numeric,
    sql: |c| format!("percentile_cont(0.5) WITHIN GROUP (ORDER BY {})", c.column));

primitive!(Mode, "mode", "Most common value", agg: true,
    input: Input::Column, types: CATEGORICAL, time: false,
    out: |t| t, sql: |c| format!("MODE() WITHIN GROUP (ORDER BY {})", c.column));

primitive!(NumUnique, "num_unique", "Count of distinct values", agg: true,
    input: Input::Column,
    types: &[PgType::Integer, PgType::Numeric, PgType::Boolean, PgType::Text, PgType::Timestamp, PgType::Date],
    time: false,
    out: |_t| PgType::Integer, sql: |c| format!("COUNT(DISTINCT {})", c.column));

primitive!(PercentTrue, "percent_true", "Percentage of true values", agg: true,
    input: Input::Column, types: &[PgType::Boolean], time: false,
    out: |_t| PgType::Numeric, sql: |c| format!("(AVG(({})::int) * 100)", c.column));

primitive!(AnyTrue, "any", "Whether any value is true", agg: true,
    input: Input::Column, types: &[PgType::Boolean], time: false,
    out: |_t| PgType::Boolean, sql: |c| format!("bool_or({})", c.column));

primitive!(AllTrue, "all", "Whether all values are true", agg: true,
    input: Input::Column, types: &[PgType::Boolean], time: false,
    out: |_t| PgType::Boolean, sql: |c| format!("bool_and({})", c.column));

primitive!(First, "first", "Earliest value by time index", agg: true,
    input: Input::Column, types: CATEGORICAL, time: true,
    out: |t| t,
    sql: |c| format!("(array_agg({} ORDER BY {} ASC NULLS LAST))[1]", c.column, c.time()));

primitive!(Last, "last", "Latest value by time index", agg: true,
    input: Input::Column, types: CATEGORICAL, time: true,
    out: |t| t,
    sql: |c| format!("(array_agg({} ORDER BY {} DESC NULLS LAST))[1]", c.column, c.time()));

primitive!(Trend, "trend", "Slope of a linear fit over time (change per day)", agg: true,
input: Input::Column, types: NUMERIC, time: true,
out: |_t| PgType::Numeric,
sql: |c| format!(
    "regr_slope(({})::float8, EXTRACT(EPOCH FROM {})::float8 / 86400)",
    c.column,
    c.time()
));

primitive!(TimeSinceLast, "time_since_last", "Seconds from the latest row to the cutoff", agg: true,
    input: Input::TimeIndex, types: &[], time: true,
    out: |_t| PgType::Numeric,
    sql: |c| epoch(&format!("{} - MAX({})", c.cutoff, c.time())));

primitive!(TimeSinceFirst, "time_since_first", "Seconds from the earliest row to the cutoff", agg: true,
    input: Input::TimeIndex, types: &[], time: true,
    out: |_t| PgType::Numeric,
    sql: |c| epoch(&format!("{} - MIN({})", c.cutoff, c.time())));

primitive!(AvgTimeBetween, "avg_time_between", "Average seconds between consecutive rows", agg: true,
input: Input::TimeIndex, types: &[], time: true,
out: |_t| PgType::Numeric,
// Difference of epochs, not epoch of a difference: two dates subtract to an integer
sql: |c| format!(
    "((EXTRACT(EPOCH FROM MAX({t})) - EXTRACT(EPOCH FROM MIN({t}))) / NULLIF(COUNT({t}) - 1, 0))",
    t = c.time()
));

// =============================================================================
// Transform Primitives
// =============================================================================

macro_rules! extract {
    ($ty:ident, $name:literal, $field:literal, $desc:literal, $types:expr) => {
        primitive!($ty, $name, $desc, agg: false,
            input: Input::Column, types: $types, time: false,
            out: |_t| PgType::Integer,
            sql: |c| format!(concat!("EXTRACT(", $field, " FROM {})"), c.column));
    };
}

extract!(
    Year,
    "year",
    "YEAR",
    "Extract year from date/timestamp",
    TEMPORAL
);
extract!(
    Quarter,
    "quarter",
    "QUARTER",
    "Extract quarter (1-4) from date/timestamp",
    TEMPORAL
);
extract!(
    Month,
    "month",
    "MONTH",
    "Extract month from date/timestamp",
    TEMPORAL
);
extract!(
    Week,
    "week",
    "WEEK",
    "Extract ISO week of year from date/timestamp",
    TEMPORAL
);
extract!(
    Day,
    "day",
    "DAY",
    "Extract day from date/timestamp",
    TEMPORAL
);
extract!(
    Weekday,
    "weekday",
    "DOW",
    "Extract day of week (0=Sunday, 6=Saturday)",
    TEMPORAL
);
extract!(
    Hour,
    "hour",
    "HOUR",
    "Extract hour from timestamp/time",
    &[PgType::Timestamp, PgType::Time]
);
extract!(
    Minute,
    "minute",
    "MINUTE",
    "Extract minute from timestamp/time",
    &[PgType::Timestamp, PgType::Time]
);

primitive!(IsWeekend, "is_weekend", "Whether date is on weekend", agg: false,
    input: Input::Column, types: TEMPORAL, time: false,
    out: |_t| PgType::Boolean,
    sql: |c| format!("(EXTRACT(DOW FROM {}) IN (0, 6))", c.column));

primitive!(TimeSince, "time_since", "Seconds from the value to the cutoff", agg: false,
    input: Input::Column, types: TEMPORAL, time: false,
    out: |_t| PgType::Numeric,
    sql: |c| epoch(&format!("{} - {}", c.cutoff, c.column)));

primitive!(Absolute, "absolute", "Absolute value", agg: false,
    input: Input::Column, types: NUMERIC, time: false,
    out: |t| t, sql: |c| format!("ABS({})", c.column));

primitive!(NumCharacters, "num_characters", "Number of characters in text", agg: false,
    input: Input::Column, types: &[PgType::Text], time: false,
    out: |_t| PgType::Integer, sql: |c| format!("LENGTH({})", c.column));

primitive!(NumWords, "num_words", "Number of words in text", agg: false,
input: Input::Column, types: &[PgType::Text], time: false,
out: |_t| PgType::Integer,
sql: |c| format!(
    "COALESCE(array_length(regexp_split_to_array(NULLIF(TRIM({}), ''), '\\s+'), 1), 0)",
    c.column
));

primitive!(IsNull, "is_null", "Whether value is null", agg: false,
    input: Input::Column, types: ANY, time: false,
    out: |_t| PgType::Boolean, sql: |c| format!("({} IS NULL)", c.column));

// =============================================================================
// Primitive Registry
// =============================================================================

static AGGREGATIONS: &[Prim] = &[
    &Count,
    &Sum,
    &Mean,
    &Min,
    &Max,
    &Std,
    &Median,
    &Mode,
    &NumUnique,
    &PercentTrue,
    &AnyTrue,
    &AllTrue,
    &First,
    &Last,
    &Trend,
    &TimeSinceLast,
    &TimeSinceFirst,
    &AvgTimeBetween,
];

static TRANSFORMS: &[Prim] = &[
    &Year,
    &Quarter,
    &Month,
    &Week,
    &Day,
    &Weekday,
    &Hour,
    &Minute,
    &IsWeekend,
    &TimeSince,
    &Absolute,
    &NumCharacters,
    &NumWords,
    &IsNull,
];

/// Outer primitives allowed when stacking at depth 2
pub const STACKABLE_OUTER: &[&str] = &["sum", "mean", "std", "median", "min", "max"];

/// Get all available aggregation primitives
pub fn all_aggregation_primitives() -> &'static [Prim] {
    AGGREGATIONS
}

/// Get all available transform primitives
pub fn all_transform_primitives() -> &'static [Prim] {
    TRANSFORMS
}

/// Get primitives by name, in registry order.
///
/// Unknown names are an error so a typo does not silently drop features.
pub fn get_primitives_by_name(names: &[String], aggregation: bool) -> Result<Vec<Prim>, String> {
    let all = if aggregation {
        AGGREGATIONS
    } else {
        TRANSFORMS
    };
    let wanted: Vec<String> = names.iter().map(|n| n.trim().to_lowercase()).collect();
    if let Some(unknown) = wanted
        .iter()
        .find(|n| !n.is_empty() && !all.iter().any(|p| p.name() == n.as_str()))
    {
        let known: Vec<&str> = all.iter().map(|p| p.name()).collect();
        return Err(format!(
            "unknown {} primitive '{}' (available: {})",
            if aggregation {
                "aggregation"
            } else {
                "transform"
            },
            unknown,
            known.join(", ")
        ));
    }
    Ok(all
        .iter()
        .copied()
        .filter(|p| wanted.iter().any(|n| n == p.name()))
        .collect())
}

/// Check if a column-input primitive is applicable to a given column type
pub fn is_applicable(primitive: &dyn Primitive, column_type: PgType) -> bool {
    primitive.input() == Input::Column && primitive.applicable_types().contains(&column_type)
}

/// Whether a primitive's output type is numeric (can feed an outer aggregation)
pub fn is_numeric(t: PgType) -> bool {
    matches!(t, PgType::Integer | PgType::Numeric)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(prims: &[Prim]) -> Vec<&'static str> {
        prims.iter().map(|p| p.name()).collect()
    }

    #[test]
    fn test_count_counts_rows_of_key() {
        assert_eq!(
            Count.to_sql(&SqlCtx::column("child.cid")),
            "COUNT(child.cid)"
        );
        assert_eq!(Count.input(), Input::Rows);
        assert!(Count.is_aggregation());
    }

    #[test]
    fn test_sum_to_sql() {
        assert_eq!(Sum.to_sql(&SqlCtx::column("total")), "SUM(total)");
    }

    #[test]
    fn test_year_to_sql() {
        assert_eq!(
            Year.to_sql(&SqlCtx::column("created_at")),
            "EXTRACT(YEAR FROM created_at)"
        );
        assert!(!Year.is_aggregation());
    }

    #[test]
    fn test_is_weekend_to_sql() {
        assert_eq!(
            IsWeekend.to_sql(&SqlCtx::column("order_date")),
            "(EXTRACT(DOW FROM order_date) IN (0, 6))"
        );
    }

    #[test]
    fn test_time_primitives_use_cutoff_and_time_index() {
        let ctx = SqlCtx {
            column: "child.cid",
            time: Some("child.ts"),
            cutoff: "ct.cutoff_time",
        };
        assert_eq!(
            TimeSinceLast.to_sql(&ctx),
            "EXTRACT(EPOCH FROM (ct.cutoff_time - MAX(child.ts)))"
        );
        assert_eq!(
            TimeSinceFirst.to_sql(&ctx),
            "EXTRACT(EPOCH FROM (ct.cutoff_time - MIN(child.ts)))"
        );
        assert_eq!(
            AvgTimeBetween.to_sql(&ctx),
            "((EXTRACT(EPOCH FROM MAX(child.ts)) - EXTRACT(EPOCH FROM MIN(child.ts))) / NULLIF(COUNT(child.ts) - 1, 0))"
        );
        assert!(TimeSinceLast.needs_time_index());
        assert_eq!(TimeSinceLast.input(), Input::TimeIndex);
    }

    #[test]
    fn test_first_last_trend_need_time_index() {
        let ctx = SqlCtx {
            column: "child.amount",
            time: Some("child.ts"),
            cutoff: "now()",
        };
        assert_eq!(
            Last.to_sql(&ctx),
            "(array_agg(child.amount ORDER BY child.ts DESC NULLS LAST))[1]"
        );
        assert_eq!(
            Trend.to_sql(&ctx),
            "regr_slope((child.amount)::float8, EXTRACT(EPOCH FROM child.ts)::float8 / 86400)"
        );
        assert!(First.needs_time_index());
        assert!(Last.needs_time_index());
        assert!(Trend.needs_time_index());
        assert!(!Sum.needs_time_index());
    }

    #[test]
    fn test_time_since_transform() {
        let ctx = SqlCtx {
            column: "t.signup",
            time: None,
            cutoff: "'2024-01-01'::timestamptz",
        };
        assert_eq!(
            TimeSince.to_sql(&ctx),
            "EXTRACT(EPOCH FROM ('2024-01-01'::timestamptz - t.signup))"
        );
    }

    #[test]
    fn test_applicable_types() {
        assert!(is_applicable(&Sum, PgType::Integer));
        assert!(is_applicable(&Sum, PgType::Numeric));
        assert!(!is_applicable(&Sum, PgType::Text));
        // min/max are numeric only: a MIN over text or timestamps is not an ML feature
        assert!(!is_applicable(&Min, PgType::Text));
        assert!(!is_applicable(&Max, PgType::Timestamp));
        // row/time-input primitives never apply to a column
        assert!(!is_applicable(&Count, PgType::Integer));
        assert!(!is_applicable(&TimeSinceLast, PgType::Timestamp));
    }

    #[test]
    fn test_output_types() {
        assert_eq!(Max.output_type(PgType::Integer), PgType::Integer);
        assert_eq!(Mode.output_type(PgType::Text), PgType::Text);
        assert_eq!(Mean.output_type(PgType::Integer), PgType::Numeric);
        assert_eq!(Count.output_type(PgType::Other), PgType::Integer);
    }

    #[test]
    fn test_get_primitives_by_name_in_registry_order() {
        let wanted = vec!["sum".to_string(), "COUNT".to_string(), " max ".to_string()];
        let prims = get_primitives_by_name(&wanted, true).unwrap();
        assert_eq!(names(&prims), vec!["count", "sum", "max"]);
    }

    #[test]
    fn test_get_primitives_by_name_rejects_unknown() {
        let err = get_primitives_by_name(&["average".to_string()], true).unwrap_err();
        assert!(
            err.contains("unknown aggregation primitive 'average'"),
            "{err}"
        );
        // transform names are not aggregation names
        assert!(get_primitives_by_name(&["year".to_string()], true).is_err());
    }

    #[test]
    fn test_registry_names_unique() {
        let mut all = names(all_aggregation_primitives());
        all.extend(names(all_transform_primitives()));
        let mut dedup = all.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(all.len(), dedup.len());
    }

    #[test]
    fn test_stackable_outer_are_numeric_column_aggregations() {
        for name in STACKABLE_OUTER {
            let p = get_primitives_by_name(&[name.to_string()], true).unwrap()[0];
            assert!(is_applicable(p, PgType::Numeric), "{name}");
            assert!(!p.needs_time_index(), "{name}");
        }
    }
}
