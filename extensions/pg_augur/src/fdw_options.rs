//! Parse and validate FDW column and table OPTIONS for feature views.
//!
//! Column OPTIONS define per-column feature engineering (impute, encode, scale, etc.).
//! Table OPTIONS define the stage kind (features vs compare/tune/create) and chain links.

use crate::error::AugurPgError;
use pgrx::pg_sys;
use std::ffi::CStr;

// ─── Stage classification ──────────────────────

/// The kind of pipeline stage this foreign table represents.
#[derive(Debug, Clone, PartialEq)]
pub enum StageKind {
    /// Feature engineering only: reads from a real table, column OPTIONS define transforms.
    /// SELECT returns preprocessed data.
    Features,
    /// Combined: features + action in one table (source_table + action).
    /// Shorthand for the common "define features and train" pattern.
    /// SELECT runs the action and returns its results.
    FeaturesWithAction(ActionKind),
    /// Chained action stage: reads from a previous foreign table (source_view + action).
    Action(ActionKind),
}

/// The ML action to run.
#[derive(Debug, Clone, PartialEq)]
pub enum ActionKind {
    /// AutoML compare: trains all models, returns leaderboard.
    Compare,
    /// Hyperparameter tuning: tunes the best model from upstream.
    Tune,
    /// Model creation: builds final model from upstream chain.
    Create,
    /// Exploratory data analysis: returns column profiles.
    Eda,
}

// ─── Parsed table OPTIONS ──────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)] // Phase 2 fields (chaining, tune, forecasting)
pub struct TableOptions {
    pub stage: StageKind,
    /// Source table (for Features stage). E.g. "public.customers"
    pub source_table: Option<String>,
    /// Source view — chain link to previous stage. E.g. "pgaugur.churn_features"
    pub source_view: Option<String>,
    /// Task type: classification, regression, forecasting
    pub task: Option<String>,
    /// Train/test split fraction (default 0.3)
    pub test_fraction: f64,
    /// Random seed (default 42)
    pub seed: u64,
    /// Tune strategy: random, grid, halving
    pub strategy: Option<String>,
    /// Number of tune trials
    pub n_trials: Option<i32>,
    /// Inline tune spec: "random:20" or "grid:10" (strategy:n_trials)
    pub tune: Option<String>,
    /// Semantic flag: if "true", run LLM-powered analysis before EDA/training
    pub semantic: bool,
    /// EDA flag: if "true", run exploratory data analysis before training
    pub eda: bool,
    /// Pipeline search spec: "random:50" or "grid:20" (strategy:n_trials)
    pub search_pipeline: Option<String>,
    /// Deploy flag: if "true", create final model after compare/tune
    pub deploy: bool,
    /// Algorithm for action='create': e.g., "xgboost", "rf"
    pub algorithm: Option<String>,
    /// Output table for materialized preprocessed features: e.g., "public.churn_preprocessed"
    pub output_table: Option<String>,
    /// Global missing indicators: "true" → add _missing columns for all nullable cols
    pub missing_indicators: bool,
    /// Global datetime extraction: "true" → extract features from datetime columns
    pub extract_datetime_global: bool,
    /// Global winsorize: "iqr:1.5" or "zscore:2.0"
    pub winsorize_global: Option<String>,
    /// Interaction features: "true" or "squares"
    pub interactions: Option<String>,
    /// Discretize: "uniform:5" or "quantile:10"
    pub discretize_global: Option<String>,
    /// Mutual information feature selection: top-k features
    pub mutual_info_k: Option<i32>,
    /// Conformal prediction coverage: e.g., 0.95
    pub conformal: Option<f64>,
    /// Time column for forecasting
    pub time_column: Option<String>,
    /// Forecast horizon
    pub horizon: Option<i32>,
}

// ─── Parsed column OPTIONS ──────────────────────

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ColumnOptions {
    /// Imputation strategy: mean, median, mode, drop
    pub impute: Option<String>,
    /// Encoding strategy: onehot, label, ordinal, target, frequency
    pub encode: Option<String>,
    /// Scaling strategy: standard, minmax, robust, maxabs, none
    pub scale: Option<String>,
    /// Feature transform: yeo_johnson, quantile
    pub transform: Option<String>,
    /// Column role: target, ignore, id
    pub role: Option<String>,
    /// Derived feature expression
    pub derive: Option<String>,
    /// Outlier removal: iqr:FACTOR, zscore:SIGMA
    pub outlier: Option<String>,
    /// Winsorize: cap extremes at bounds instead of dropping. iqr:1.5, zscore:2.0
    pub winsorize: Option<String>,
    /// Discretize: bin into intervals. uniform:5, quantile:10
    pub discretize: Option<String>,
    /// Extract datetime features: true → year, month, day, day_of_week, etc.
    pub extract_datetime: Option<String>,
    /// Missing indicator: true → add {col}_missing binary column
    pub missing_indicator: Option<String>,
    /// String split delimiter: e.g., "/", ",", "_"
    pub split: Option<String>,
    /// Comma-separated part names for split: e.g., "deck,num,side"
    pub split_names: Option<String>,
}

// ─── Known valid option values ──────────────────

const VALID_IMPUTE: &[&str] = &["mean", "median", "mode", "drop"];
const VALID_ENCODE: &[&str] = &["onehot", "label", "ordinal", "target", "frequency"];
const VALID_SCALE: &[&str] = &["standard", "minmax", "robust", "maxabs", "none"];
const VALID_TRANSFORM: &[&str] = &["yeo_johnson", "quantile"];
const VALID_ROLE: &[&str] = &["target", "ignore", "id"];
const VALID_ACTION: &[&str] = &["compare", "tune", "create", "eda"];
const VALID_TASK: &[&str] = &["classification", "regression", "forecasting"];
const VALID_STRATEGY: &[&str] = &["random", "grid", "halving"];

const VALID_COLUMN_OPTIONS: &[&str] = &[
    "impute",
    "encode",
    "scale",
    "transform",
    "role",
    "winsorize",
    "discretize",
    "extract_datetime",
    "missing_indicator",
    "split",
    "split_names",
    "derive",
    "outlier",
];
const VALID_TABLE_OPTIONS: &[&str] = &[
    "source_table",
    "source_view",
    "action",
    "task",
    "test_fraction",
    "seed",
    "strategy",
    "n_trials",
    "tune",
    "semantic",
    "eda",
    "search_pipeline",
    "deploy",
    "algorithm",
    "output_table",
    "missing_indicators",
    "extract_datetime",
    "winsorize",
    "interactions",
    "discretize",
    "mutual_info_k",
    "conformal",
    "time_column",
    "horizon",
];

// ─── DefElem list extraction ────────────────────

/// Extract (name, value) pairs from a pg_sys::List of DefElem nodes.
///
/// # Safety
/// The caller must ensure `options` is a valid List pointer.
pub unsafe fn extract_options_from_list(options: *mut pg_sys::List) -> Vec<(String, String)> {
    let mut result = Vec::new();
    if options.is_null() {
        return result;
    }

    let list = &*options;
    for i in 0..list.length {
        let cell = list.elements.add(i as usize);
        let defelem = (*cell).ptr_value as *mut pg_sys::DefElem;
        if defelem.is_null() {
            continue;
        }
        let de = &*defelem;

        let name = if de.defname.is_null() {
            continue;
        } else {
            CStr::from_ptr(de.defname).to_string_lossy().to_string()
        };

        // Extract string value from the DefElem's arg node
        let value = extract_defelem_string_value(de.arg);

        result.push((name, value));
    }
    result
}

/// Extract a string value from a DefElem arg node.
unsafe fn extract_defelem_string_value(arg: *mut pg_sys::Node) -> String {
    if arg.is_null() {
        return String::new();
    }
    let tag = (*arg).type_;
    if tag == pg_sys::NodeTag::T_String {
        let s = arg as *mut pg_sys::String;
        if !(*s).sval.is_null() {
            return CStr::from_ptr((*s).sval).to_string_lossy().to_string();
        }
    }
    if tag == pg_sys::NodeTag::T_Integer {
        let i = arg as *mut pg_sys::Integer;
        return (*i).ival.to_string();
    }
    if tag == pg_sys::NodeTag::T_Float {
        let f = arg as *mut pg_sys::Float;
        if !(*f).fval.is_null() {
            return CStr::from_ptr((*f).fval).to_string_lossy().to_string();
        }
    }
    String::new()
}

// ─── Validation ─────────────────────────────────

fn validate_value(option: &str, value: &str, valid: &[&str]) -> Result<(), AugurPgError> {
    if !valid.contains(&value) {
        return Err(AugurPgError::DslParse(format!(
            "invalid value '{}' for option '{}' (valid: {})",
            value,
            option,
            valid.join(", ")
        )));
    }
    Ok(())
}

/// Validate column-level OPTIONS and return parsed ColumnOptions.
pub fn validate_column_options(
    options: &[(String, String)],
) -> Result<ColumnOptions, AugurPgError> {
    let mut col_opts = ColumnOptions::default();

    for (name, value) in options {
        if !VALID_COLUMN_OPTIONS.contains(&name.as_str()) {
            return Err(AugurPgError::DslParse(format!(
                "unknown column option '{}' (valid: {})",
                name,
                VALID_COLUMN_OPTIONS.join(", ")
            )));
        }

        match name.as_str() {
            "impute" => {
                validate_value("impute", value, VALID_IMPUTE)?;
                col_opts.impute = Some(value.clone());
            }
            "encode" => {
                validate_value("encode", value, VALID_ENCODE)?;
                col_opts.encode = Some(value.clone());
            }
            "scale" => {
                validate_value("scale", value, VALID_SCALE)?;
                col_opts.scale = Some(value.clone());
            }
            "transform" => {
                validate_value("transform", value, VALID_TRANSFORM)?;
                col_opts.transform = Some(value.clone());
            }
            "role" => {
                validate_value("role", value, VALID_ROLE)?;
                col_opts.role = Some(value.clone());
            }
            "derive" => {
                col_opts.derive = Some(value.clone());
            }
            "outlier" => {
                // Format: "iqr:1.5" or "zscore:3.0"
                if !value.starts_with("iqr:") && !value.starts_with("zscore:") {
                    return Err(AugurPgError::DslParse(format!(
                        "invalid outlier spec '{}' (expected 'iqr:FACTOR' or 'zscore:SIGMA')",
                        value
                    )));
                }
                col_opts.outlier = Some(value.clone());
            }
            "winsorize" => {
                if !value.starts_with("iqr:") && !value.starts_with("zscore:") {
                    return Err(AugurPgError::DslParse(format!(
                        "invalid winsorize spec '{}' (expected 'iqr:FACTOR' or 'zscore:SIGMA')",
                        value
                    )));
                }
                col_opts.winsorize = Some(value.clone());
            }
            "discretize" => {
                col_opts.discretize = Some(value.clone());
            }
            "extract_datetime" => {
                col_opts.extract_datetime = Some(value.clone());
            }
            "missing_indicator" => {
                col_opts.missing_indicator = Some(value.clone());
            }
            "split" => {
                col_opts.split = Some(value.clone());
            }
            "split_names" => {
                col_opts.split_names = Some(value.clone());
            }
            _ => {}
        }
    }

    Ok(col_opts)
}

/// Validate table-level OPTIONS and return parsed TableOptions.
pub fn validate_table_options(options: &[(String, String)]) -> Result<TableOptions, AugurPgError> {
    let mut source_table: Option<String> = None;
    let mut source_view: Option<String> = None;
    let mut action: Option<String> = None;
    let mut task: Option<String> = None;
    let mut test_fraction: f64 = 0.3;
    let mut seed: u64 = 42;
    let mut strategy: Option<String> = None;
    let mut n_trials: Option<i32> = None;
    let mut tune: Option<String> = None;
    let mut semantic: bool = false;
    let mut eda: bool = false;
    let mut search_pipeline: Option<String> = None;
    let mut deploy: bool = false;
    let mut algorithm: Option<String> = None;
    let mut output_table: Option<String> = None;
    let mut missing_indicators: bool = false;
    let mut extract_datetime_global: bool = false;
    let mut winsorize_global: Option<String> = None;
    let mut interactions: Option<String> = None;
    let mut discretize_global: Option<String> = None;
    let mut mutual_info_k: Option<i32> = None;
    let mut conformal: Option<f64> = None;
    let mut time_column: Option<String> = None;
    let mut horizon: Option<i32> = None;

    for (name, value) in options {
        if !VALID_TABLE_OPTIONS.contains(&name.as_str()) {
            return Err(AugurPgError::DslParse(format!(
                "unknown table option '{}' (valid: {})",
                name,
                VALID_TABLE_OPTIONS.join(", ")
            )));
        }

        match name.as_str() {
            "source_table" => source_table = Some(value.clone()),
            "source_view" => source_view = Some(value.clone()),
            "action" => {
                validate_value("action", value, VALID_ACTION)?;
                action = Some(value.clone());
            }
            "task" => {
                validate_value("task", value, VALID_TASK)?;
                task = Some(value.clone());
            }
            "test_fraction" => {
                test_fraction = value.parse::<f64>().map_err(|_| {
                    AugurPgError::DslParse(format!("test_fraction must be a number: '{}'", value))
                })?;
            }
            "seed" => {
                seed = value.parse::<u64>().map_err(|_| {
                    AugurPgError::DslParse(format!("seed must be a positive integer: '{}'", value))
                })?;
            }
            "strategy" => {
                validate_value("strategy", value, VALID_STRATEGY)?;
                strategy = Some(value.clone());
            }
            "n_trials" => {
                n_trials = Some(value.parse::<i32>().map_err(|_| {
                    AugurPgError::DslParse(format!("n_trials must be an integer: '{}'", value))
                })?);
            }
            "tune" => tune = Some(value.clone()),
            "semantic" => semantic = value == "true" || value == "t",
            "eda" => eda = value == "true" || value == "t",
            "search_pipeline" => search_pipeline = Some(value.clone()),
            "deploy" => deploy = value == "true" || value == "t",
            "algorithm" => algorithm = Some(value.clone()),
            "output_table" => output_table = Some(value.clone()),
            "missing_indicators" => missing_indicators = value == "true" || value == "t",
            "extract_datetime" => extract_datetime_global = value == "true" || value == "t",
            "winsorize" => winsorize_global = Some(value.clone()),
            "interactions" => interactions = Some(value.clone()),
            "discretize" => discretize_global = Some(value.clone()),
            "mutual_info_k" => {
                mutual_info_k = Some(value.parse::<i32>().map_err(|_| {
                    AugurPgError::DslParse(format!("mutual_info_k must be an integer: '{}'", value))
                })?);
            }
            "conformal" => {
                conformal = Some(value.parse::<f64>().map_err(|_| {
                    AugurPgError::DslParse(format!(
                        "conformal must be a number (e.g., 0.95): '{}'",
                        value
                    ))
                })?);
            }
            "time_column" => time_column = Some(value.clone()),
            "horizon" => {
                horizon = Some(value.parse::<i32>().map_err(|_| {
                    AugurPgError::DslParse(format!("horizon must be an integer: '{}'", value))
                })?);
            }
            _ => {}
        }
    }

    // Determine stage kind
    let action_kind = action.as_deref().map(|act| match act {
        "compare" => ActionKind::Compare,
        "tune" => ActionKind::Tune,
        "create" => ActionKind::Create,
        "eda" => ActionKind::Eda,
        _ => unreachable!("validated above"),
    });

    let stage = match (source_table.is_some(), source_view.is_some(), action_kind) {
        // source_table only → pure feature view
        (true, false, None) => StageKind::Features,
        // source_table + action → combined features + action (single-call shorthand)
        (true, false, Some(ak)) => StageKind::FeaturesWithAction(ak),
        // source_view + action → chained action stage
        (false, true, Some(ak)) => StageKind::Action(ak),
        // source_view without action → error
        (false, true, None) => {
            return Err(AugurPgError::DslParse(
                "source_view requires an action option (compare, tune, create, eda)".into(),
            ));
        }
        // both source_table and source_view → error
        (true, true, _) => {
            return Err(AugurPgError::DslParse(
                "cannot set both source_table and source_view".into(),
            ));
        }
        // neither → error
        (false, false, _) => {
            return Err(AugurPgError::DslParse(
                "foreign table must have source_table (for feature stage) or source_view + action (for chained stage)".into(),
            ));
        }
    };

    Ok(TableOptions {
        stage,
        source_table,
        source_view,
        task,
        test_fraction,
        seed,
        strategy,
        n_trials,
        tune,
        semantic,
        eda,
        search_pipeline,
        deploy,
        algorithm,
        output_table,
        missing_indicators,
        extract_datetime_global,
        winsorize_global,
        interactions,
        discretize_global,
        mutual_info_k,
        conformal,
        time_column,
        horizon,
    })
}

/// Read table-level OPTIONS from a foreign table OID at runtime.
pub fn read_table_options(relid: pg_sys::Oid) -> Result<TableOptions, AugurPgError> {
    let ft = unsafe { pg_sys::GetForeignTable(relid) };
    if ft.is_null() {
        return Err(AugurPgError::Other("not a foreign table".into()));
    }
    let options = unsafe { extract_options_from_list((*ft).options) };
    validate_table_options(&options)
}

/// Read column-level OPTIONS for a specific column of a foreign table at runtime.
pub fn read_column_options(relid: pg_sys::Oid, attnum: i16) -> Result<ColumnOptions, AugurPgError> {
    let options_list = unsafe { pg_sys::GetForeignColumnOptions(relid, attnum) };
    let options = unsafe { extract_options_from_list(options_list) };
    validate_column_options(&options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_valid_column_options() {
        let opts = vec![
            ("impute".into(), "mean".into()),
            ("scale".into(), "standard".into()),
        ];
        let result = validate_column_options(&opts).unwrap();
        assert_eq!(result.impute.as_deref(), Some("mean"));
        assert_eq!(result.scale.as_deref(), Some("standard"));
    }

    #[test]
    fn validate_invalid_column_option_name() {
        let opts = vec![("bogus".into(), "value".into())];
        assert!(validate_column_options(&opts).is_err());
    }

    #[test]
    fn validate_invalid_impute_value() {
        let opts = vec![("impute".into(), "bogus".into())];
        assert!(validate_column_options(&opts).is_err());
    }

    #[test]
    fn validate_valid_table_options_features() {
        let opts = vec![
            ("source_table".into(), "public.iris".into()),
            ("task".into(), "classification".into()),
        ];
        let result = validate_table_options(&opts).unwrap();
        assert_eq!(result.stage, StageKind::Features);
        assert_eq!(result.source_table.as_deref(), Some("public.iris"));
    }

    #[test]
    fn validate_valid_table_options_compare_chained() {
        let opts = vec![
            ("source_view".into(), "pgaugur.my_features".into()),
            ("action".into(), "compare".into()),
        ];
        let result = validate_table_options(&opts).unwrap();
        assert_eq!(result.stage, StageKind::Action(ActionKind::Compare));
    }

    #[test]
    fn validate_features_with_action_shorthand() {
        // Single-call: source_table + action = combined stage
        let opts = vec![
            ("source_table".into(), "public.iris".into()),
            ("task".into(), "classification".into()),
            ("action".into(), "compare".into()),
        ];
        let result = validate_table_options(&opts).unwrap();
        assert_eq!(
            result.stage,
            StageKind::FeaturesWithAction(ActionKind::Compare)
        );
        assert_eq!(result.source_table.as_deref(), Some("public.iris"));
    }

    #[test]
    fn validate_table_no_source() {
        let opts = vec![("task".into(), "classification".into())];
        assert!(validate_table_options(&opts).is_err());
    }

    #[test]
    fn validate_table_action_without_source_view() {
        // action alone (no source_table or source_view) → error
        let opts = vec![("action".into(), "compare".into())];
        assert!(validate_table_options(&opts).is_err());
    }

    #[test]
    fn validate_source_view_without_action() {
        let opts = vec![("source_view".into(), "pgaugur.my_features".into())];
        assert!(validate_table_options(&opts).is_err());
    }

    #[test]
    fn validate_both_sources_rejected() {
        let opts = vec![
            ("source_table".into(), "public.iris".into()),
            ("source_view".into(), "pgaugur.my_features".into()),
            ("action".into(), "compare".into()),
        ];
        assert!(validate_table_options(&opts).is_err());
    }

    #[test]
    fn validate_outlier_format() {
        let opts = vec![("outlier".into(), "iqr:1.5".into())];
        assert!(validate_column_options(&opts).is_ok());

        let opts = vec![("outlier".into(), "zscore:3.0".into())];
        assert!(validate_column_options(&opts).is_ok());

        let opts = vec![("outlier".into(), "bad".into())];
        assert!(validate_column_options(&opts).is_err());
    }

    #[test]
    fn validate_role_target() {
        let opts = vec![("role".into(), "target".into())];
        let result = validate_column_options(&opts).unwrap();
        assert_eq!(result.role.as_deref(), Some("target"));
    }

    #[test]
    fn validate_tune_options() {
        let opts = vec![
            ("source_view".into(), "pgaugur.my_compare".into()),
            ("action".into(), "tune".into()),
            ("strategy".into(), "random".into()),
            ("n_trials".into(), "20".into()),
        ];
        let result = validate_table_options(&opts).unwrap();
        assert_eq!(result.stage, StageKind::Action(ActionKind::Tune));
        assert_eq!(result.strategy.as_deref(), Some("random"));
        assert_eq!(result.n_trials, Some(20));
    }
}
