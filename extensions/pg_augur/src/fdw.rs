//! Foreign Data Wrapper for pgaugur feature views.
//!
//! Implements the FDW handler, validator, and scan callbacks that turn
//! `CREATE FOREIGN TABLE` into ML pipeline stages.

use crate::data;
use crate::error::AugurPgError;
use crate::fdw_options::{
    read_column_options, read_table_options, ActionKind, StageKind, TableOptions,
};
// Used by the validator (currently a no-op stub, but imported for when validation is re-enabled)
#[allow(unused_imports)]
use crate::fdw_options::{
    extract_options_from_list, validate_column_options, validate_table_options,
};
use crate::models;
use crate::predict;
use crate::task::{infer_task, to_augur_task, PgTask};
use crate::train;

use augur::prelude::{compare_models, create_model, save_model_to_string, setup, SetupConfig};
use augur_core::types::{ColumnConfig, EncoderKind, ImputeStrategy, ScalerKind};
use pgrx::pg_guard;
use pgrx::pg_sys;
use polars::prelude::*;
use std::ffi::CStr;
use std::os::raw::c_int;

// ─── FDW Handler ──────────────────────────────

/// FDW handler: returns an FdwRoutine with our scan callbacks.
///
/// Exported as a raw C function with manual PG_FUNCTION_INFO_V1
/// registration because pgrx's Internal type doesn't produce the
/// correct Datum format for fdw_handler return values.
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn fdw_handler_wrapper(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    let routine =
        pg_sys::palloc0(std::mem::size_of::<pg_sys::FdwRoutine>()) as *mut pg_sys::FdwRoutine;
    (*routine).type_ = pg_sys::NodeTag::T_FdwRoutine;
    (*routine).GetForeignRelSize = Some(fdw_get_rel_size);
    (*routine).GetForeignPaths = Some(fdw_get_paths);
    (*routine).GetForeignPlan = Some(fdw_get_plan);
    (*routine).BeginForeignScan = Some(fdw_begin_scan);
    (*routine).IterateForeignScan = Some(fdw_iterate_scan);
    (*routine).EndForeignScan = Some(fdw_end_scan);
    pg_sys::Datum::from(routine as usize)
}

/// PG_FUNCTION_INFO_V1 registration for fdw_handler_wrapper.
#[no_mangle]
pub extern "C" fn pg_finfo_fdw_handler_wrapper() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

// ─── FDW Validator ────────────────────────────

/// Validates OPTIONS at CREATE FOREIGN TABLE time.
///
/// Accepts all options — validation is deferred to BeginForeignScan
/// where we read options via GetForeignTable/GetForeignColumnOptions
/// (the safe runtime API).
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn fdw_validator_wrapper(_fcinfo: pg_sys::FunctionCallInfo) {
    // Validation deferred to scan time — the GetForeignColumnOptions API
    // is safer than parsing fcinfo args directly for FDW validators.
}

/// PG_FUNCTION_INFO_V1 registration for fdw_validator_wrapper.
#[no_mangle]
pub extern "C" fn pg_finfo_fdw_validator_wrapper() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

// ─── Scan State ───────────────────────────────

/// Per-scan state stored in fdw_state during query execution.
struct ScanState {
    /// Rows to return, as Vec of Vec<Datum-like values>
    rows: Vec<Vec<Option<String>>>,
    /// Column names for the foreign table (used for Phase 2 dynamic columns)
    #[allow(dead_code)]
    col_names: Vec<String>,
    /// Current row index
    current: usize,
}

// ─── Build SetupConfig from Column OPTIONS ────

fn parse_impute(s: &str) -> ImputeStrategy {
    match s {
        "mean" => ImputeStrategy::Mean,
        "median" => ImputeStrategy::Median,
        "mode" => ImputeStrategy::Mode,
        "drop" => ImputeStrategy::Drop,
        _ => ImputeStrategy::Mean,
    }
}

fn parse_encoder(s: &str) -> EncoderKind {
    match s {
        "onehot" => EncoderKind::OneHot,
        "label" => EncoderKind::Label,
        "ordinal" => EncoderKind::Ordinal(vec![]),
        "target" => EncoderKind::Target,
        "frequency" => EncoderKind::Frequency,
        _ => EncoderKind::OneHot,
    }
}

fn parse_scaler(s: &str) -> ScalerKind {
    match s {
        "standard" => ScalerKind::Standard,
        "minmax" => ScalerKind::MinMax,
        "robust" => ScalerKind::Robust,
        "maxabs" => ScalerKind::MaxAbs,
        "none" => ScalerKind::None,
        _ => ScalerKind::Standard,
    }
}

/// Get the declared column names from a foreign table definition.
fn get_foreign_table_columns(relid: pg_sys::Oid) -> Result<Vec<String>, AugurPgError> {
    let rel = unsafe { pg_sys::RelationIdGetRelation(relid) };
    if rel.is_null() {
        return Err(AugurPgError::Other(
            "cannot open foreign table relation".into(),
        ));
    }
    let tupdesc = unsafe { (*rel).rd_att };
    let natts = unsafe { (*tupdesc).natts } as i16;
    let mut columns = Vec::new();
    for attnum in 1..=natts {
        let col_name = unsafe {
            let name_ptr = pg_sys::get_attname(relid, attnum, false);
            CStr::from_ptr(name_ptr).to_string_lossy().to_string()
        };
        columns.push(col_name);
    }
    unsafe { pg_sys::RelationClose(rel) };
    Ok(columns)
}

/// Build a SetupConfig from the foreign table's column OPTIONS.
/// Used by execute_stage for SELECT queries on feature views.
#[allow(dead_code)]
fn build_setup_config(
    relid: pg_sys::Oid,
    table_opts: &TableOptions,
    df: &DataFrame,
) -> Result<(SetupConfig, PgTask, Vec<String>), AugurPgError> {
    // Find the target column and collect feature columns
    let mut target_column: Option<String> = None;
    let mut ignore_columns: Vec<String> = Vec::new();

    // Get number of columns from the relation
    let rel = unsafe { pg_sys::RelationIdGetRelation(relid) };
    if rel.is_null() {
        return Err(AugurPgError::Other(
            "cannot open foreign table relation".into(),
        ));
    }
    let tupdesc = unsafe { (*rel).rd_att };
    let natts = unsafe { (*tupdesc).natts } as i16;

    let mut column_configs: Vec<(String, ColumnConfig)> = Vec::new();

    for attnum in 1..=natts {
        let col_name = unsafe {
            let name_ptr = pg_sys::get_attname(relid, attnum, false);
            CStr::from_ptr(name_ptr).to_string_lossy().to_string()
        };

        let col_opts = read_column_options(relid, attnum).unwrap_or_default();

        if col_opts.role.as_deref() == Some("target") {
            target_column = Some(col_name.clone());
            continue;
        }
        if col_opts.role.as_deref() == Some("ignore") || col_opts.role.as_deref() == Some("id") {
            ignore_columns.push(col_name.clone());
            continue;
        }

        let mut cc = ColumnConfig::default();
        if let Some(ref imp) = col_opts.impute {
            cc.impute = Some(parse_impute(imp));
        }
        if let Some(ref enc) = col_opts.encode {
            cc.encode = Some(parse_encoder(enc));
        }
        if let Some(ref sc) = col_opts.scale {
            cc.scale = Some(parse_scaler(sc));
        }

        if cc.impute.is_some() || cc.encode.is_some() || cc.scale.is_some() {
            column_configs.push((col_name, cc));
        }
    }

    unsafe { pg_sys::RelationClose(rel) };

    let target = target_column.ok_or_else(|| {
        AugurPgError::DslParse("no column with OPTIONS (role 'target') found".into())
    })?;

    // Infer or use explicit task
    let pg_task = if let Some(ref task_str) = table_opts.task {
        PgTask::parse(task_str)?
    } else {
        infer_task(df, &target)?
    };

    let n_classes = if pg_task == PgTask::Classification {
        let col = df.column(&target).ok();
        col.map(|c| c.as_materialized_series().n_unique().unwrap_or(2))
    } else {
        None
    };
    let task_type = to_augur_task(pg_task, n_classes);

    let mut config = SetupConfig::new(&target)
        .task_type(task_type)
        .ignore(ignore_columns.clone())
        .test_fraction(table_opts.test_fraction)
        .seed(table_opts.seed);

    for (col_name, cc) in column_configs {
        config = config.column_config(col_name, cc);
    }

    let feature_columns: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .filter(|c| c != &target && !ignore_columns.contains(c))
        .collect();

    Ok((config, pg_task, feature_columns))
}

// ─── Chain Resolution ────────────────────────────

/// A resolved pipeline chain from FDW definitions.
#[derive(Debug, Clone)]
pub struct TrainChain {
    /// Project name for storing the trained model (= endpoint foreign table name)
    pub project_name: String,
    pub source_schema: Option<String>,
    pub source_table: String,
    pub target_column: String,
    pub task: String,
    #[allow(dead_code)] // Used for future async worker OID lookup
    pub feature_view_relid: pg_sys::Oid,
    pub feature_columns: Vec<String>,
    pub test_fraction: f64,
    pub seed: u64,
    pub actions: Vec<ChainAction>,
    /// Output table for materialized preprocessed features
    pub output_table: Option<String>,
    // ── Global preprocessing options from table OPTIONS ──
    pub missing_indicators: bool,
    pub extract_datetime_global: bool,
    pub winsorize_global: Option<String>,
    pub interactions: Option<String>,
    pub discretize_global: Option<String>,
    pub mutual_info_k: Option<i32>,
    pub conformal: Option<f64>,
    /// Serialized column OPTIONS for the background worker
    pub column_options: Vec<(String, crate::fdw_options::ColumnOptions)>,
}

/// An action in the training pipeline.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChainAction {
    Semantic,
    Eda,
    SearchPipeline { strategy: String, n_trials: i32 },
    Compare,
    Tune { strategy: String, n_trials: i32 },
    Create { algorithm: Option<String> },
}

/// Look up a foreign table OID by qualified name (e.g., "pgaugur.churn_features").
pub fn lookup_foreign_table_oid(qualified_name: &str) -> Result<pg_sys::Oid, AugurPgError> {
    let sql = format!(
        "SELECT c.oid::bigint FROM pg_class c
         JOIN pg_namespace n ON c.relnamespace = n.oid
         WHERE n.nspname || '.' || c.relname = {}
           AND c.relkind = 'f'",
        crate::models::quote_literal(qualified_name)
    );
    pgrx::Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("lookup foreign table: {e}")))?
        .map(|oid| pg_sys::Oid::from(oid as u32))
        .ok_or_else(|| AugurPgError::Other(format!("foreign table '{}' not found", qualified_name)))
}

/// Get foreign table name from OID.
pub fn get_foreign_table_name_pub(relid: pg_sys::Oid) -> Result<String, AugurPgError> {
    get_foreign_table_name(relid)
}

fn get_foreign_table_name(relid: pg_sys::Oid) -> Result<String, AugurPgError> {
    let rel = unsafe { pg_sys::RelationIdGetRelation(relid) };
    if rel.is_null() {
        return Err(AugurPgError::Other("cannot open relation".into()));
    }
    let name = unsafe {
        let rd_rel = (*rel).rd_rel;
        CStr::from_ptr((*rd_rel).relname.data.as_ptr())
            .to_string_lossy()
            .to_string()
    };
    unsafe { pg_sys::RelationClose(rel) };
    Ok(name)
}

/// Collect column OPTIONS for all columns in a foreign table.
fn collect_column_options(
    relid: pg_sys::Oid,
) -> Result<Vec<(String, crate::fdw_options::ColumnOptions)>, AugurPgError> {
    let rel = unsafe { pg_sys::RelationIdGetRelation(relid) };
    if rel.is_null() {
        return Err(AugurPgError::Other("cannot open relation".into()));
    }
    let tupdesc = unsafe { (*rel).rd_att };
    let natts = unsafe { (*tupdesc).natts } as i16;
    let mut result = Vec::new();
    for attnum in 1..=natts {
        let col_name = unsafe {
            let name_ptr = pg_sys::get_attname(relid, attnum, false);
            CStr::from_ptr(name_ptr).to_string_lossy().to_string()
        };
        let col_opts = read_column_options(relid, attnum).unwrap_or_default();
        result.push((col_name, col_opts));
    }
    unsafe { pg_sys::RelationClose(rel) };
    Ok(result)
}

/// Resolve the chain of FDW definitions back to the source table.
///
/// Walks `source_view` links recursively until it finds a Features stage
/// with `source_table`. Collects actions from each stage in order.
pub fn resolve_chain(relid: pg_sys::Oid) -> Result<TrainChain, AugurPgError> {
    let table_opts = read_table_options(relid)?;

    match &table_opts.stage {
        StageKind::Features | StageKind::FeaturesWithAction(_) => {
            // Terminal: this is the features stage
            let source_ref = table_opts.source_table.as_ref().ok_or_else(|| {
                AugurPgError::DslParse("features stage has no source_table".into())
            })?;
            let (source_schema, source_table) = data::parse_relation(source_ref)?;

            let ftable_columns = get_foreign_table_columns(relid)?;
            let column_options = collect_column_options(relid)?;

            // Find target column
            let target_column = column_options
                .iter()
                .find(|(_, opts)| opts.role.as_deref() == Some("target"))
                .map(|(name, _)| name.clone())
                .ok_or_else(|| AugurPgError::DslParse("no column with role 'target'".into()))?;

            let task = table_opts
                .task
                .clone()
                .unwrap_or_else(|| "classification".to_string());

            // Build actions from inline pipeline options
            let mut actions = Vec::new();

            if table_opts.semantic {
                actions.push(ChainAction::Semantic);
            }
            if table_opts.eda {
                actions.push(ChainAction::Eda);
            }

            // Pipeline search: try different preprocessing configs, pick best
            if let Some(ref sp) = table_opts.search_pipeline {
                let parts: Vec<&str> = sp.split(':').collect();
                let strategy = parts.first().unwrap_or(&"random").to_string();
                let n = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(20);
                actions.push(ChainAction::SearchPipeline {
                    strategy,
                    n_trials: n,
                });
            }

            // Action from stage kind or table options
            if let StageKind::FeaturesWithAction(action) = &table_opts.stage {
                match action {
                    ActionKind::Compare => actions.push(ChainAction::Compare),
                    ActionKind::Create => actions.push(ChainAction::Create {
                        algorithm: table_opts.algorithm.clone(),
                    }),
                    ActionKind::Tune => {
                        let strategy = table_opts
                            .strategy
                            .clone()
                            .unwrap_or_else(|| "random".into());
                        let n = table_opts.n_trials.unwrap_or(10);
                        actions.push(ChainAction::Tune {
                            strategy,
                            n_trials: n,
                        });
                    }
                    ActionKind::Eda => actions.push(ChainAction::Eda),
                }
            }

            // Inline tune option (e.g., tune='random:20')
            if let Some(ref tune_spec) = table_opts.tune {
                let parts: Vec<&str> = tune_spec.split(':').collect();
                let strategy = parts.first().unwrap_or(&"random").to_string();
                let n = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(10);
                actions.push(ChainAction::Tune {
                    strategy,
                    n_trials: n,
                });
            }

            // Deploy flag → create final model
            if table_opts.deploy {
                actions.push(ChainAction::Create { algorithm: None });
            }

            // Feature columns (excluding target and ignored)
            let feature_cols: Vec<String> = ftable_columns
                .iter()
                .filter(|c| {
                    let opts = column_options.iter().find(|(n, _)| n == *c);
                    match opts {
                        Some((_, o)) => {
                            o.role.as_deref() != Some("target")
                                && o.role.as_deref() != Some("ignore")
                                && o.role.as_deref() != Some("id")
                        }
                        None => true,
                    }
                })
                .cloned()
                .collect();

            let project_name =
                get_foreign_table_name(relid).unwrap_or_else(|_| "fdw_model".to_string());

            Ok(TrainChain {
                project_name,
                source_schema,
                source_table,
                target_column,
                task,
                feature_view_relid: relid,
                feature_columns: feature_cols,
                test_fraction: table_opts.test_fraction,
                seed: table_opts.seed,
                actions,
                output_table: table_opts.output_table.clone(),
                missing_indicators: table_opts.missing_indicators,
                extract_datetime_global: table_opts.extract_datetime_global,
                winsorize_global: table_opts.winsorize_global.clone(),
                interactions: table_opts.interactions.clone(),
                discretize_global: table_opts.discretize_global.clone(),
                mutual_info_k: table_opts.mutual_info_k,
                conformal: table_opts.conformal,
                column_options,
            })
        }
        StageKind::Action(action) => {
            // Chained stage: resolve upstream first
            let upstream_name = table_opts
                .source_view
                .as_ref()
                .ok_or_else(|| AugurPgError::DslParse("action stage has no source_view".into()))?;
            let upstream_relid = lookup_foreign_table_oid(upstream_name)?;
            let mut chain = resolve_chain(upstream_relid)?;

            // Append this stage's action
            match action {
                ActionKind::Compare => chain.actions.push(ChainAction::Compare),
                ActionKind::Create => chain.actions.push(ChainAction::Create {
                    algorithm: table_opts.algorithm.clone(),
                }),
                ActionKind::Tune => {
                    let strategy = table_opts
                        .strategy
                        .clone()
                        .unwrap_or_else(|| "random".into());
                    let n = table_opts.n_trials.unwrap_or(10);
                    chain.actions.push(ChainAction::Tune {
                        strategy,
                        n_trials: n,
                    });
                }
                ActionKind::Eda => chain.actions.push(ChainAction::Eda),
            }

            Ok(chain)
        }
    }
}

// ─── Execute stage (read-only for SELECT) ────────

/// Load source data and return rows. Training is NOT triggered here —
/// use `train()` to trigger training.
#[allow(clippy::type_complexity)]
fn execute_stage(
    relid: pg_sys::Oid,
    table_opts: &TableOptions,
) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), AugurPgError> {
    // Resolve the source table (walk chain if needed)
    let source_ref = match (&table_opts.source_table, &table_opts.source_view) {
        (Some(st), _) => st.clone(),
        (None, Some(sv)) => {
            // Chained: resolve the upstream features stage to get source_table
            let upstream_relid = lookup_foreign_table_oid(sv)?;
            let chain = resolve_chain(upstream_relid)?;
            match chain.source_schema {
                Some(ref s) => format!("{}.{}", s, chain.source_table),
                None => chain.source_table,
            }
        }
        _ => {
            return Err(AugurPgError::DslParse(
                "no source_table or source_view".into(),
            ))
        }
    };

    let (source_schema, source_table) = data::parse_relation(&source_ref)?;
    let ftable_columns = get_foreign_table_columns(relid)?;
    let df = data::load_table_columns(source_schema.as_deref(), &source_table, &ftable_columns)?;

    let col_names: Vec<String> = df
        .get_column_names()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let rows = dataframe_to_string_rows(&df);
    Ok((col_names, rows))
}

// ─── Training (called by train() function) ───────

/// Execute a training chain synchronously. Called by the background worker
/// or directly by `train()` for sync execution.
pub fn execute_train_chain(chain: &TrainChain) -> Result<(), AugurPgError> {
    let ftable_columns: Vec<String> = chain
        .column_options
        .iter()
        .map(|(n, _)| n.clone())
        .collect();
    let df = data::load_table_columns(
        chain.source_schema.as_deref(),
        &chain.source_table,
        &ftable_columns,
    )?;

    // Build SetupConfig from column OPTIONS
    let (mut config, pg_task) = build_setup_config_from_options(
        &chain.column_options,
        &chain.target_column,
        chain.task.as_str(),
        chain.test_fraction,
        chain.seed,
        &df,
    )?;

    // Apply global table-level preprocessing options
    if chain.missing_indicators {
        config.create_missing_indicators = true;
    }
    if chain.extract_datetime_global {
        config.extract_datetime = true;
    }
    if let Some(ref w) = chain.winsorize_global {
        let parts: Vec<&str> = w.split(':').collect();
        let method = parts.first().unwrap_or(&"iqr");
        let factor: f64 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1.5);
        config.winsorize = Some(augur_core::types::OutlierRemovalConfig {
            method: if *method == "zscore" {
                augur_core::types::OutlierMethod::Zscore
            } else {
                augur_core::types::OutlierMethod::Iqr
            },
            threshold: factor,
            columns: vec![],
        });
    }
    if let Some(ref inter) = chain.interactions {
        let include_squares = inter == "squares";
        config.interactions = Some(augur_core::types::InteractionConfig {
            columns: vec![],
            include_squares,
        });
    }
    if let Some(ref disc) = chain.discretize_global {
        let parts: Vec<&str> = disc.split(':').collect();
        let strategy = parts.first().unwrap_or(&"uniform").to_string();
        let n_bins: usize = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(5);
        config.discretize = Some(augur_core::types::DiscretizeConfig {
            n_bins,
            strategy,
            columns: vec![],
        });
    }
    if let Some(k) = chain.mutual_info_k {
        let fs = config
            .feature_selection
            .get_or_insert_with(Default::default);
        fs.mutual_info_k = Some(k as usize);
    }
    if let Some(coverage) = chain.conformal {
        config.conformal_coverage = Some(coverage);
    }

    let project_name = &chain.project_name;

    // Semantic analysis runs BEFORE EDA — LLM-powered column recommendations.
    // Priority: user OPTIONS > semantic > EDA > augur defaults.
    // Requires: augur compiled with `semantic` feature + AUGUR_LLM_API_KEY env var.
    let has_semantic = chain
        .actions
        .iter()
        .any(|a| matches!(a, ChainAction::Semantic));
    if has_semantic {
        // Semantic analysis requires the augur `semantic` feature (augur-semantic crate)
        // which adds ~50MB compile dependency. When enabled, it calls the Anthropic API
        // to analyze column names/samples and recommend preprocessing.
        //
        // To enable: add `features = ["semantic"]` to the augur dependency in Cargo.toml
        // and set AUGUR_LLM_API_KEY environment variable.
        //
        // For now, semantic is a recognized step that logs a notice when not compiled in.
        pgrx::notice!(
            "pgaugur: semantic analysis requested but not compiled in. \
             Add features=[\"semantic\"] to augur dependency and set AUGUR_LLM_API_KEY."
        );
    }

    // If EDA is in the chain, run it BEFORE setup to refine the config.
    // EDA recommendations only fill unset fields — user OPTIONS always win.
    let has_eda = chain.actions.iter().any(|a| matches!(a, ChainAction::Eda));
    if has_eda {
        let task_type = config.task_type;
        let ignore = config.ignore_columns.clone();
        let eda_report = augur::prelude::run_eda(
            &df,
            &chain.target_column,
            task_type,
            &ignore,
            config.categorical_threshold,
        )?;

        // Apply global recommendations (rare categories, cardinality routing, etc.)
        augur::eda::apply_recommendations(&mut config, &eda_report);

        // Apply per-column recommendations for columns without explicit OPTIONS.
        // User-specified column OPTIONS (impute, encode, scale) take priority.
        for col_profile in &eda_report.columns {
            let user_opts = chain
                .column_options
                .iter()
                .find(|(n, _)| n == &col_profile.name);
            let has_user_impute = user_opts.map(|(_, o)| o.impute.is_some()).unwrap_or(false);
            let has_user_encode = user_opts.map(|(_, o)| o.encode.is_some()).unwrap_or(false);
            let has_user_scale = user_opts.map(|(_, o)| o.scale.is_some()).unwrap_or(false);

            let existing = config.column_configs.get(&col_profile.name);
            let mut cc = existing.cloned().unwrap_or_default();
            let rec = &col_profile.recommendation;

            if !has_user_impute && cc.impute.is_none() {
                cc.impute = rec.impute.clone();
            }
            if !has_user_encode && cc.encode.is_none() {
                cc.encode = rec.encode.clone();
            }
            if !has_user_scale && cc.scale.is_none() {
                cc.scale = rec.scale.clone();
            }

            if cc.impute.is_some() || cc.encode.is_some() || cc.scale.is_some() {
                config = config.column_config(col_profile.name.clone(), cc);
            }
        }

        // Store profiles in config for downstream pipeline state
        let dataset_profile = augur::eda::eda_report_to_dataset_profile(&eda_report);
        config.profiles = dataset_profile.columns;

        // Store EDA profiles in typed table
        let _ = models::clear_eda_profiles(project_name);
        for col in &eda_report.columns {
            let rec = &col.recommendation;
            let impute_str = rec.impute.as_ref().map(|i| format!("{:?}", i));
            let encode_str = rec.encode.as_ref().map(|e| format!("{:?}", e));
            let scale_str = rec.scale.as_ref().map(|s| format!("{:?}", s));
            let _ = models::insert_eda_profile(
                project_name,
                &col.name,
                &col.dtype,
                col.null_count as i32,
                col.null_fraction,
                col.n_unique as i32,
                impute_str.as_deref(),
                encode_str.as_deref(),
                scale_str.as_deref(),
                &rec.reasons,
            );
        }
    }

    // Pipeline search: try different preprocessing configs, pick the best.
    // Runs AFTER EDA (EDA provides profiles/recommendations as starting point)
    // but BEFORE setup (search produces a new optimized config).
    let has_search = chain.actions.iter().find_map(|a| match a {
        ChainAction::SearchPipeline { strategy, n_trials } => Some((strategy.clone(), *n_trials)),
        _ => None,
    });
    if let Some((strategy, n_trials)) = has_search {
        let search_strategy = match strategy.as_str() {
            "grid" => augur_core::tuning::SearchStrategy::Grid {
                n_points: n_trials as usize,
            },
            "halving" => augur_core::tuning::SearchStrategy::Halving {
                n_candidates: n_trials as usize,
                seed: chain.seed,
                eta: 3,
            },
            _ => augur_core::tuning::SearchStrategy::Random {
                n_iter: n_trials as usize,
                seed: chain.seed,
            },
        };
        let search_config = augur::prelude::PipelineSearchConfig {
            space: vec![], // empty = auto-detect from data
            strategy: search_strategy,
            proxy_models: vec![],
            eval_folds: Some(3),
            optimize: None,
            seed: chain.seed,
            per_column: false,
            cardinality_threshold: None,
        };
        let search_result =
            augur::prelude::search_pipeline(df.clone(), config.clone(), search_config)?;
        // Replace config with the best-found pipeline configuration
        config = search_result.best_config;
        pgrx::notice!(
            "pgaugur: pipeline search found best config ({} trials, {:.3}s)",
            search_result.trials.len(),
            search_result.total_time_secs
        );

        // Store search result
        let search_json = serde_json::json!({
            "n_trials": search_result.trials.len(),
            "total_time_secs": search_result.total_time_secs,
            "trials": search_result.trials.iter().take(10).map(|t| serde_json::json!({
                "mean_rank": t.mean_rank,
                "n_features": t.n_features,
                "time_secs": t.time_secs,
            })).collect::<Vec<_>>(),
        });
        let _ = models::insert_stage_result(project_name, "search_pipeline", &search_json);
    }

    // Run the experiment setup (with EDA/search-refined config if applicable)
    let experiment = setup(df.clone(), config)?;

    // Execute each action in the chain (skip pre-setup steps — already processed above)
    for action in &chain.actions {
        match action {
            ChainAction::Semantic | ChainAction::Eda | ChainAction::SearchPipeline { .. } => {} // Already processed before setup
            ChainAction::Compare => {
                let compare_result = compare_models(&experiment, None)?;

                // Store per-model metrics in typed compare_results table
                let _ = models::clear_compare_results(project_name);
                let best_id = compare_result.best().map(|b| b.id.clone());
                for (rank, m) in compare_result.models.iter().enumerate() {
                    let metrics = train::compute_metrics_from_experiment(m, &experiment);
                    let is_best = best_id.as_deref() == Some(&m.id);
                    let _ = models::insert_compare_result(
                        project_name,
                        &m.id,
                        &m.display_name,
                        &metrics,
                        (rank + 1) as i32,
                        is_best,
                    );
                }

                if let Some(best) = compare_result.best() {
                    // Generate and store plots for the best model
                    generate_and_store_plots(project_name, best, &experiment);

                    store_model(
                        best,
                        &experiment,
                        project_name,
                        pg_task,
                        &chain.target_column,
                        &chain.feature_columns,
                        chain.source_schema.as_deref(),
                        &chain.source_table,
                    )?;
                }
            }
            ChainAction::Create { algorithm } => {
                let algo = algorithm.as_deref().unwrap_or("rf");
                let model = create_model(&experiment, algo)?;

                // Store create result
                let create_json = serde_json::json!({
                    "algorithm": model.id,
                    "display_name": model.display_name,
                });
                let _ = models::insert_stage_result(project_name, "create", &create_json);

                // Generate and store plots
                generate_and_store_plots(project_name, &model, &experiment);

                store_model(
                    &model,
                    &experiment,
                    project_name,
                    pg_task,
                    &chain.target_column,
                    &chain.feature_columns,
                    chain.source_schema.as_deref(),
                    &chain.source_table,
                )?;
            }
            ChainAction::Tune { .. } => {
                // Tune requires augur::tune_model — Phase 2
                pgrx::notice!("pgaugur: tune step (not yet implemented, skipping)");
            }
        }
    }

    // Materialize preprocessed features to output_table if specified
    if let Some(ref output_table) = chain.output_table {
        materialize_features(&experiment, output_table)?;
    }

    // Store train/test split lineage in typed table
    let _ = models::insert_lineage(
        project_name,
        experiment.train_features.height() as i32,
        experiment.test_features.height() as i32,
        (experiment.train_features.height() + experiment.test_features.height()) as i32,
        chain.test_fraction,
        chain.seed,
        &format!("{:?}", experiment.task_type),
        &chain.target_column,
        &experiment.feature_names,
        experiment.preprocessing.len() as i32,
    );

    Ok(())
}

/// Generate plots for a trained model and store them in pgaugur.plots.
///
/// Requires augur compiled with `features = ["plots"]`. When not available,
/// silently skips. Writes plots to a temp directory, reads PNG bytes, and
/// inserts into the plots table.
fn generate_and_store_plots(
    project_name: &str,
    _model: &augur::prelude::TrainedModel,
    _experiment: &augur::prelude::Experiment,
) {
    #[cfg(feature = "augur_plots")]
    {
        let tmp_dir = match std::env::temp_dir()
            .join(format!("pgaugur_plots_{}", project_name))
            .to_str()
            .map(|s| s.to_string())
        {
            Some(d) => d,
            None => return,
        };

        let plot_config = augur::runner::PlotConfig {
            output_dir: tmp_dir.clone(),
            kinds: vec![],
        };

        let _ = models::clear_plots(project_name);

        let generated = augur::prelude::plot_model(_model, _experiment, &plot_config);
        if let Ok(paths) = generated {
            for path in paths {
                let plot_type = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                let fmt = path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or("png")
                    .to_string();
                if let Ok(data) = std::fs::read(&path) {
                    let _ = models::insert_plot(project_name, &plot_type, &fmt, &data);
                }
            }
        }
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    #[cfg(not(feature = "augur_plots"))]
    {
        let _ = project_name; // suppress unused warning
    }
}

/// Materialize the preprocessed feature data into a real PostgreSQL table.
///
/// Drops the table if it exists, then creates it from the experiment's
/// preprocessed training + test data with the fitted pipeline applied.
fn materialize_features(
    experiment: &augur::prelude::Experiment,
    output_table: &str,
) -> Result<(), AugurPgError> {
    let train = &experiment.train_features;
    let test = &experiment.test_features;

    // Determine column definitions from the training features
    let col_defs: Vec<String> = train
        .get_columns()
        .iter()
        .map(|c| {
            let name = c.name();
            let pg_type = match c.as_materialized_series().dtype() {
                polars::prelude::DataType::Float64 | polars::prelude::DataType::Float32 => "FLOAT8",
                polars::prelude::DataType::Int64 | polars::prelude::DataType::Int32 => "BIGINT",
                polars::prelude::DataType::Boolean => "BOOLEAN",
                _ => "TEXT",
            };
            format!("\"{}\" {}", name, pg_type)
        })
        .collect();

    let drop_sql = format!("DROP TABLE IF EXISTS {}", output_table);
    pgrx::Spi::run(&drop_sql).map_err(|e| AugurPgError::Spi(e.to_string()))?;

    // Add _row_num and _split columns for lineage traceability
    let create_sql = format!(
        "CREATE TABLE {} (_row_num INT, _split TEXT NOT NULL, {})",
        output_table,
        col_defs.join(", ")
    );
    pgrx::Spi::run(&create_sql).map_err(|e| AugurPgError::Spi(e.to_string()))?;

    // Insert train rows with _split = 'train'
    insert_df_rows(output_table, train, "train", 0)?;
    // Insert test rows with _split = 'test', row numbers continue from train
    insert_df_rows(output_table, test, "test", train.height())?;

    Ok(())
}

/// Insert DataFrame rows into the output table with _row_num and _split columns.
fn insert_df_rows(
    output_table: &str,
    df: &DataFrame,
    split: &str,
    row_offset: usize,
) -> Result<(), AugurPgError> {
    let ncols = df.width();
    for row_idx in 0..df.height() {
        let row_num = row_offset + row_idx + 1;
        let mut values: Vec<String> = Vec::with_capacity(ncols + 2);
        values.push(row_num.to_string());
        values.push(crate::models::quote_literal(split));

        for col in df.get_columns() {
            let s = col.as_materialized_series();
            let val = s.get(row_idx).ok();
            match val {
                None => values.push("NULL".to_string()),
                Some(av) => {
                    use polars::prelude::AnyValue;
                    match av {
                        AnyValue::Null => values.push("NULL".to_string()),
                        AnyValue::Float64(f) => values.push(f.to_string()),
                        AnyValue::Float32(f) => values.push(f.to_string()),
                        AnyValue::Int64(i) => values.push(i.to_string()),
                        AnyValue::Int32(i) => values.push(i.to_string()),
                        AnyValue::Boolean(b) => values.push(b.to_string()),
                        AnyValue::String(s) => values.push(crate::models::quote_literal(s)),
                        other => values.push(crate::models::quote_literal(&format!("{}", other))),
                    }
                }
            }
        }
        let insert_sql = format!(
            "INSERT INTO {} VALUES ({})",
            output_table,
            values.join(", ")
        );
        pgrx::Spi::run(&insert_sql).map_err(|e| AugurPgError::Spi(e.to_string()))?;
    }
    Ok(())
}

/// Build SetupConfig from serialized column OPTIONS (no relation access needed).
fn build_setup_config_from_options(
    column_options: &[(String, crate::fdw_options::ColumnOptions)],
    target_column: &str,
    task_str: &str,
    test_fraction: f64,
    seed: u64,
    df: &DataFrame,
) -> Result<(SetupConfig, PgTask), AugurPgError> {
    let pg_task = PgTask::parse(task_str)?;
    let n_classes = if pg_task == PgTask::Classification {
        let col = df.column(target_column).ok();
        col.map(|c| c.as_materialized_series().n_unique().unwrap_or(2))
    } else {
        None
    };
    let task_type = to_augur_task(pg_task, n_classes);

    let mut ignore_columns = Vec::new();
    let mut config = SetupConfig::new(target_column)
        .task_type(task_type)
        .test_fraction(test_fraction)
        .seed(seed);

    for (col_name, col_opts) in column_options {
        if col_opts.role.as_deref() == Some("target") {
            continue;
        }
        if col_opts.role.as_deref() == Some("ignore") || col_opts.role.as_deref() == Some("id") {
            ignore_columns.push(col_name.clone());
            continue;
        }

        let mut cc = ColumnConfig::default();
        if let Some(ref imp) = col_opts.impute {
            cc.impute = Some(parse_impute(imp));
        }
        if let Some(ref enc) = col_opts.encode {
            cc.encode = Some(parse_encoder(enc));
        }
        if let Some(ref sc) = col_opts.scale {
            cc.scale = Some(parse_scaler(sc));
        }
        if cc.impute.is_some() || cc.encode.is_some() || cc.scale.is_some() {
            config = config.column_config(col_name.clone(), cc);
        }

        // String splitting: split column on delimiter before other preprocessing
        if let Some(ref delimiter) = col_opts.split {
            let part_names: Vec<String> = col_opts
                .split_names
                .as_ref()
                .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
                .unwrap_or_default();
            config = config.split_string(col_name.clone(), delimiter.clone(), part_names);
        }
    }

    config = config.ignore(ignore_columns);

    // Global table-level options are applied via the TrainChain fields
    // (missing_indicators, extract_datetime, winsorize, interactions,
    //  discretize, mutual_info_k, conformal). These are wired in
    // execute_train_chain after calling this function.

    Ok((config, pg_task))
}

/// Run an ML action as a side effect: trains model, stores in pgaugur.models.
/// Kept for potential future use (e.g., sync SELECT-based training opt-in).
#[allow(clippy::too_many_arguments, dead_code)]
fn run_action(
    action: &ActionKind,
    experiment: &augur::prelude::Experiment,
    project_name: &str,
    pg_task: PgTask,
    target: &str,
    feature_columns: &[String],
    source_schema: Option<&str>,
    source_table: &str,
) -> Result<(), AugurPgError> {
    match action {
        ActionKind::Compare => {
            let compare_result = compare_models(experiment, None)?;
            if let Some(best) = compare_result.best() {
                store_model(
                    best,
                    experiment,
                    project_name,
                    pg_task,
                    target,
                    feature_columns,
                    source_schema,
                    source_table,
                )?;
            }
            Ok(())
        }
        ActionKind::Create => {
            let model = create_model(experiment, "rf")?;
            store_model(
                &model,
                experiment,
                project_name,
                pg_task,
                target,
                feature_columns,
                source_schema,
                source_table,
            )?;
            Ok(())
        }
        ActionKind::Eda | ActionKind::Tune => Err(AugurPgError::NotSupported(format!(
            "{:?} action via FDW (coming in Phase 2)",
            action
        ))),
    }
}

/// Store a trained model in pgaugur.models.
#[allow(clippy::too_many_arguments)]
fn store_model(
    model: &augur::prelude::TrainedModel,
    experiment: &augur::prelude::Experiment,
    project_name: &str,
    pg_task: PgTask,
    target: &str,
    feature_columns: &[String],
    source_schema: Option<&str>,
    source_table: &str,
) -> Result<(), AugurPgError> {
    let metrics = train::compute_metrics_from_experiment(model, experiment);
    let artifact = save_model_to_string(model, experiment)
        .map_err(AugurPgError::from)?
        .into_bytes();
    let label_classes = train::label_classes_for_experiment(experiment);

    let project_id = models::upsert_project(
        project_name,
        pg_task.as_str(),
        target,
        feature_columns,
        source_schema,
        Some(source_table),
        None,
        None,
        None,
    )?;

    models::insert_model(
        project_id,
        &model.id,
        None,
        &metrics,
        &artifact,
        label_classes.as_deref(),
        true,
        false,
        None,
        0.0,
    )?;

    predict::invalidate(project_name);
    Ok(())
}

/// Convert a DataFrame to rows of Option<String> for returning via FDW.
///
/// Values are formatted as plain strings suitable for PostgreSQL's type
/// input functions (no surrounding quotes for strings).
fn dataframe_to_string_rows(df: &DataFrame) -> Vec<Vec<Option<String>>> {
    let ncols = df.width();
    let nrows = df.height();
    let mut rows = Vec::with_capacity(nrows);

    for row_idx in 0..nrows {
        let mut row = Vec::with_capacity(ncols);
        for col in df.get_columns() {
            let s = col.as_materialized_series();
            let val = s.get(row_idx).ok().and_then(|av| {
                use polars::prelude::AnyValue;
                match av {
                    AnyValue::Null => None,
                    AnyValue::String(s) => Some(s.to_string()),
                    AnyValue::Float64(f) => Some(f.to_string()),
                    AnyValue::Float32(f) => Some(f.to_string()),
                    AnyValue::Int64(i) => Some(i.to_string()),
                    AnyValue::Int32(i) => Some(i.to_string()),
                    AnyValue::Boolean(b) => Some(b.to_string()),
                    other => Some(format!("{}", other)),
                }
            });
            row.push(val);
        }
        rows.push(row);
    }
    rows
}

// ─── Planner Callbacks ────────────────────────

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    // Estimate: 100 rows, 100 bytes each
    (*baserel).rows = 100.0;
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    // Add a single sequential scan path.
    // PG18 added extra parameters (parameterized_required, fdw_restrictinfo).
    #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        std::ptr::null_mut(), // target list
        (*baserel).rows,
        pg_sys::Cost::from(10_u32),  // startup cost
        pg_sys::Cost::from(100_u32), // total cost
        std::ptr::null_mut(),        // pathkeys
        std::ptr::null_mut(),        // required_outer
        std::ptr::null_mut(),        // outer path
        std::ptr::null_mut(),        // fdw_private
    );
    #[cfg(feature = "pg18")]
    let path = pg_sys::create_foreignscan_path(
        root,
        baserel,
        std::ptr::null_mut(), // target list
        (*baserel).rows,
        0,                           // disabled_nodes
        pg_sys::Cost::from(10_u32),  // startup cost
        pg_sys::Cost::from(100_u32), // total cost
        std::ptr::null_mut(),        // pathkeys
        std::ptr::null_mut(),        // required_outer
        std::ptr::null_mut(),        // outer path
        std::ptr::null_mut(),        // fdw_private
        std::ptr::null_mut(),        // fdw_restrictinfo
    );
    pg_sys::add_path(baserel, path as *mut pg_sys::Path);
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    // Extract scan clauses
    let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);

    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        (*baserel).relid,
        std::ptr::null_mut(), // fdw_exprs
        std::ptr::null_mut(), // fdw_private
        std::ptr::null_mut(), // fdw_scan_tlist
        std::ptr::null_mut(), // fdw_recheck_quals
        outer_plan,
    )
}

/// Convert a string value to a Datum of the given type OID.
///
/// Uses PostgreSQL's own type input functions (`OidInputFunctionCall`) which
/// correctly handle pass-by-value vs pass-by-reference across all PG versions.
unsafe fn string_to_datum(val: &str, type_oid: pg_sys::Oid) -> pg_sys::Datum {
    let cstr = std::ffi::CString::new(val).unwrap_or_default();
    let mut typinput = pg_sys::InvalidOid;
    let mut typioparam = pg_sys::InvalidOid;
    pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
    pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, -1)
}

// ─── Executor Callbacks ─────────────────────────

#[pg_guard]
unsafe extern "C-unwind" fn fdw_begin_scan(node: *mut pg_sys::ForeignScanState, _eflags: c_int) {
    let scan_rel = (*node).ss.ss_currentRelation;
    let relid = (*scan_rel).rd_id;

    // Read table options and execute the stage
    let table_opts = match read_table_options(relid) {
        Ok(opts) => opts,
        Err(e) => pgrx::error!("pgaugur FDW: {e}"),
    };

    let (col_names, rows) = match execute_stage(relid, &table_opts) {
        Ok(r) => r,
        Err(e) => pgrx::error!("pgaugur FDW: {e}"),
    };

    let state = Box::new(ScanState {
        rows,
        col_names,
        current: 0,
    });

    (*node).fdw_state = Box::into_raw(state) as *mut std::ffi::c_void;
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_iterate_scan(
    node: *mut pg_sys::ForeignScanState,
) -> *mut pg_sys::TupleTableSlot {
    let slot = (*node).ss.ss_ScanTupleSlot;
    let state = &mut *((*node).fdw_state as *mut ScanState);

    // Clear previous tuple
    let exec_clear = (*(*slot).tts_ops).clear;
    if let Some(clear_fn) = exec_clear {
        clear_fn(slot);
    }

    if state.current >= state.rows.len() {
        return slot; // Return empty slot = end of scan
    }

    let row = &state.rows[state.current];
    state.current += 1;

    let tupdesc = (*slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    // Get the relation OID for type lookups (works across PG14-18)
    let scan_rel = (*node).ss.ss_currentRelation;
    let relid = (*scan_rel).rd_id;

    // Fill slot with values — convert string representations to the
    // actual column types. We handle common types directly and fall
    // back to PostgreSQL's input functions for others.
    for (i, cell) in row.iter().enumerate().take(natts) {
        match cell {
            Some(val) => {
                // get_atttype works across all PG versions (no TupleDescData.attrs dependency)
                let type_oid = pg_sys::get_atttype(relid, (i + 1) as i16);

                let datum = string_to_datum(val, type_oid);
                *(*slot).tts_values.add(i) = datum;
                *(*slot).tts_isnull.add(i) = false;
            }
            None => {
                *(*slot).tts_isnull.add(i) = true;
            }
        }
    }

    // Mark the slot as containing a virtual tuple
    pg_sys::ExecStoreVirtualTuple(slot);
    slot
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_end_scan(node: *mut pg_sys::ForeignScanState) {
    let state_ptr = (*node).fdw_state as *mut ScanState;
    if !state_ptr.is_null() {
        drop(Box::from_raw(state_ptr));
        (*node).fdw_state = std::ptr::null_mut();
    }
}
