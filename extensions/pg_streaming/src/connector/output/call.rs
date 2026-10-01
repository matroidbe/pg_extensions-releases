//! Call output connector — lands each record through a PostgreSQL function
//!
//! Instead of writing a table directly, each record is passed to a function the
//! database already owns: a stored-procedure write API, an `INSTEAD OF` target,
//! per-record identity resolution for RLS, validation richer than a CHECK.
//!
//! Per-record error isolation is this connector's own job. The engine's
//! `error_handling` / `dead_letter` path is *batch*-granular — on failure it
//! writes the entire original batch onward — which is the wrong granularity for
//! a sink calling user code, where one poison record would discard a whole batch
//! of good ones. So each call runs inside a subtransaction (via the PL/pgSQL
//! `EXCEPTION` block in `pgstreams.call_guarded`, which is exactly the
//! `BeginInternalSubTransaction` / `RollbackAndReleaseCurrentSubTransaction`
//! mechanism) and a raise routes that one record onward, then processing
//! continues.
//!
//! `on_record_error: fail` skips the subtransaction entirely — no isolation,
//! cheapest path, the high-throughput choice.
//!
//! See design/pg_streaming/call-sink.md.

use crate::connector::OutputConnector;
use crate::dsl::types::{CallOutputConfig, OnRecordError};
use crate::engine::subxact::in_subtransaction_flat;
use crate::record::RecordBatch;
use pgrx::prelude::*;

/// The bare argument that passes the whole record as `jsonb`.
const RECORD_ARG: &str = "record";

pub struct CallOutput {
    /// Pipeline name, for error_log attribution
    pipeline: String,
    /// Fully-qualified function name (already validated as an identifier)
    function: String,
    /// Number of arguments — used for the compile-time existence check
    arity: usize,
    /// Pre-built `SELECT fn(args...)` statement; takes the single-record
    /// batch as `$1` (a one-element JSONB array)
    call_sql: String,
    /// Pre-built `SELECT set_config($1,$2,true), ...`, or None when unset
    set_config_sql: Option<String>,
    /// Flattened key/value pairs bound to `set_config_sql`
    set_config_args: Vec<String>,
    /// Pre-built `SET LOCAL ROLE <ident>`, or None when unset. The role is
    /// interpolated (SET ROLE takes no parameter), which is safe because
    /// `validate.rs` restricts it to an unquoted identifier.
    set_role_sql: Option<String>,
    /// The same role, passed as a bound parameter to `pgstreams.call_guarded`
    /// on the isolating path so it wraps only the user function.
    assume_role: Option<String>,
    on_record_error: OnRecordError,
    /// Dead-letter sink, compiled from the pipeline's `dead_letter` config
    dead_letter: Option<Box<dyn OutputConnector>>,
}

impl CallOutput {
    /// Build the connector. Pure — no SPI, so this stays unit-testable.
    /// `cte` is the input batch CTE (same preamble the processor chain uses).
    pub fn new(
        config: &CallOutputConfig,
        cte: &str,
        pipeline: &str,
        dead_letter: Option<Box<dyn OutputConnector>>,
    ) -> Self {
        let (set_config_sql, set_config_args) = build_set_config_sql(config);

        Self {
            pipeline: pipeline.to_string(),
            function: config.function.clone(),
            arity: config.args.len(),
            call_sql: build_call_sql(&config.function, &config.args, cte),
            set_config_sql,
            set_config_args,
            set_role_sql: config
                .set_role
                .as_ref()
                .map(|r| format!("SET LOCAL ROLE {}", r)),
            assume_role: config.set_role.clone(),
            on_record_error: config.on_record_error,
            dead_letter,
        }
    }

    /// Resolve the function *name* at compile time, so a typo fails at
    /// `start_pipeline` instead of once per record. The design deliberately
    /// does not re-resolve per call: it buys little and costs a catalog lookup
    /// per record. Any dynamic dispatch belongs in the function body.
    pub fn verify_function_exists(&self) -> Result<(), String> {
        let (schema, name) = split_function_name(&self.function);
        let nargs = self.arity as i32;

        // Accept a match when the requested arity falls between the function's
        // required and declared argument counts (i.e. defaults are allowed to
        // absorb the difference), or when it is variadic.
        let found = match schema {
            Some(schema) => Spi::get_one_with_args::<i64>(
                "SELECT count(*)::bigint FROM pg_catalog.pg_proc p \
                 JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
                 WHERE n.nspname = $1 AND p.proname = $2 \
                   AND (p.provariadic <> 0 \
                        OR ($3 <= p.pronargs \
                            AND $3 >= p.pronargs - COALESCE(p.pronargdefaults, 0)))",
                &[schema.into(), name.into(), nargs.into()],
            ),
            None => Spi::get_one_with_args::<i64>(
                "SELECT count(*)::bigint FROM pg_catalog.pg_proc p \
                 WHERE p.proname = $1 AND pg_catalog.pg_function_is_visible(p.oid) \
                   AND (p.provariadic <> 0 \
                        OR ($2 <= p.pronargs \
                            AND $2 >= p.pronargs - COALESCE(p.pronargdefaults, 0)))",
                &[name.into(), nargs.into()],
            ),
        }
        .map_err(|e| format!("output.call: function lookup failed: {}", e))?
        .unwrap_or(0);

        if found == 0 {
            return Err(format!(
                "output.call: function '{}' taking {} argument(s) does not exist",
                self.function, self.arity
            ));
        }
        Ok(())
    }

    /// Apply `set_config` settings. Deliberately run *outside* the per-record
    /// subtransaction: a rollback would revert them and the next record would
    /// inherit a half-set session.
    fn apply_set_config(&self) -> Result<(), String> {
        let Some(ref sql) = self.set_config_sql else {
            return Ok(());
        };
        // Guarded: an unknown GUC or a value the setting rejects raises, and an
        // escaping raise would restart the executor on every tick. Coming back
        // as Err instead fails the pipeline once, with the reason recorded.
        in_subtransaction_flat(|| {
            let args: Vec<_> = self
                .set_config_args
                .iter()
                .map(|s| s.as_str().into())
                .collect();
            Spi::run_with_args(sql, &args).map_err(|e| e.to_string())
        })
        .map_err(|e| format!("output.call: set_config failed: {}", e))
    }

    /// Assume the configured role for the call. Like `set_config`, applied
    /// *outside* the per-record subtransaction so an inner rollback does not
    /// revert it mid-batch.
    ///
    /// `SET LOCAL` is transaction-scoped, so the role would lapse at the end of
    /// the batch anyway — but it is reset explicitly after every call so the
    /// error path (writing `pgstreams.error_log`, the dead-letter sink) runs as
    /// the worker's own role rather than the application one, which may not be
    /// granted those tables.
    fn apply_set_role(&self) -> Result<(), String> {
        let Some(ref sql) = self.set_role_sql else {
            return Ok(());
        };
        in_subtransaction_flat(|| Spi::run(sql).map_err(|e| e.to_string()))
            .map_err(|e| format!("output.call: set_role failed: {}", e))
    }

    fn reset_role(&self) {
        if self.set_role_sql.is_none() {
            return;
        }
        // Best-effort: a failure here must not mask the record's own outcome,
        // and `SET LOCAL` lapses at commit regardless.
        if let Err(e) = in_subtransaction_flat(|| Spi::run("RESET ROLE").map_err(|e| e.to_string()))
        {
            pgrx::warning!(
                "pg_streaming pipeline '{}': call sink failed to reset role: {}",
                self.pipeline,
                e
            );
        }
    }

    /// Route one failed record onward: always to `pgstreams.error_log`,
    /// and additionally to the dead-letter sink in `dead_letter` mode.
    ///
    /// The record goes to the dead-letter sink **unchanged**, so a dead-letter
    /// table or topic keeps the shape it already expects. The SQLSTATE and
    /// message land in `pgstreams.error_log` alongside the same record.
    ///
    /// Both writes are themselves guarded. This is the error path: it is
    /// reached precisely when something has already gone wrong, and a
    /// dead-letter sink that cannot accept the record (a dead-letter table
    /// missing a column, say) would otherwise raise an error that is *not* a
    /// Rust `Err`, escape `write()`, and restart the executor straight back
    /// into the same record.
    fn route_failure(&self, record: &serde_json::Value, error: &str) {
        let logged = in_subtransaction_flat(|| {
            Spi::run_with_args(
                "INSERT INTO pgstreams.error_log (pipeline, processor, error, record) \
                 VALUES ($1, 'output.call', $2, $3)",
                &[
                    self.pipeline.as_str().into(),
                    error.into(),
                    pgrx::JsonB(record.clone()).into(),
                ],
            )
            .map_err(|e| e.to_string())
        });
        if let Err(e) = logged {
            pgrx::warning!(
                "pg_streaming pipeline '{}': call sink failed to log error: {}",
                self.pipeline,
                e
            );
        }

        if self.on_record_error == OnRecordError::DeadLetter {
            if let Some(ref dl) = self.dead_letter {
                let written = in_subtransaction_flat(|| dl.write(&vec![record.clone()]));
                if let Err(dl_err) = written {
                    pgrx::warning!(
                        "pg_streaming pipeline '{}': call sink dead letter write failed \
                         (record kept in pgstreams.error_log): {}",
                        self.pipeline,
                        dl_err
                    );
                }
            }
        }
    }
}

impl OutputConnector for CallOutput {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        if self.on_record_error == OnRecordError::Fail {
            // No *per-record* isolation — that is the point of `fail`, and what
            // makes it the cheap path. One subtransaction wraps the whole batch
            // so the first raise still discards every record in it, but comes
            // back as an Err that fails the pipeline once, instead of escaping
            // and restarting the executor into the same poison record forever.
            return in_subtransaction_flat(|| {
                for record in records {
                    self.apply_set_config()?;
                    self.apply_set_role()?;
                    let payload = pgrx::JsonB(serde_json::Value::Array(vec![record.clone()]));
                    let outcome = Spi::run_with_args(&self.call_sql, &[payload.into()])
                        .map_err(|e| e.to_string());
                    self.reset_role();
                    outcome?;
                }
                Ok(())
            })
            .map_err(|e| format!("output.call: '{}' failed: {}", self.function, e));
        }

        for record in records {
            // One-element array: the CTE unpacks it with jsonb_array_elements.
            let payload = pgrx::JsonB(serde_json::Value::Array(vec![record.clone()]));

            // Settings first, then enter the subtransaction.
            self.apply_set_config()?;

            // The role is passed INTO call_guarded rather than assumed around
            // this call: setting it out here means invoking
            // `pgstreams.call_guarded` itself as the application role, which
            // has no rights on the pgstreams schema — the pipeline then fails
            // with `permission denied for schema pgstreams` before the user
            // function is ever reached.
            let err = Spi::get_one_with_args::<pgrx::JsonB>(
                "SELECT pgstreams.call_guarded($1, $2, $3)",
                &[
                    self.call_sql.as_str().into(),
                    payload.into(),
                    self.assume_role.as_deref().into(),
                ],
            )
            .map_err(|e| format!("output.call: '{}' failed: {}", self.function, e))?;

            if let Some(err) = err {
                let sqlstate = err.0.get("sqlstate").and_then(|v| v.as_str()).unwrap_or("");
                let message = err.0.get("message").and_then(|v| v.as_str()).unwrap_or("");
                let detail = format!("{}: {} [{}]", self.function, message, sqlstate);

                log!(
                    "pg_streaming pipeline '{}': call sink record error ({:?}): {}",
                    self.pipeline,
                    self.on_record_error,
                    detail
                );
                self.route_failure(record, &detail);
            }
        }

        Ok(())
    }
}

/// Split `schema.function` into its parts. Unqualified names resolve through
/// `search_path`.
fn split_function_name(function: &str) -> (Option<&str>, &str) {
    match function.split_once('.') {
        Some((schema, name)) => (Some(schema), name),
        None => (None, function),
    }
}

/// Validate a function name for direct interpolation into SQL. Accepts one or
/// two unquoted identifier segments (`function` or `schema.function`).
pub fn is_valid_function_name(function: &str) -> bool {
    let segments: Vec<&str> = function.split('.').collect();
    if segments.is_empty() || segments.len() > 2 {
        return false;
    }
    segments.iter().all(|seg| is_identifier(seg))
}

fn is_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

/// Validate a role name for `set_role`. Interpolated into `SET LOCAL ROLE`,
/// which takes no parameter, so it must be a bare identifier.
pub fn is_valid_role_name(name: &str) -> bool {
    is_identifier(name)
}

/// Validate a GUC name for `set_config`. Postgres allows `class.name` for
/// custom settings; keep it to identifier characters.
pub fn is_valid_setting_name(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    if segments.len() > 2 {
        return false;
    }
    segments.iter().all(|seg| is_identifier(seg))
}

/// Build the per-record call statement.
///
/// When every argument is the bare word `record`, the batch CTE is skipped
/// entirely — nothing needs the unpacked columns, and a *typed* input CTE
/// would otherwise re-cast columns that a mapping processor may have already
/// reshaped away.
fn build_call_sql(function: &str, args: &[String], cte: &str) -> String {
    let all_bare_record = args.iter().all(|a| a.trim() == RECORD_ARG);

    if all_bare_record {
        let arg_list = vec!["r"; args.len()].join(", ");
        return format!(
            "SELECT {}({}) FROM jsonb_array_elements($1) AS r",
            function, arg_list
        );
    }

    let arg_list: Vec<String> = args
        .iter()
        .map(|a| {
            if a.trim() == RECORD_ARG {
                "_original".to_string()
            } else {
                format!("({})", a)
            }
        })
        .collect();

    format!(
        "{}SELECT {}({}) FROM _batch",
        cte,
        function,
        arg_list.join(", ")
    )
}

/// Build the `set_config` statement and its bound arguments. Values are bound,
/// never interpolated.
fn build_set_config_sql(config: &CallOutputConfig) -> (Option<String>, Vec<String>) {
    if config.set_config.is_empty() {
        return (None, Vec::new());
    }

    let mut calls = Vec::new();
    let mut args = Vec::new();
    for (key, value) in &config.set_config {
        let n = args.len();
        calls.push(format!("set_config(${}, ${}, true)", n + 1, n + 2));
        args.push(key.clone());
        args.push(value.clone());
    }

    (Some(format!("SELECT {}", calls.join(", "))), args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch_cte;
    use std::collections::BTreeMap;

    fn config(function: &str, args: &[&str]) -> CallOutputConfig {
        CallOutputConfig {
            function: function.to_string(),
            args: args.iter().map(|s| s.to_string()).collect(),
            set_config: BTreeMap::new(),
            set_role: None,
            on_record_error: OnRecordError::DeadLetter,
            batch: false,
        }
    }

    // =========================================================================
    // Call SQL generation
    // =========================================================================

    #[test]
    fn test_bare_record_arg_skips_cte() {
        let sql = build_call_sql("app.ingest", &["record".to_string()], batch_cte());
        assert_eq!(
            sql,
            "SELECT app.ingest(r) FROM jsonb_array_elements($1) AS r"
        );
        // No CTE — nothing needs the unpacked columns.
        assert!(!sql.contains("WITH _batch"));
    }

    #[test]
    fn test_expression_arg_uses_cte() {
        let sql = build_call_sql(
            "app.ingest",
            &["value_json->>'id'".to_string()],
            batch_cte(),
        );
        assert!(sql.starts_with("WITH _batch AS"));
        assert!(sql.contains("SELECT app.ingest((value_json->>'id')) FROM _batch"));
    }

    #[test]
    fn test_mixed_args_map_record_to_original() {
        let sql = build_call_sql(
            "app.ingest",
            &["record".to_string(), "'orders'".to_string()],
            batch_cte(),
        );
        assert!(sql.starts_with("WITH _batch AS"));
        assert!(sql.contains("app.ingest(_original, ('orders'))"));
    }

    #[test]
    fn test_literal_arg() {
        let sql = build_call_sql("app.ingest", &["'literal'".to_string()], batch_cte());
        assert!(sql.contains("app.ingest(('literal'))"));
    }

    #[test]
    fn test_multiple_bare_record_args() {
        let sql = build_call_sql(
            "app.ingest",
            &["record".to_string(), "record".to_string()],
            batch_cte(),
        );
        assert_eq!(
            sql,
            "SELECT app.ingest(r, r) FROM jsonb_array_elements($1) AS r"
        );
    }

    #[test]
    fn test_zero_args() {
        let sql = build_call_sql("app.tick", &[], batch_cte());
        // No args at all is still "all bare record" vacuously — no CTE needed.
        assert_eq!(sql, "SELECT app.tick() FROM jsonb_array_elements($1) AS r");
    }

    #[test]
    fn test_arg_whitespace_is_tolerated() {
        let sql = build_call_sql("app.ingest", &["  record  ".to_string()], batch_cte());
        assert!(sql.contains("jsonb_array_elements"));
        assert!(!sql.contains("WITH _batch"));
    }

    // =========================================================================
    // set_role

    #[test]
    fn test_set_role_none_when_unset() {
        let out = CallOutput::new(&config("app.f", &["record"]), batch_cte(), "p", None);
        assert!(out.set_role_sql.is_none());
    }

    /// The isolating path passes the role to `call_guarded` rather than
    /// assuming it around the call, so `assume_role` must be populated from
    /// config. Assuming it around the call site fails with
    /// `permission denied for schema pgstreams`.
    #[test]
    fn test_set_role_is_passed_through_to_call_guarded() {
        let mut c = config("app.f", &["record"]);
        c.set_role = Some("app_user".to_string());
        let out = CallOutput::new(&c, batch_cte(), "p", None);
        assert_eq!(out.assume_role.as_deref(), Some("app_user"));

        let none = CallOutput::new(&config("app.f", &["record"]), batch_cte(), "p", None);
        assert!(none.assume_role.is_none());
    }

    #[test]
    fn test_set_role_builds_local_statement() {
        let mut c = config("app.f", &["record"]);
        c.set_role = Some("app_user".to_string());
        let out = CallOutput::new(&c, batch_cte(), "p", None);
        // LOCAL, not session-wide: it must lapse with the batch transaction.
        assert_eq!(out.set_role_sql.as_deref(), Some("SET LOCAL ROLE app_user"));
    }

    #[test]
    fn test_valid_role_names() {
        assert!(is_valid_role_name("app_user"));
        assert!(is_valid_role_name("_svc"));
        assert!(is_valid_role_name("r2d2"));
    }

    /// The role is interpolated into `SET LOCAL ROLE` because that statement
    /// takes no parameter, so the identifier rule is the only thing standing
    /// between the config and SQL injection.
    #[test]
    fn test_invalid_role_names_rejected() {
        assert!(!is_valid_role_name("app_user; DROP TABLE x"));
        assert!(!is_valid_role_name("\"quoted\""));
        assert!(!is_valid_role_name("2fast"));
        assert!(!is_valid_role_name(""));
        assert!(!is_valid_role_name("a.b"));
    }

    // set_config
    // =========================================================================

    #[test]
    fn test_set_config_none_when_empty() {
        let (sql, args) = build_set_config_sql(&config("app.f", &["record"]));
        assert!(sql.is_none());
        assert!(args.is_empty());
    }

    #[test]
    fn test_set_config_binds_values() {
        let mut cfg = config("app.f", &["record"]);
        cfg.set_config
            .insert("app.user_role".to_string(), "ingest_service".to_string());
        let (sql, args) = build_set_config_sql(&cfg);
        assert_eq!(sql.unwrap(), "SELECT set_config($1, $2, true)");
        assert_eq!(args, vec!["app.user_role", "ingest_service"]);
    }

    #[test]
    fn test_set_config_multiple_is_deterministic() {
        let mut cfg = config("app.f", &["record"]);
        cfg.set_config
            .insert("z.setting".to_string(), "last".to_string());
        cfg.set_config
            .insert("a.setting".to_string(), "first".to_string());
        let (sql, args) = build_set_config_sql(&cfg);
        assert_eq!(
            sql.unwrap(),
            "SELECT set_config($1, $2, true), set_config($3, $4, true)"
        );
        // BTreeMap ordering — 'a' before 'z', regardless of insertion order.
        assert_eq!(args, vec!["a.setting", "first", "z.setting", "last"]);
    }

    #[test]
    fn test_set_config_value_is_not_interpolated() {
        let mut cfg = config("app.f", &["record"]);
        cfg.set_config.insert(
            "app.role".to_string(),
            "o'brien'); DROP TABLE t; --".to_string(),
        );
        let (sql, args) = build_set_config_sql(&cfg);
        // The injection payload lives in the bound args, never in the SQL text.
        assert!(!sql.unwrap().contains("DROP TABLE"));
        assert!(args[1].contains("DROP TABLE"));
    }

    // =========================================================================
    // Name validation
    // =========================================================================

    #[test]
    fn test_valid_function_names() {
        assert!(is_valid_function_name("ingest_order"));
        assert!(is_valid_function_name("myschema.ingest_order"));
        assert!(is_valid_function_name("_private.fn$1"));
    }

    #[test]
    fn test_invalid_function_names() {
        assert!(!is_valid_function_name(""));
        assert!(!is_valid_function_name("a.b.c"));
        assert!(!is_valid_function_name("1bad"));
        assert!(!is_valid_function_name("drop table t; --"));
        assert!(!is_valid_function_name("fn(1)"));
        assert!(!is_valid_function_name("sch ema.fn"));
        assert!(!is_valid_function_name("\"quoted\".fn"));
    }

    #[test]
    fn test_valid_setting_names() {
        assert!(is_valid_setting_name("app.user_role"));
        assert!(is_valid_setting_name("work_mem"));
        assert!(!is_valid_setting_name("a.b.c"));
        assert!(!is_valid_setting_name("app.role'); --"));
    }

    // =========================================================================
    // split_function_name
    // =========================================================================

    #[test]
    fn test_split_function_name() {
        assert_eq!(split_function_name("app.ingest"), (Some("app"), "ingest"));
        assert_eq!(split_function_name("ingest"), (None, "ingest"));
    }
}
