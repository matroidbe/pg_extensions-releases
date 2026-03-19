//! CEP (Complex Event Processing) pattern-matching processor
//!
//! Detects ordered event sequences per key within time-bounded windows.
//! State machine transitions run in Rust; state persistence uses SQL.

use crate::dsl::types::CepConfig;
use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use std::collections::HashMap;

// =============================================================================
// Quantifier
// =============================================================================

#[derive(Debug, Clone, PartialEq)]
pub enum Quantifier {
    /// Exactly N matches required
    Exact(i32),
    /// N or more matches (greedy — advances when next step also matches)
    AtLeast(i32),
    /// Zero or one match (skip if event doesn't match)
    Optional,
}

pub fn parse_quantifier(s: &str) -> Result<Quantifier, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("quantifier is empty".to_string());
    }
    if s == "1?" {
        return Ok(Quantifier::Optional);
    }
    if s.ends_with('?') {
        return Err(format!("invalid quantifier '{}': optional must be '1?'", s));
    }
    if let Some(prefix) = s.strip_suffix('+') {
        let n: i32 = prefix
            .parse()
            .map_err(|_| format!("invalid quantifier '{}'", s))?;
        if n < 1 {
            return Err(format!("quantifier '{}': N must be >= 1", s));
        }
        return Ok(Quantifier::AtLeast(n));
    }
    let n: i32 = s
        .parse()
        .map_err(|_| format!("invalid quantifier '{}'", s))?;
    if n < 1 {
        return Err(format!("quantifier '{}': must be >= 1", s));
    }
    Ok(Quantifier::Exact(n))
}

// =============================================================================
// Parsed step
// =============================================================================

#[derive(Debug, Clone)]
pub(crate) struct ParsedStep {
    #[allow(dead_code)]
    name: String,
    condition: String,
    quantifier: Quantifier,
}

// =============================================================================
// State machine types (pure Rust, no SPI)
// =============================================================================

#[derive(Debug, Clone)]
pub struct PatternState {
    pub step_index: usize,
    pub step_count: i32,
    pub match_start: Option<String>, // ISO timestamp from DB, None if new
    pub events: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct CompletedPattern {
    pub key: String,
    pub events: Vec<serde_json::Value>,
    pub match_start: Option<String>,
}

/// Annotated event from the load SQL
#[derive(Debug, Clone)]
pub(crate) struct AnnotatedEvent {
    pk: String,
    event: serde_json::Value,
    matches: Vec<bool>,
    state: Option<PatternState>,
}

// =============================================================================
// State machine logic (pure Rust — highly testable)
// =============================================================================

/// Advance a pattern state with one event. Returns true if pattern completes.
pub(crate) fn advance_state(
    steps: &[ParsedStep],
    state: &mut PatternState,
    event_matches: &[bool],
    event: &serde_json::Value,
) -> bool {
    if state.step_index >= steps.len() {
        return false; // already complete (shouldn't happen)
    }

    let step = &steps[state.step_index];

    if event_matches[state.step_index] {
        // Event matches current step
        state.step_count += 1;
        state.events.push(event.clone());

        match &step.quantifier {
            Quantifier::Exact(n) => {
                if state.step_count >= *n {
                    return advance_to_next(steps, state);
                }
            }
            Quantifier::AtLeast(n) => {
                if state.step_count >= *n {
                    // Check if next step also matches (greedy lookahead)
                    let next = state.step_index + 1;
                    if next < steps.len() && event_matches[next] {
                        if advance_to_next(steps, state) {
                            return true;
                        }
                        // Event also matches the new current step — consume it
                        state.step_count = 1;
                        // event already in state.events from above
                        let next_step = &steps[state.step_index];
                        match &next_step.quantifier {
                            Quantifier::Exact(m) if state.step_count >= *m => {
                                return advance_to_next(steps, state);
                            }
                            Quantifier::Optional => {
                                return advance_to_next(steps, state);
                            }
                            _ => {}
                        }
                    }
                    // Otherwise stay on current step, keep accumulating
                }
            }
            Quantifier::Optional => {
                // Optional step matched — advance
                return advance_to_next(steps, state);
            }
        }
    } else if step.quantifier == Quantifier::Optional {
        // Optional step doesn't match — skip it without consuming the event
        state.step_index += 1;
        state.step_count = 0;

        // Check if current event matches the NEW current step
        if state.step_index < steps.len() && event_matches[state.step_index] {
            state.step_count = 1;
            state.events.push(event.clone());

            let step = &steps[state.step_index];
            match &step.quantifier {
                Quantifier::Exact(n) if state.step_count >= *n => {
                    return advance_to_next(steps, state);
                }
                Quantifier::AtLeast(n) if state.step_count >= *n => {
                    let next = state.step_index + 1;
                    if next < steps.len() && event_matches[next] {
                        return advance_to_next(steps, state);
                    }
                }
                Quantifier::Optional => {
                    return advance_to_next(steps, state);
                }
                _ => {}
            }
        } else if state.step_index >= steps.len() {
            // Trailing optional was the last step — pattern complete
            return true;
        }
    }
    // else: event doesn't match current non-optional step — ignore

    false
}

/// Advance to the next step, skipping trailing optional steps.
/// Returns true if pattern is complete.
fn advance_to_next(steps: &[ParsedStep], state: &mut PatternState) -> bool {
    state.step_index += 1;
    state.step_count = 0;

    // Only skip optional steps if ALL remaining steps are optional (truly trailing)
    if state.step_index < steps.len()
        && steps[state.step_index..]
            .iter()
            .all(|s| s.quantifier == Quantifier::Optional)
    {
        state.step_index = steps.len();
    }

    state.step_index >= steps.len()
}

/// Run the state machine for a batch of annotated events.
/// Returns (completed patterns, state updates keyed by pk).
pub(crate) fn run_state_machine(
    steps: &[ParsedStep],
    annotated: Vec<AnnotatedEvent>,
) -> (Vec<CompletedPattern>, HashMap<String, PatternState>) {
    let mut active: HashMap<String, PatternState> = HashMap::new();
    let mut completed: Vec<CompletedPattern> = Vec::new();

    // Seed active map from events that have existing state
    for event in &annotated {
        if let Some(ref state) = event.state {
            active
                .entry(event.pk.clone())
                .or_insert_with(|| state.clone());
        }
    }

    for event in &annotated {
        let pk = &event.pk;

        if let Some(state) = active.get_mut(pk) {
            let done = advance_state(steps, state, &event.matches, &event.event);
            if done {
                completed.push(CompletedPattern {
                    key: pk.clone(),
                    events: state.events.clone(),
                    match_start: state.match_start.clone(),
                });
                active.remove(pk);
            }
        } else {
            // No active pattern — check if step 0 matches
            if !event.matches.is_empty() && event.matches[0] {
                let mut new_state = PatternState {
                    step_index: 0,
                    step_count: 0,
                    match_start: None,
                    events: vec![],
                };
                let done = advance_state(steps, &mut new_state, &event.matches, &event.event);
                if done {
                    completed.push(CompletedPattern {
                        key: pk.clone(),
                        events: new_state.events,
                        match_start: None,
                    });
                } else {
                    active.insert(pk.clone(), new_state);
                }
            }
        }
    }

    (completed, active)
}

// =============================================================================
// SQL generation
// =============================================================================

/// Generate DDL for the CEP state table
fn cep_state_ddl(state_table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         pattern_key TEXT NOT NULL PRIMARY KEY, \
         step_index  INT NOT NULL DEFAULT 0, \
         step_count  INT NOT NULL DEFAULT 0, \
         match_start TIMESTAMPTZ NOT NULL DEFAULT now(), \
         expires_at  TIMESTAMPTZ NOT NULL, \
         events      JSONB NOT NULL DEFAULT '[]'::jsonb, \
         updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()\
         );\
         CREATE INDEX IF NOT EXISTS {}_expires_idx ON {} (expires_at)",
        state_table,
        state_table.replace('.', "_"),
        state_table
    )
}

/// Build the load SQL that joins batch events with existing state
fn build_load_sql(cte: &str, key_expr: &str, steps: &[ParsedStep], state_table: &str) -> String {
    // Build boolean match columns for each step
    let match_cols: Vec<String> = steps
        .iter()
        .enumerate()
        .map(|(i, s)| format!("({})::boolean AS _m{}", s.condition, i))
        .collect();

    let match_array: Vec<String> = (0..steps.len()).map(|i| format!("k._m{}", i)).collect();

    format!(
        "{}, \
         _keyed AS (\
           SELECT ({})::text AS _pk, _original, {}, \
                  row_number() OVER (ORDER BY offset_id) AS _rn \
           FROM _batch\
         ) \
         SELECT COALESCE(jsonb_agg(jsonb_build_object(\
           'pk', k._pk, \
           'event', k._original, \
           'matches', jsonb_build_array({}), \
           'rn', k._rn, \
           'state', CASE WHEN s.pattern_key IS NOT NULL THEN \
             jsonb_build_object(\
               'step_index', s.step_index, \
               'step_count', s.step_count, \
               'match_start', s.match_start, \
               'events', s.events\
             ) ELSE NULL END\
         ) ORDER BY k._rn), '[]'::jsonb) \
         FROM _keyed k \
         LEFT JOIN {} s ON s.pattern_key = k._pk AND s.expires_at > now()",
        cte,
        key_expr,
        match_cols.join(", "),
        match_array.join(", "),
        state_table
    )
}

/// Build the write SQL for upserting state changes
fn build_write_sql(state_table: &str, within: &str) -> String {
    format!(
        "WITH _changes AS (\
           SELECT \
             r->>'pk' AS pattern_key, \
             (r->>'step_index')::int AS step_index, \
             (r->>'step_count')::int AS step_count, \
             COALESCE((r->>'match_start')::timestamptz, now()) AS match_start, \
             COALESCE((r->>'match_start')::timestamptz, now()) + interval '{}' AS expires_at, \
             (r->'events') AS events \
           FROM jsonb_array_elements($1) AS r\
         ) \
         INSERT INTO {} (pattern_key, step_index, step_count, match_start, expires_at, events, updated_at) \
         SELECT pattern_key, step_index, step_count, match_start, expires_at, events, now() \
         FROM _changes \
         ON CONFLICT (pattern_key) DO UPDATE SET \
           step_index = EXCLUDED.step_index, \
           step_count = EXCLUDED.step_count, \
           events = EXCLUDED.events, \
           updated_at = now()",
        within, state_table
    )
}

/// Build the emit SQL that creates output records from completed patterns
fn build_emit_sql(emit: &[(String, String)]) -> String {
    // Build jsonb_build_object pairs
    let pairs: Vec<String> = emit
        .iter()
        .map(|(name, expr)| {
            // Replace special variables
            let resolved = expr
                .replace("_matched_count", "jsonb_array_length(_c._events)")
                .replace("_match_start", "_c._match_start")
                .replace("_match_end", "now()")
                .replace("_matched_events", "_c._events");
            // Replace top-level column references to use first event
            // e.g., value_json->>'user_id' → (_c._events->0)->>'user_id'
            format!("'{}', {}", name, resolved)
        })
        .collect();

    format!(
        "WITH _completed AS (\
           SELECT \
             r->>'key' AS _key, \
             (r->'events') AS _events, \
             (r->>'match_start')::timestamptz AS _match_start \
           FROM jsonb_array_elements($1) AS r\
         ) \
         SELECT COALESCE(jsonb_agg(jsonb_build_object({})), '[]'::jsonb) \
         FROM _completed _c",
        pairs.join(", ")
    )
}

/// Build the cleanup SQL for expired partial matches
fn build_cleanup_sql(state_table: &str) -> String {
    format!("DELETE FROM {} WHERE expires_at <= now()", state_table)
}

/// Build the delete SQL for completed patterns
fn build_delete_sql(state_table: &str) -> String {
    format!(
        "DELETE FROM {} WHERE pattern_key = ANY(\
         SELECT r::text FROM jsonb_array_elements_text($1) AS r)",
        state_table
    )
}

// =============================================================================
// CepProcessor
// =============================================================================

pub struct CepProcessor {
    state_table: String,
    #[allow(dead_code)]
    pipeline_name: String,
    steps: Vec<ParsedStep>,
    load_sql: String,
    write_sql: String,
    emit_sql: String,
    cleanup_sql: String,
    delete_sql: String,
}

impl CepProcessor {
    pub fn new(config: &CepConfig, pipeline_name: &str, cte: &str) -> Result<Self, String> {
        let steps: Vec<ParsedStep> = config
            .pattern
            .iter()
            .map(|s| {
                Ok(ParsedStep {
                    name: s.name.clone(),
                    condition: s.condition.clone(),
                    quantifier: parse_quantifier(&s.times)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;

        let table_name = format!(
            "pgstreams.{}_cep_state",
            pipeline_name.replace(['-', '.'], "_")
        );

        // Sort emit fields for deterministic SQL
        let mut emit: Vec<(String, String)> = config.emit.clone().into_iter().collect();
        emit.sort_by(|a, b| a.0.cmp(&b.0));

        let load_sql = build_load_sql(cte, &config.key, &steps, &table_name);
        let write_sql = build_write_sql(&table_name, &config.within);
        let emit_sql = build_emit_sql(&emit);
        let cleanup_sql = build_cleanup_sql(&table_name);
        let delete_sql = build_delete_sql(&table_name);

        Ok(Self {
            state_table: table_name,
            pipeline_name: pipeline_name.to_string(),
            steps,
            load_sql,
            write_sql,
            emit_sql,
            cleanup_sql,
            delete_sql,
        })
    }

    pub fn ensure_state_table(&self) -> Result<(), String> {
        let ddl = cep_state_ddl(&self.state_table);
        Spi::run(&ddl).map_err(|e| format!("Failed to create CEP state table: {}", e))?;

        // Register in state_tables
        Spi::run_with_args(
            "INSERT INTO pgstreams.state_tables (pipeline, processor, table_name, table_type, definition) \
             VALUES ($1, $2, $3, 'cep', '{}'::jsonb) \
             ON CONFLICT (table_name) DO NOTHING",
            &[
                self.pipeline_name.as_str().into(),
                "cep".into(),
                self.state_table.as_str().into(),
            ],
        )
        .map_err(|e| format!("Failed to register CEP state table: {}", e))?;

        Ok(())
    }

    /// Parse annotated events from load SQL result
    fn parse_annotated(&self, jsonb: &serde_json::Value) -> Vec<AnnotatedEvent> {
        let arr = match jsonb.as_array() {
            Some(a) => a,
            None => return vec![],
        };

        arr.iter()
            .map(|row| {
                let pk = row["pk"].as_str().unwrap_or("").to_string();
                let event = row["event"].clone();
                let matches: Vec<bool> = row["matches"]
                    .as_array()
                    .map(|a| a.iter().map(|v| v.as_bool().unwrap_or(false)).collect())
                    .unwrap_or_default();
                let state = if row["state"].is_null() {
                    None
                } else {
                    let s = &row["state"];
                    Some(PatternState {
                        step_index: s["step_index"].as_i64().unwrap_or(0) as usize,
                        step_count: s["step_count"].as_i64().unwrap_or(0) as i32,
                        match_start: s["match_start"].as_str().map(|s| s.to_string()),
                        events: s["events"].as_array().cloned().unwrap_or_default(),
                    })
                };
                AnnotatedEvent {
                    pk,
                    event,
                    matches,
                    state,
                }
            })
            .collect()
    }

    #[cfg(test)]
    pub fn load_sql(&self) -> &str {
        &self.load_sql
    }

    #[cfg(test)]
    pub fn write_sql(&self) -> &str {
        &self.write_sql
    }

    #[cfg(test)]
    pub fn emit_sql(&self) -> &str {
        &self.emit_sql
    }
}

impl Processor for CepProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            // Still clean up expired patterns
            Spi::run(&self.cleanup_sql).map_err(|e| format!("CEP cleanup failed: {}", e))?;
            return Ok(vec![]);
        }

        // 1. Load states + annotated events
        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json);

        let load_result =
            Spi::get_one_with_args::<pgrx::JsonB>(&self.load_sql, &[batch_jsonb.into()])
                .map_err(|e| format!("CEP load failed: {}", e))?
                .unwrap_or(pgrx::JsonB(serde_json::json!([])));

        let annotated = self.parse_annotated(&load_result.0);

        // 2. Run state machine in Rust
        let (completed, active_states) = run_state_machine(&self.steps, annotated);

        // 3. Write state changes (upsert active states)
        if !active_states.is_empty() {
            let changes: Vec<serde_json::Value> = active_states
                .iter()
                .map(|(pk, state)| {
                    serde_json::json!({
                        "pk": pk,
                        "step_index": state.step_index,
                        "step_count": state.step_count,
                        "match_start": state.match_start,
                        "events": state.events,
                    })
                })
                .collect();
            let changes_json = serde_json::Value::Array(changes);
            Spi::run_with_args(&self.write_sql, &[pgrx::JsonB(changes_json).into()])
                .map_err(|e| format!("CEP write failed: {}", e))?;
        }

        // 4. Delete completed patterns from state
        if !completed.is_empty() {
            let keys: Vec<serde_json::Value> = completed
                .iter()
                .map(|c| serde_json::Value::String(c.key.clone()))
                .collect();
            let keys_json = serde_json::Value::Array(keys);
            Spi::run_with_args(&self.delete_sql, &[pgrx::JsonB(keys_json).into()])
                .map_err(|e| format!("CEP delete failed: {}", e))?;
        }

        // 5. Cleanup expired
        Spi::run(&self.cleanup_sql).map_err(|e| format!("CEP cleanup failed: {}", e))?;

        // 6. Build emit records from completed patterns
        if completed.is_empty() {
            return Ok(vec![]);
        }

        let completed_json: Vec<serde_json::Value> = completed
            .iter()
            .map(|c| {
                serde_json::json!({
                    "key": c.key,
                    "events": c.events,
                    "match_start": c.match_start,
                })
            })
            .collect();
        let completed_jsonb = pgrx::JsonB(serde_json::Value::Array(completed_json));

        let emit_result =
            Spi::get_one_with_args::<pgrx::JsonB>(&self.emit_sql, &[completed_jsonb.into()])
                .map_err(|e| format!("CEP emit failed: {}", e))?
                .unwrap_or(pgrx::JsonB(serde_json::json!([])));

        let emitted = emit_result.0.as_array().cloned().unwrap_or_default();

        Ok(emitted)
    }

    fn name(&self) -> &str {
        "cep"
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::types::{CepConfig, CepPatternStep};

    fn messages_cte() -> String {
        crate::record::TopicShape::Messages.batch_cte()
    }

    // ---- Quantifier parsing ----

    #[test]
    fn test_parse_quantifier_exact() {
        assert_eq!(parse_quantifier("1"), Ok(Quantifier::Exact(1)));
        assert_eq!(parse_quantifier("3"), Ok(Quantifier::Exact(3)));
    }

    #[test]
    fn test_parse_quantifier_at_least() {
        assert_eq!(parse_quantifier("3+"), Ok(Quantifier::AtLeast(3)));
        assert_eq!(parse_quantifier("1+"), Ok(Quantifier::AtLeast(1)));
    }

    #[test]
    fn test_parse_quantifier_optional() {
        assert_eq!(parse_quantifier("1?"), Ok(Quantifier::Optional));
    }

    #[test]
    fn test_parse_quantifier_invalid() {
        assert!(parse_quantifier("0").is_err());
        assert!(parse_quantifier("abc").is_err());
        assert!(parse_quantifier("2?").is_err());
        assert!(parse_quantifier("").is_err());
        assert!(parse_quantifier("0+").is_err());
    }

    // ---- State table DDL ----

    #[test]
    fn test_cep_state_ddl() {
        let ddl = cep_state_ddl("pgstreams.test_cep_state");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pgstreams.test_cep_state"));
        assert!(ddl.contains("pattern_key TEXT NOT NULL PRIMARY KEY"));
        assert!(ddl.contains("step_index  INT NOT NULL DEFAULT 0"));
        assert!(ddl.contains("events      JSONB NOT NULL DEFAULT '[]'::jsonb"));
        assert!(ddl.contains("expires_at  TIMESTAMPTZ NOT NULL"));
        assert!(ddl.contains("CREATE INDEX IF NOT EXISTS"));
    }

    // ---- SQL generation ----

    fn test_config() -> CepConfig {
        CepConfig {
            key: "value_json->>'user_id'".to_string(),
            pattern: vec![
                CepPatternStep {
                    name: "fail".to_string(),
                    condition: "value_json->>'event' = 'login_failed'".to_string(),
                    times: "3+".to_string(),
                },
                CepPatternStep {
                    name: "lock".to_string(),
                    condition: "value_json->>'event' = 'account_locked'".to_string(),
                    times: "1".to_string(),
                },
            ],
            within: "5 minutes".to_string(),
            emit: HashMap::from([
                ("user_id".to_string(), "value_json->>'user_id'".to_string()),
                ("fail_count".to_string(), "_matched_count".to_string()),
            ]),
        }
    }

    #[test]
    fn test_load_sql_structure() {
        let proc = CepProcessor::new(&test_config(), "test_pipe", &messages_cte()).unwrap();
        let sql = proc.load_sql();
        assert!(sql.contains("_keyed AS"));
        assert!(sql.contains("value_json->>'user_id'"));
        assert!(sql.contains("value_json->>'event' = 'login_failed'"));
        assert!(sql.contains("value_json->>'event' = 'account_locked'"));
        assert!(sql.contains("_m0"));
        assert!(sql.contains("_m1"));
        assert!(sql.contains("LEFT JOIN pgstreams.test_pipe_cep_state"));
        assert!(sql.contains("row_number()"));
        assert!(sql.contains("jsonb_agg"));
    }

    #[test]
    fn test_write_sql_structure() {
        let proc = CepProcessor::new(&test_config(), "test_pipe", &messages_cte()).unwrap();
        let sql = proc.write_sql();
        assert!(sql.contains("INSERT INTO pgstreams.test_pipe_cep_state"));
        assert!(sql.contains("ON CONFLICT (pattern_key) DO UPDATE"));
        assert!(sql.contains("interval '5 minutes'"));
    }

    #[test]
    fn test_emit_sql_structure() {
        let proc = CepProcessor::new(&test_config(), "test_pipe", &messages_cte()).unwrap();
        let sql = proc.emit_sql();
        assert!(sql.contains("jsonb_array_length(_c._events)")); // _matched_count
        assert!(sql.contains("'user_id'")); // field name
    }

    // ---- State machine ----

    fn make_steps(specs: &[(&str, &str, &str)]) -> Vec<ParsedStep> {
        specs
            .iter()
            .map(|(name, cond, times)| ParsedStep {
                name: name.to_string(),
                condition: cond.to_string(),
                quantifier: parse_quantifier(times).unwrap(),
            })
            .collect()
    }

    #[test]
    fn test_advance_exact_simple() {
        let steps = make_steps(&[("a", "true", "1"), ("b", "true", "1")]);
        let mut state = PatternState {
            step_index: 0,
            step_count: 0,
            match_start: None,
            events: vec![],
        };

        // Event matches step 0 only
        let done = advance_state(
            &steps,
            &mut state,
            &[true, false],
            &serde_json::json!({"v": 1}),
        );
        assert!(!done);
        assert_eq!(state.step_index, 1);
        assert_eq!(state.step_count, 0);
        assert_eq!(state.events.len(), 1);

        // Event matches step 1
        let done = advance_state(
            &steps,
            &mut state,
            &[false, true],
            &serde_json::json!({"v": 2}),
        );
        assert!(done);
        assert_eq!(state.events.len(), 2);
    }

    #[test]
    fn test_advance_at_least() {
        // Pattern: A(3+) then B
        let steps = make_steps(&[("a", "true", "3+"), ("b", "true", "1")]);
        let mut state = PatternState {
            step_index: 0,
            step_count: 0,
            match_start: None,
            events: vec![],
        };

        // 3 events match step 0 only
        for i in 0..3 {
            let done = advance_state(
                &steps,
                &mut state,
                &[true, false],
                &serde_json::json!({"v": i}),
            );
            assert!(!done);
        }
        assert_eq!(state.step_index, 0);
        assert_eq!(state.step_count, 3);

        // 4th event matches both step 0 AND step 1 — greedy lookahead advances
        let done = advance_state(
            &steps,
            &mut state,
            &[true, true],
            &serde_json::json!({"v": 3}),
        );
        assert!(done);
    }

    #[test]
    fn test_advance_at_least_stays() {
        // Pattern: A(3+) then B — keep accumulating when only A matches
        let steps = make_steps(&[("a", "true", "3+"), ("b", "true", "1")]);
        let mut state = PatternState {
            step_index: 0,
            step_count: 0,
            match_start: None,
            events: vec![],
        };

        // 5 events all match only step 0
        for i in 0..5 {
            let done = advance_state(
                &steps,
                &mut state,
                &[true, false],
                &serde_json::json!({"v": i}),
            );
            assert!(!done);
        }
        assert_eq!(state.step_index, 0);
        assert_eq!(state.step_count, 5); // greedy — keeps accumulating
    }

    #[test]
    fn test_advance_optional_skip() {
        // Pattern: A, B?, C — B doesn't match, skip to C
        let steps = make_steps(&[("a", "true", "1"), ("b", "true", "1?"), ("c", "true", "1")]);
        let mut state = PatternState {
            step_index: 0,
            step_count: 0,
            match_start: None,
            events: vec![],
        };

        // Match A
        advance_state(
            &steps,
            &mut state,
            &[true, false, false],
            &serde_json::json!({"v": 1}),
        );
        assert_eq!(state.step_index, 1);

        // Event matches C but NOT B — should skip B and match C
        let done = advance_state(
            &steps,
            &mut state,
            &[false, false, true],
            &serde_json::json!({"v": 2}),
        );
        assert!(done);
        assert_eq!(state.events.len(), 2);
    }

    #[test]
    fn test_advance_optional_match() {
        // Pattern: A, B?, C — B actually matches
        let steps = make_steps(&[("a", "true", "1"), ("b", "true", "1?"), ("c", "true", "1")]);
        let mut state = PatternState {
            step_index: 0,
            step_count: 0,
            match_start: None,
            events: vec![],
        };

        // Match A
        advance_state(
            &steps,
            &mut state,
            &[true, false, false],
            &serde_json::json!({"v": 1}),
        );

        // Match B (optional satisfied)
        advance_state(
            &steps,
            &mut state,
            &[false, true, false],
            &serde_json::json!({"v": 2}),
        );
        assert_eq!(state.step_index, 2); // advanced past B to C

        // Match C
        let done = advance_state(
            &steps,
            &mut state,
            &[false, false, true],
            &serde_json::json!({"v": 3}),
        );
        assert!(done);
        assert_eq!(state.events.len(), 3);
    }

    #[test]
    fn test_new_pattern_from_step0() {
        let steps = make_steps(&[("a", "true", "1"), ("b", "true", "1")]);
        let annotated = vec![AnnotatedEvent {
            pk: "user1".to_string(),
            event: serde_json::json!({"v": 1}),
            matches: vec![true, false],
            state: None,
        }];

        let (completed, active) = run_state_machine(&steps, annotated);
        assert!(completed.is_empty());
        assert!(active.contains_key("user1"));
        assert_eq!(active["user1"].step_index, 1);
    }

    #[test]
    fn test_multiple_keys_same_batch() {
        let steps = make_steps(&[("a", "true", "1"), ("b", "true", "1")]);
        let annotated = vec![
            AnnotatedEvent {
                pk: "user1".to_string(),
                event: serde_json::json!({"v": "a1"}),
                matches: vec![true, false],
                state: None,
            },
            AnnotatedEvent {
                pk: "user2".to_string(),
                event: serde_json::json!({"v": "a2"}),
                matches: vec![true, false],
                state: None,
            },
        ];

        let (completed, active) = run_state_machine(&steps, annotated);
        assert!(completed.is_empty());
        assert!(active.contains_key("user1"));
        assert!(active.contains_key("user2"));
    }

    #[test]
    fn test_pattern_completes_and_restarts() {
        let steps = make_steps(&[("a", "true", "1"), ("b", "true", "1")]);
        let annotated = vec![
            // Existing state at step 1 for user1
            AnnotatedEvent {
                pk: "user1".to_string(),
                event: serde_json::json!({"v": "b1"}),
                matches: vec![false, true],
                state: Some(PatternState {
                    step_index: 1,
                    step_count: 0,
                    match_start: Some("2025-01-01T00:00:00Z".to_string()),
                    events: vec![serde_json::json!({"v": "a1"})],
                }),
            },
            // New event that matches step 0 for user1
            AnnotatedEvent {
                pk: "user1".to_string(),
                event: serde_json::json!({"v": "a2"}),
                matches: vec![true, false],
                state: None, // load SQL deduped — only first row has state
            },
        ];

        let (completed, active) = run_state_machine(&steps, annotated);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].key, "user1");
        // New pattern started
        assert!(active.contains_key("user1"));
        assert_eq!(active["user1"].step_index, 1);
    }

    #[test]
    fn test_empty_batch() {
        let steps = make_steps(&[("a", "true", "1")]);
        let (completed, active) = run_state_machine(&steps, vec![]);
        assert!(completed.is_empty());
        assert!(active.is_empty());
    }
}
