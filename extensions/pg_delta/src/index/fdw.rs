//! Foreign Data Wrapper over the index path — a `CREATE FOREIGN TABLE`
//! façade that runs the same prune + read pipeline as `delta.fetch`, but with
//! real typed columns and WHERE-clause predicate pushdown.
//!
//! Pushdown is best-effort: recognized `Var op Const` quals are translated into
//! the shared filter-JSON model and used to prune files/row-groups. **Every**
//! qual is also left in the scan's plan quals so Postgres re-checks them on the
//! returned rows — so our approximate filtering can only over-return, never drop
//! a matching row. Mirrors the hand-rolled FDW in pg_xarray.

use crate::index::fetch_rows;
use pgrx::pg_guard;
use pgrx::pg_sys;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::ffi::{CStr, CString};
use std::os::raw::c_int;

/// Hard cap on rows materialized per scan (the whole result set is buffered).
/// A warning is logged if hit so truncation is never silent.
const FDW_ROW_CAP: usize = 5_000_000;

// =============================================================================
// Handler + validator — C-ABI entry points
// =============================================================================

#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn delta_fdw_handler(
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
    (*routine).ReScanForeignScan = Some(fdw_rescan_scan);
    (*routine).EndForeignScan = Some(fdw_end_scan);
    pg_sys::Datum::from(routine as usize)
}

#[no_mangle]
pub extern "C" fn pg_finfo_delta_fdw_handler() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn delta_fdw_validator(_fcinfo: pg_sys::FunctionCallInfo) {}

#[no_mangle]
pub extern "C" fn pg_finfo_delta_fdw_validator() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

// =============================================================================
// Plan-time payload + per-scan state
// =============================================================================

/// Shipped from `GetForeignPlan` to `BeginForeignScan` via `fdw_private`.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct Payload {
    /// Filter JSON built from pushed-down quals (`{"col": {"op": v}}`).
    filter: Value,
}

struct ScanState {
    table_name: String,
    filter: Value,
    columns: Vec<String>,
    rows: Option<Vec<Value>>,
    current: usize,
}

// =============================================================================
// Planner callbacks
// =============================================================================

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
) {
    // Estimate rows = Σ num_records over files surviving tier-1 (best effort).
    let rows = match read_table_option(foreigntableid) {
        Some(name) => crate::index::estimate_rows(&name, &Value::Object(Map::new()))
            .unwrap_or(1000.0)
            .max(1.0),
        None => 1000.0,
    };
    (*baserel).rows = rows;
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    let path = make_foreignscan_path(root, baserel, (*baserel).rows);
    pg_sys::add_path(baserel, path as *mut pg_sys::Path);
}

/// PG version-stable `create_foreignscan_path` (pg18 inserted a disabled-node
/// count between rows and startup_cost).
unsafe fn make_foreignscan_path(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    rows: f64,
) -> *mut pg_sys::ForeignPath {
    #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
    {
        pg_sys::create_foreignscan_path(
            root,
            baserel,
            std::ptr::null_mut(),
            rows,
            pg_sys::Cost::from(10u32),
            pg_sys::Cost::from(100u32),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    }
    #[cfg(feature = "pg18")]
    {
        pg_sys::create_foreignscan_path(
            root,
            baserel,
            std::ptr::null_mut(),
            rows,
            0,
            pg_sys::Cost::from(10u32),
            pg_sys::Cost::from(100u32),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_plan(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    foreigntableid: pg_sys::Oid,
    _best_path: *mut pg_sys::ForeignPath,
    tlist: *mut pg_sys::List,
    scan_clauses: *mut pg_sys::List,
    outer_plan: *mut pg_sys::Plan,
) -> *mut pg_sys::ForeignScan {
    let scan_clauses = pg_sys::extract_actual_clauses(scan_clauses, false);
    let filter = classify_clauses(scan_clauses, foreigntableid);
    let fdw_private = serialize_payload(&Payload { filter });

    pg_sys::make_foreignscan(
        tlist,
        scan_clauses, // keep all quals for PG to re-check (correctness)
        (*baserel).relid,
        std::ptr::null_mut(), // no fdw_exprs (no join-param pushdown in v1)
        fdw_private,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        outer_plan,
    )
}

/// Walk `scan_clauses`, fold every recognized `Var op Const` into the filter.
unsafe fn classify_clauses(scan_clauses: *mut pg_sys::List, table_oid: pg_sys::Oid) -> Value {
    let mut filter: Map<String, Value> = Map::new();
    if scan_clauses.is_null() {
        return Value::Object(filter);
    }
    let n = (*scan_clauses).length as isize;
    let elements = (*scan_clauses).elements;
    for i in 0..n {
        let node = (*elements.offset(i)).ptr_value as *mut pg_sys::Node;
        if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_OpExpr {
            continue;
        }
        let op_expr = node as *mut pg_sys::OpExpr;
        let args = (*op_expr).args;
        if args.is_null() || (*args).length != 2 {
            continue;
        }
        let arg0 = (*(*args).elements.offset(0)).ptr_value as *mut pg_sys::Node;
        let arg1 = (*(*args).elements.offset(1)).ptr_value as *mut pg_sys::Node;
        let raw_op = op_name(op_expr);

        // Var op Const, or Const op Var (operator flipped).
        if let Some((col, value)) = read_var_const(arg0, arg1, table_oid) {
            if let Some(op) = op_key(&raw_op) {
                add_pred(&mut filter, &col, op, value);
            }
        } else if let Some((col, value)) = read_var_const(arg1, arg0, table_oid) {
            if let Some(op) = op_key(&flip_op(&raw_op)) {
                add_pred(&mut filter, &col, op, value);
            }
        }
    }
    Value::Object(filter)
}

fn add_pred(filter: &mut Map<String, Value>, col: &str, op: &str, value: Value) {
    let entry = filter
        .entry(col.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(obj) = entry {
        obj.insert(op.to_string(), value);
    }
}

unsafe fn op_name(op_expr: *mut pg_sys::OpExpr) -> String {
    let name_ptr = pg_sys::get_opname((*op_expr).opno);
    if name_ptr.is_null() {
        return String::new();
    }
    CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
}

fn op_key(name: &str) -> Option<&'static str> {
    match name {
        "=" => Some("eq"),
        "<" => Some("lt"),
        "<=" => Some("lte"),
        ">" => Some("gt"),
        ">=" => Some("gte"),
        _ => None,
    }
}

fn flip_op(op: &str) -> String {
    match op {
        "<" => ">",
        "<=" => ">=",
        ">" => "<",
        ">=" => "<=",
        other => other,
    }
    .to_string()
}

/// If `lhs` is a Var on our foreign table and `rhs` is a Const, return
/// (column name, value as JSON).
unsafe fn read_var_const(
    lhs: *mut pg_sys::Node,
    rhs: *mut pg_sys::Node,
    relid: pg_sys::Oid,
) -> Option<(String, Value)> {
    if (*lhs).type_ != pg_sys::NodeTag::T_Var || (*rhs).type_ != pg_sys::NodeTag::T_Const {
        return None;
    }
    let var = lhs as *mut pg_sys::Var;
    let attno = (*var).varattno;
    if attno < 1 {
        return None;
    }
    let name_ptr = pg_sys::get_attname(relid, attno, false);
    if name_ptr.is_null() {
        return None;
    }
    let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
    let value = const_to_json(rhs as *mut pg_sys::Const)?;
    Some((name, value))
}

/// Convert a `Const` to a JSON scalar matching the reader's value formatting
/// (so per-row comparison is exact). Returns `None` for types we don't push
/// down — the qual still gets re-checked by Postgres.
unsafe fn const_to_json(c: *mut pg_sys::Const) -> Option<Value> {
    if (*c).constisnull {
        return None;
    }
    let oid = (*c).consttype;
    let datum = (*c).constvalue;

    use pgrx::datum::FromDatum;
    match oid {
        t if t == pg_sys::BOOLOID => Some(Value::Bool(datum.value() != 0)),
        t if t == pg_sys::INT2OID => Some(Value::from(i16::from_datum(datum, false)?)),
        t if t == pg_sys::INT4OID => Some(Value::from(i32::from_datum(datum, false)?)),
        t if t == pg_sys::INT8OID => Some(Value::from(i64::from_datum(datum, false)?)),
        t if t == pg_sys::FLOAT4OID => Some(json_num(f32::from_datum(datum, false)? as f64)),
        t if t == pg_sys::FLOAT8OID => Some(json_num(f64::from_datum(datum, false)?)),
        t if t == pg_sys::NUMERICOID => {
            let n = pgrx::AnyNumeric::from_datum(datum, false)?;
            n.try_into().ok().map(json_num)
        }
        t if t == pg_sys::TEXTOID || t == pg_sys::VARCHAROID || t == pg_sys::BPCHAROID => {
            String::from_datum(datum, false).map(Value::String)
        }
        t if t == pg_sys::TIMESTAMPTZOID || t == pg_sys::TIMESTAMPOID => {
            // Datum is i64 microseconds since the PG epoch (2000-01-01).
            const PG_TO_UNIX_MICROS: i64 = 946_684_800 * 1_000_000;
            Some(micros_to_rfc3339(datum.value() as i64 + PG_TO_UNIX_MICROS))
        }
        t if t == pg_sys::DATEOID => {
            // Datum is i32 days since 2000-01-01.
            let days = datum.value() as i32;
            let base = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)?;
            let date = base.checked_add_signed(chrono::Duration::days(days as i64))?;
            Some(Value::String(date.format("%Y-%m-%d").to_string()))
        }
        _ => None,
    }
}

fn json_num(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

/// Match the reader's RFC3339 (micros, UTC `Z`) timestamp formatting exactly.
fn micros_to_rfc3339(micros: i64) -> Value {
    let secs = micros.div_euclid(1_000_000);
    let nsecs = (micros.rem_euclid(1_000_000) * 1000) as u32;
    match chrono::DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        None => Value::Null,
    }
}

unsafe fn serialize_payload(payload: &Payload) -> *mut pg_sys::List {
    if payload
        .filter
        .as_object()
        .map(|o| o.is_empty())
        .unwrap_or(true)
    {
        return std::ptr::null_mut();
    }
    let json = serde_json::to_string(payload).unwrap_or_default();
    let cstring = CString::new(json).unwrap_or_default();
    let s = pg_sys::makeString(pg_sys::pstrdup(cstring.as_ptr()));
    pg_sys::list_make1_impl(
        pg_sys::NodeTag::T_List,
        pg_sys::ListCell {
            ptr_value: s as *mut std::ffi::c_void,
        },
    )
}

unsafe fn deserialize_payload(fdw_private: *mut pg_sys::List) -> Payload {
    if fdw_private.is_null() || (*fdw_private).length == 0 {
        return Payload::default();
    }
    let cell = (*fdw_private).elements;
    let node = (*cell).ptr_value as *mut pg_sys::Node;
    if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_String {
        return Payload::default();
    }
    let s = node as *mut pg_sys::String;
    let sval = (*s).sval;
    if sval.is_null() {
        return Payload::default();
    }
    match CStr::from_ptr(sval).to_str() {
        Ok(json) => serde_json::from_str(json).unwrap_or_default(),
        Err(_) => Payload::default(),
    }
}

// =============================================================================
// Executor callbacks
// =============================================================================

#[pg_guard]
unsafe extern "C-unwind" fn fdw_begin_scan(node: *mut pg_sys::ForeignScanState, _eflags: c_int) {
    let scan_rel = (*node).ss.ss_currentRelation;
    let relid = (*scan_rel).rd_id;

    let table_name = match read_table_option(relid) {
        Some(n) => n,
        None => pgrx::error!("pg_delta FDW: OPTIONS missing 'delta_table'"),
    };

    let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
    let payload = if plan.is_null() {
        Payload::default()
    } else {
        deserialize_payload((*plan).fdw_private)
    };

    // Column names in tuple-descriptor order — what the slot must be filled with.
    let tupdesc = (*scan_rel).rd_att;
    let natts = (*tupdesc).natts as usize;
    let mut columns = Vec::with_capacity(natts);
    for i in 0..natts {
        let attno = (i + 1) as i16;
        let name_ptr = pg_sys::get_attname(relid, attno, true);
        if name_ptr.is_null() {
            columns.push(String::new()); // dropped column placeholder
        } else {
            columns.push(CStr::from_ptr(name_ptr).to_string_lossy().into_owned());
        }
    }

    let state = Box::new(ScanState {
        table_name,
        filter: payload.filter,
        columns,
        rows: None,
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

    if let Some(clear) = (*(*slot).tts_ops).clear {
        clear(slot);
    }

    if state.rows.is_none() {
        let rows = match fetch_rows(
            &state.table_name,
            &state.filter,
            Some(&state.columns),
            FDW_ROW_CAP,
        ) {
            Ok(r) => r,
            Err(e) => pgrx::error!("pg_delta FDW: {}", e),
        };
        if rows.len() >= FDW_ROW_CAP {
            pgrx::warning!(
                "pg_delta FDW: result truncated at {} rows for '{}'",
                FDW_ROW_CAP,
                state.table_name
            );
        }
        state.rows = Some(rows);
        state.current = 0;
    }

    let rows = state.rows.as_ref().unwrap();
    if state.current >= rows.len() {
        return slot; // empty slot ends the scan
    }
    let row = &rows[state.current];
    state.current += 1;

    let scan_rel = (*node).ss.ss_currentRelation;
    let relid = (*scan_rel).rd_id;
    let tupdesc = (*slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    for i in 0..natts {
        let isnull_slot = (*slot).tts_isnull.add(i);
        let value_slot = (*slot).tts_values.add(i);
        let attno = (i + 1) as i16;
        let type_oid = pg_sys::get_atttype(relid, attno);
        let json_val = state.columns.get(i).and_then(|c| row.get(c.as_str()));
        write_value(value_slot, isnull_slot, json_val, type_oid);
    }

    pg_sys::ExecStoreVirtualTuple(slot);
    slot
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_rescan_scan(node: *mut pg_sys::ForeignScanState) {
    let state_ptr = (*node).fdw_state as *mut ScanState;
    if state_ptr.is_null() {
        return;
    }
    let state = &mut *state_ptr;
    state.rows = None;
    state.current = 0;
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_end_scan(node: *mut pg_sys::ForeignScanState) {
    let state_ptr = (*node).fdw_state as *mut ScanState;
    if !state_ptr.is_null() {
        drop(Box::from_raw(state_ptr));
        (*node).fdw_state = std::ptr::null_mut();
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Write a JSON value into a slot column, coercing to the declared type via its
/// input function (universal path). Missing/null → SQL NULL.
unsafe fn write_value(
    value_slot: *mut pg_sys::Datum,
    isnull_slot: *mut bool,
    json: Option<&Value>,
    type_oid: pg_sys::Oid,
) {
    let text = match json {
        None | Some(Value::Null) => {
            *isnull_slot = true;
            return;
        }
        Some(Value::String(s)) => s.clone(),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        Some(other) => other.to_string(), // object/array → JSON text (for jsonb cols)
    };

    let cstr = match CString::new(text) {
        Ok(c) => c,
        Err(_) => {
            *isnull_slot = true;
            return;
        }
    };
    let mut typinput = pg_sys::InvalidOid;
    let mut typioparam = pg_sys::InvalidOid;
    pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
    *value_slot = pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, -1);
    *isnull_slot = false;
}

/// Read the `delta_table` OPTION from a foreign table's catalog row.
unsafe fn read_table_option(relid: pg_sys::Oid) -> Option<String> {
    let ft = pg_sys::GetForeignTable(relid);
    if ft.is_null() {
        return None;
    }
    let list = (*ft).options;
    if list.is_null() {
        return None;
    }
    let n = (*list).length as isize;
    let elements = (*list).elements;
    for i in 0..n {
        let de = (*elements.offset(i)).ptr_value as *mut pg_sys::DefElem;
        if de.is_null() {
            continue;
        }
        let name_ptr = (*de).defname;
        if name_ptr.is_null() {
            continue;
        }
        let name = match CStr::from_ptr(name_ptr).to_str() {
            Ok(s) => s,
            Err(_) => continue,
        };
        if name == "delta_table" {
            let arg = (*de).arg;
            if !arg.is_null() && (*arg).type_ == pg_sys::NodeTag::T_String {
                let s = arg as *mut pg_sys::String;
                if !(*s).sval.is_null() {
                    return CStr::from_ptr((*s).sval).to_str().ok().map(String::from);
                }
            }
        }
    }
    None
}

// =============================================================================
// FDW SQL objects
// =============================================================================

pgrx::extension_sql!(
    r#"
CREATE FUNCTION delta.fdw_handler() RETURNS fdw_handler
    AS '$libdir/pg_delta', 'delta_fdw_handler' LANGUAGE C STRICT;

CREATE FUNCTION delta.fdw_validator(text[], oid) RETURNS void
    AS '$libdir/pg_delta', 'delta_fdw_validator' LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pg_delta_fdw
    HANDLER delta.fdw_handler
    VALIDATOR delta.fdw_validator;

CREATE SERVER pg_delta_server FOREIGN DATA WRAPPER pg_delta_fdw;
"#,
    name = "fdw_bootstrap",
    requires = ["index_catalog"]
);
