//! Foreign Data Wrapper for `pgx` datasets — a `CREATE FOREIGN TABLE`
//! façade over the same chunk-lookup + reader-dispatch that `pgx.fetch`
//! uses.
//!
//! Phase 3a: skeleton — `SELECT * FROM wx_t2m` returns the full
//! per-cell row set for `(dataset='era5', variable='t2m')`.
//!
//! Phase 3b (this commit): in `GetForeignPlan`, walk `scan_clauses`,
//! pick out `lat`/`lon`/`level`/`time` Var-vs-Const comparisons, and
//! stash the extracted bounds as a JSON string on `fdw_private`. The
//! executor reads them in `BeginForeignScan`, builds a bbox WKT and a
//! time / level predicate, and calls `fetch_impl` with those so the
//! catalog's GIST + range indexes prune before any bytes are read.
//! WHERE clauses that don't fit the pushdown grammar (e.g.
//! `value > 5`) are left for PG to apply after the scan, same as
//! before.

use crate::srf::fetch::fetch_impl;
use pgrx::pg_guard;
use pgrx::pg_sys;
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};
use std::os::raw::c_int;

// =============================================================================
// FDW handler + validator — C-ABI entry points
// =============================================================================

/// FDW handler — returns the populated `FdwRoutine` Postgres uses to
/// drive scans of foreign tables backed by this wrapper.
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn pgx_fdw_handler_wrapper(
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
pub extern "C" fn pg_finfo_pgx_fdw_handler_wrapper() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

/// Validator — called at `CREATE FOREIGN TABLE` time to vet OPTIONS.
/// Deferred to scan time today; the OPTIONS API at runtime is safer
/// than parsing `fcinfo` args here.
#[no_mangle]
#[pg_guard]
pub unsafe extern "C-unwind" fn pgx_fdw_validator_wrapper(_fcinfo: pg_sys::FunctionCallInfo) {}

#[no_mangle]
pub extern "C" fn pg_finfo_pgx_fdw_validator_wrapper() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

// =============================================================================
// Per-scan state
// =============================================================================

struct ScanState {
    /// Dataset / variable from FDW OPTIONS — looked up once in
    /// BeginScan, reused on every (re-)scan.
    dataset: String,
    variable: String,
    /// Static (Var-vs-Const) predicates extracted at plan time — same
    /// across every rescan.
    static_preds: ScanPredicates,
    /// Parameter-shaped predicates — one entry per `fdw_exprs` slot.
    /// Re-evaluated at every rescan against the outer row.
    param_specs: Vec<ParamSpec>,
    /// `ExprState`s built from `(*plan).fdw_exprs` at BeginScan, one
    /// per `param_specs` entry (parallel arrays).
    expr_states: Vec<*mut pg_sys::ExprState>,
    /// Materialised cells from the most recent `fetch_impl`. `None`
    /// until first iterate; cleared by ReScan so the next iterate
    /// re-evaluates parameters and re-fetches.
    rows: Option<Vec<FetchRow>>,
    /// Index of the next row to emit out of `rows`.
    current: usize,
}

/// Same shape as `srf::fetch::FetchRow` — re-spelled here so we don't
/// import the private alias.
type FetchRow = (
    Option<f64>,                                // lat
    Option<f64>,                                // lon
    Option<f64>,                                // level
    Option<pgrx::datum::TimestampWithTimeZone>, // time
    f64,                                        // value
);

/// Predicates extracted from `scan_clauses` in `GetForeignPlan` and
/// stashed on `fdw_private` for `BeginForeignScan` to consume. All
/// fields optional — set only when the planner sees a matching
/// Var-op-Const clause.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct ScanPredicates {
    /// `lat >= x` (or `lat = x` → both min and max set)
    lat_min: Option<f64>,
    lat_max: Option<f64>,
    lon_min: Option<f64>,
    lon_max: Option<f64>,
    level_min: Option<f64>,
    level_max: Option<f64>,
    /// `time = x` — RFC 3339 string so we don't have to round-trip
    /// through pgrx's PG-epoch TimestampWithTimeZone. Used as an
    /// exact-point predicate (catalog: `time_range @> at_time`).
    at_time: Option<String>,
    /// `time >= x` / `time <= y` / `time BETWEEN x AND y` — half-open
    /// or closed range. Catalog: `time_range && tstzrange(min,max,'[]')`.
    /// Either bound may be NULL.
    time_min: Option<String>,
    time_max: Option<String>,
}

impl ScanPredicates {
    fn is_empty(&self) -> bool {
        self.lat_min.is_none()
            && self.lat_max.is_none()
            && self.lon_min.is_none()
            && self.lon_max.is_none()
            && self.level_min.is_none()
            && self.level_max.is_none()
            && self.at_time.is_none()
            && self.time_min.is_none()
            && self.time_max.is_none()
    }

    /// Build a bbox WKT if both lat and lon ranges are fully bounded.
    /// Open ranges (e.g. only `lat >= 50`) are not enough — PostGIS
    /// `&&` needs a closed envelope. Returns None otherwise.
    fn bbox_wkt(&self) -> Option<String> {
        let (lat_min, lat_max) = match (self.lat_min, self.lat_max) {
            (Some(a), Some(b)) => (a.min(b), a.max(b)),
            _ => return None,
        };
        let (lon_min, lon_max) = match (self.lon_min, self.lon_max) {
            (Some(a), Some(b)) => (a.min(b), a.max(b)),
            _ => return None,
        };
        Some(format!(
            "POLYGON(({lon_min} {lat_min}, {lon_max} {lat_min}, \
             {lon_max} {lat_max}, {lon_min} {lat_max}, {lon_min} {lat_min}))"
        ))
    }
}

/// A parameter-shaped predicate: at plan time we recorded the axis
/// + comparison op + which slot of `fdw_exprs` carries the RHS Expr.
///
/// At scan time we evaluate `fdw_exprs[expr_index]` against the
/// current outer row, then fold the result into a fresh
/// `ScanPredicates` (merged with the static ones) before fetching.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParamSpec {
    axis: Axis,
    op: CmpOp,
    expr_index: usize,
}

/// Map a foreign-table column name (case-insensitive) to a semantic
/// axis we know how to push down on. Returns None for columns we
/// don't recognise (e.g. `value`).
fn axis_for_column(name: &str) -> Option<Axis> {
    match name.to_ascii_lowercase().as_str() {
        "lat" | "latitude" => Some(Axis::Lat),
        "lon" | "longitude" => Some(Axis::Lon),
        "level" | "altitude" | "depth" => Some(Axis::Level),
        "time" => Some(Axis::Time),
        _ => None,
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum Axis {
    Lat,
    Lon,
    Level,
    Time,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

fn cmp_op_from_name(name: &str) -> Option<CmpOp> {
    match name {
        "=" => Some(CmpOp::Eq),
        "<" => Some(CmpOp::Lt),
        "<=" => Some(CmpOp::Le),
        ">" => Some(CmpOp::Gt),
        ">=" => Some(CmpOp::Ge),
        _ => None,
    }
}

// =============================================================================
// Planner callbacks
// =============================================================================

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_rel_size(
    _root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    // Placeholder row estimate. Phase 3b will refine this with
    // catalog-driven chunk counts once predicates are pushed down.
    (*baserel).rows = 1000.0;
}

#[pg_guard]
unsafe extern "C-unwind" fn fdw_get_paths(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    _foreigntableid: pg_sys::Oid,
) {
    // 1) Plain (non-parameterised) path — always present so the planner
    //    can pick it when no useful outer rels exist.
    let plain = make_foreignscan_path(root, baserel, std::ptr::null_mut(), (*baserel).rows);
    pg_sys::add_path(baserel, plain as *mut pg_sys::Path);

    // 2) Parameterised path — when our baserel sits inside a join
    //    whose clauses constrain our axis columns against the outer rel.
    //    The planner will pick this path when the FDW is the inner side
    //    of a nested-loop join, binding the join clauses as runtime
    //    parameters that we evaluate in (Re)ScanForeignScan.
    // PG's planner often drops equality join clauses (lat = s.lat,
    // lon = s.lon) from `baserel->joininfo` into equivalence classes,
    // so walking joininfo alone misses the common case. Instead,
    // produce a parameterized path whenever there's ANY other baserel
    // in the query — let the planner decide whether the path is useful
    // via standard cost comparison. `scan_clauses` at GetForeignPlan
    // time will carry the actually-applicable join clauses (including
    // the ones materialised from equivalence classes).
    let all_baserels = (*root).all_baserels;
    let outer_relids = if !all_baserels.is_null() {
        pg_sys::bms_difference(all_baserels, (*baserel).relids)
    } else {
        std::ptr::null_mut()
    };
    if !outer_relids.is_null() && pg_sys::bms_num_members(outer_relids) > 0 {
        // Parameterised path: per-loop the FDW returns a small slab
        // (the catalog GIST/range indexes prune via the runtime params)
        // — advertise that as a tiny per-call row count and a near-zero
        // cost so the planner prefers nested-loop-with-FDW-inner over
        // alternatives like hash/merge join (which would read the
        // whole foreign table once). The optimiser scales by outer
        // cardinality automatically.
        let param = make_foreignscan_path_with_cost(
            root,
            baserel,
            outer_relids,
            1.0, /* rows per call */
            1,
            5, /* startup/total cost */
        );
        pg_sys::add_path(baserel, param as *mut pg_sys::Path);
    }
}

/// PG version-stable wrapper around `create_foreignscan_path` — pg18
/// inserted an extra disabled-node count between rows and startup_cost.
unsafe fn make_foreignscan_path(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    required_outer: *mut pg_sys::Bitmapset,
    rows: f64,
) -> *mut pg_sys::ForeignPath {
    make_foreignscan_path_with_cost(root, baserel, required_outer, rows, 10, 100)
}

/// Same as `make_foreignscan_path` but lets the caller dial the
/// startup/total cost — used by the parameterised path to advertise a
/// near-zero per-call cost so nested-loop wins over merge/hash for the
/// small-outer-cardinality case.
unsafe fn make_foreignscan_path_with_cost(
    root: *mut pg_sys::PlannerInfo,
    baserel: *mut pg_sys::RelOptInfo,
    required_outer: *mut pg_sys::Bitmapset,
    rows: f64,
    startup_cost: u32,
    total_cost: u32,
) -> *mut pg_sys::ForeignPath {
    #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
    {
        pg_sys::create_foreignscan_path(
            root,
            baserel,
            std::ptr::null_mut(),
            rows,
            pg_sys::Cost::from(startup_cost),
            pg_sys::Cost::from(total_cost),
            std::ptr::null_mut(),
            required_outer,
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
            pg_sys::Cost::from(startup_cost),
            pg_sys::Cost::from(total_cost),
            std::ptr::null_mut(),
            required_outer,
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
    let our_relid = (*baserel).relid;
    let (payload, fdw_exprs) = classify_clauses(scan_clauses, foreigntableid, our_relid);
    let fdw_private = serialize_payload(&payload);
    pg_sys::make_foreignscan(
        tlist,
        scan_clauses,
        our_relid,
        fdw_exprs,
        fdw_private,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        outer_plan,
    )
}

/// Walk `scan_clauses` and split each `OpExpr(=|<|<=|>=|>, ...)` into
/// either:
///   * Var-vs-Const on an axis column → fold into `ScanPredicates`
///     (the existing static-pushdown path).
///   * Var-on-our-rel vs. any non-Const Expr referencing OUTER rels →
///     emit a `ParamSpec` and append the outer Expr to `fdw_exprs`.
///     At scan time we `ExecInitExpr` each entry and re-evaluate it on
///     every (Re)ScanForeignScan call to rebuild the predicates.
///
/// Clauses that don't fit either pattern stay in `scan_clauses` and are
/// applied by PG above the scan.
unsafe fn classify_clauses(
    scan_clauses: *mut pg_sys::List,
    table_oid: pg_sys::Oid,
    our_relid: pg_sys::Index,
) -> (FdwPlanPayload, *mut pg_sys::List) {
    let mut payload = FdwPlanPayload::default();
    let mut fdw_exprs: *mut pg_sys::List = std::ptr::null_mut();
    if scan_clauses.is_null() {
        return (payload, fdw_exprs);
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

        // Path A: Var-vs-Const (existing).
        if let Some((axis, value)) = read_var_const(arg0, arg1, table_oid) {
            if let Some(op) = cmp_op_from_name(&raw_op) {
                apply_predicate(&mut payload.static_preds, axis, op, value);
                continue;
            }
        }
        if let Some((axis, value)) = read_var_const(arg1, arg0, table_oid) {
            if let Some(op) = cmp_op_from_name(&flip_op_name(raw_op.clone())) {
                apply_predicate(&mut payload.static_preds, axis, op, value);
                continue;
            }
        }

        // Path B: Var-on-our-rel vs. outer Expr (parameterised).
        // axis side is the side that IS a Var on our relid pointing at
        // an axis column. param side is everything else.
        let (axis_opt, param_expr, op_str) =
            if let Some(axis) = our_axis_var(arg0, our_relid, table_oid) {
                (Some(axis), arg1, raw_op.clone())
            } else if let Some(axis) = our_axis_var(arg1, our_relid, table_oid) {
                (Some(axis), arg0, flip_op_name(raw_op.clone()))
            } else {
                (None, std::ptr::null_mut(), String::new())
            };
        let Some(axis) = axis_opt else { continue };
        let Some(op) = cmp_op_from_name(&op_str) else {
            continue;
        };
        if param_expr.is_null() || (*param_expr).type_ == pg_sys::NodeTag::T_Const {
            continue; // Path A already handled this
        }
        let expr_index = if fdw_exprs.is_null() {
            0
        } else {
            (*fdw_exprs).length as usize
        };
        fdw_exprs = pg_sys::lappend(fdw_exprs, param_expr as *mut std::ffi::c_void);
        payload.param_specs.push(ParamSpec {
            axis,
            op,
            expr_index,
        });
    }
    (payload, fdw_exprs)
}

/// Like `read_var_const` but for the "is THIS Var on our rel + an axis
/// column" check used by the parameter-classification path. Returns
/// the axis when the node is a `Var` pointing at one of our pushable
/// columns; None otherwise.
unsafe fn our_axis_var(
    node: *mut pg_sys::Node,
    our_relid: pg_sys::Index,
    table_oid: pg_sys::Oid,
) -> Option<Axis> {
    if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    let var = node as *mut pg_sys::Var;
    if (*var).varno != our_relid as i32 {
        return None;
    }
    let attno = (*var).varattno;
    if attno < 1 {
        return None;
    }
    let name_ptr = pg_sys::get_attname(table_oid, attno, false);
    if name_ptr.is_null() {
        return None;
    }
    let name = CStr::from_ptr(name_ptr).to_string_lossy();
    axis_for_column(&name)
}

unsafe fn op_name(op_expr: *mut pg_sys::OpExpr) -> String {
    let name_ptr = pg_sys::get_opname((*op_expr).opno);
    if name_ptr.is_null() {
        return String::new();
    }
    CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
}

fn flip_op_name(op: String) -> String {
    match op.as_str() {
        "<" => ">".to_string(),
        "<=" => ">=".to_string(),
        ">" => "<".to_string(),
        ">=" => "<=".to_string(),
        other => other.to_string(),
    }
}

/// If `lhs` is a Var on our foreign table and `rhs` is a Const, return
/// (axis, const-as-AxisValue). Otherwise None.
unsafe fn read_var_const(
    lhs: *mut pg_sys::Node,
    rhs: *mut pg_sys::Node,
    relid: pg_sys::Oid,
) -> Option<(Axis, AxisValue)> {
    if (*lhs).type_ != pg_sys::NodeTag::T_Var {
        return None;
    }
    if (*rhs).type_ != pg_sys::NodeTag::T_Const {
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
    let name = CStr::from_ptr(name_ptr).to_string_lossy();
    let axis = axis_for_column(&name)?;
    let value = read_const(rhs as *mut pg_sys::Const, axis)?;
    Some((axis, value))
}

enum AxisValue {
    Float(f64),
    Time(String), // RFC 3339
}

unsafe fn read_const(c: *mut pg_sys::Const, axis: Axis) -> Option<AxisValue> {
    if (*c).constisnull {
        return None;
    }
    let consttype = (*c).consttype;
    let datum = (*c).constvalue;
    match axis {
        Axis::Lat | Axis::Lon | Axis::Level => {
            // Accept float8; PG implicitly casts numeric/int literals
            // when compared against a float8 column, so this is the
            // common case.
            if consttype == pg_sys::FLOAT8OID {
                Some(AxisValue::Float(f64::from_bits(datum.value() as u64)))
            } else if consttype == pg_sys::FLOAT4OID {
                let bits = datum.value() as u32;
                Some(AxisValue::Float(f32::from_bits(bits) as f64))
            } else {
                None
            }
        }
        Axis::Time => {
            if consttype == pg_sys::TIMESTAMPTZOID {
                // Datum is i64 microseconds since PG epoch (2000-01-01).
                let pg_micros = datum.value() as i64;
                const PG_TO_UNIX_SECONDS: i64 = 946_684_800;
                let unix_micros = pg_micros + PG_TO_UNIX_SECONDS * 1_000_000;
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(unix_micros)?;
                Some(AxisValue::Time(dt.to_rfc3339()))
            } else {
                None
            }
        }
    }
}

fn apply_predicate(p: &mut ScanPredicates, axis: Axis, op: CmpOp, value: AxisValue) {
    match (axis, value) {
        (Axis::Lat, AxisValue::Float(v)) => apply_float_op(&mut p.lat_min, &mut p.lat_max, op, v),
        (Axis::Lon, AxisValue::Float(v)) => apply_float_op(&mut p.lon_min, &mut p.lon_max, op, v),
        (Axis::Level, AxisValue::Float(v)) => {
            apply_float_op(&mut p.level_min, &mut p.level_max, op, v)
        }
        (Axis::Time, AxisValue::Time(s)) => match op {
            CmpOp::Eq => p.at_time = Some(s),
            // `time >= x` (Gt and Ge both push the lower bound — the
            // `>` strict variant would lose the exact boundary, but
            // tstzrange's '[]' bound is conservative: it includes
            // chunks whose time_range touches the endpoint, never
            // misses one. Same for Lt and Le on the upper side.)
            CmpOp::Gt | CmpOp::Ge => p.time_min = Some(s),
            CmpOp::Lt | CmpOp::Le => p.time_max = Some(s),
        },
        _ => {}
    }
}

fn apply_float_op(min: &mut Option<f64>, max: &mut Option<f64>, op: CmpOp, v: f64) {
    match op {
        CmpOp::Eq => {
            *min = Some(v);
            *max = Some(v);
        }
        CmpOp::Lt | CmpOp::Le => {
            *max = Some(match *max {
                Some(cur) => cur.min(v),
                None => v,
            });
        }
        CmpOp::Gt | CmpOp::Ge => {
            *min = Some(match *min {
                Some(cur) => cur.max(v),
                None => v,
            });
        }
    }
}

/// Plan-time payload shipped from `GetForeignPlan` to `BeginForeignScan`
/// via `fdw_private`. Holds both static (Var-vs-Const) predicates and
/// the shape of any parameter-driven (Var-vs-outer-Expr) predicates;
/// the Exprs themselves live in the parallel `fdw_exprs` List on the
/// `ForeignScan` node.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct FdwPlanPayload {
    static_preds: ScanPredicates,
    param_specs: Vec<ParamSpec>,
}

/// Serialize the full plan payload as a single-element `List*`
/// containing a `T_String` Value node holding JSON. Same trick we used
/// before; the executor reads it back in BeginScan.
unsafe fn serialize_payload(payload: &FdwPlanPayload) -> *mut pg_sys::List {
    if payload.static_preds.is_empty() && payload.param_specs.is_empty() {
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

unsafe fn deserialize_payload(fdw_private: *mut pg_sys::List) -> FdwPlanPayload {
    if fdw_private.is_null() || (*fdw_private).length == 0 {
        return FdwPlanPayload::default();
    }
    let cell = (*fdw_private).elements;
    let node = (*cell).ptr_value as *mut pg_sys::Node;
    if node.is_null() || (*node).type_ != pg_sys::NodeTag::T_String {
        return FdwPlanPayload::default();
    }
    let s = node as *mut pg_sys::String;
    let sval = (*s).sval;
    if sval.is_null() {
        return FdwPlanPayload::default();
    }
    let json = match CStr::from_ptr(sval).to_str() {
        Ok(s) => s,
        Err(_) => return FdwPlanPayload::default(),
    };
    serde_json::from_str(json).unwrap_or_default()
}

// =============================================================================
// Executor callbacks
// =============================================================================

#[pg_guard]
unsafe extern "C-unwind" fn fdw_begin_scan(node: *mut pg_sys::ForeignScanState, _eflags: c_int) {
    let scan_rel = (*node).ss.ss_currentRelation;
    let relid = (*scan_rel).rd_id;

    let (dataset, variable) = match read_required_options(relid) {
        Ok(opts) => opts,
        Err(e) => pgrx::error!("pgx_fdw: {}", e),
    };

    let plan = (*node).ss.ps.plan as *mut pg_sys::ForeignScan;
    let payload = if plan.is_null() {
        FdwPlanPayload::default()
    } else {
        deserialize_payload((*plan).fdw_private)
    };

    // Build ExprStates from `(*plan).fdw_exprs` in parallel-array order
    // with payload.param_specs[i].expr_index. ExecInitExpr links each
    // expression to this PlanState so subsequent ExecEvalExpr calls
    // see the latest outer-row binding from the surrounding nested
    // loop.
    let mut expr_states: Vec<*mut pg_sys::ExprState> = Vec::new();
    if !plan.is_null() && !(*plan).fdw_exprs.is_null() {
        let exprs = (*plan).fdw_exprs;
        let n = (*exprs).length as isize;
        let elements = (*exprs).elements;
        let parent = &mut (*node).ss.ps as *mut pg_sys::PlanState;
        for i in 0..n {
            let expr = (*elements.offset(i)).ptr_value as *mut pg_sys::Expr;
            if expr.is_null() {
                expr_states.push(std::ptr::null_mut());
                continue;
            }
            let state = pg_sys::ExecInitExpr(expr, parent);
            expr_states.push(state);
        }
    }

    let state = Box::new(ScanState {
        dataset,
        variable,
        static_preds: payload.static_preds,
        param_specs: payload.param_specs,
        expr_states,
        rows: None,
        current: 0,
    });
    (*node).fdw_state = Box::into_raw(state) as *mut std::ffi::c_void;
}

/// Re-scan callback — fires once per outer-row change when this FDW
/// is the inner side of a nested-loop join. Clearing `rows` defers
/// the actual `fetch_impl` to the next `IterateForeignScan`, by which
/// point the surrounding executor has bound the outer row's columns
/// into the ExprContext used to evaluate our parameter Exprs.
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

/// Evaluate parameter ExprStates against the current ExprContext +
/// merge into a fresh `ScanPredicates`, then call `fetch_impl`.
/// Called from `IterateForeignScan` lazily on first row (or after
/// ReScan invalidated the cached result).
unsafe fn refresh_rows(node: *mut pg_sys::ForeignScanState, state: &mut ScanState) {
    let mut preds = state.static_preds.clone();
    let econtext = (*node).ss.ps.ps_ExprContext;
    for (i, spec) in state.param_specs.iter().enumerate() {
        let expr_state = match state.expr_states.get(i) {
            Some(&p) if !p.is_null() => p,
            _ => continue,
        };
        let evalfunc = match (*expr_state).evalfunc {
            Some(f) => f,
            None => continue,
        };
        let mut isnull: bool = false;
        let datum = evalfunc(expr_state, econtext, &mut isnull as *mut bool);
        if isnull {
            continue;
        }
        if let Some(value) = datum_to_axis_value(spec.axis, datum, expr_state) {
            apply_predicate(&mut preds, spec.axis, spec.op, value);
        }
    }

    let bbox_wkt = preds.bbox_wkt();
    let cast_time = |s: &str| -> Option<pgrx::datum::TimestampWithTimeZone> {
        let cast_sql = "SELECT $1::text::timestamptz";
        pgrx::Spi::get_one_with_args::<pgrx::datum::TimestampWithTimeZone>(cast_sql, &[s.into()])
            .ok()
            .flatten()
    };
    let at_time = preds.at_time.as_deref().and_then(cast_time);
    let time_from = preds.time_min.as_deref().and_then(cast_time);
    let time_to = preds.time_max.as_deref().and_then(cast_time);

    let rows = fetch_impl(
        &state.dataset,
        &state.variable,
        at_time,
        bbox_wkt.as_deref(),
        preds.level_min,
        preds.level_max,
        1_000_000,
        time_from,
        time_to,
    );
    state.rows = Some(rows);
    state.current = 0;
}

/// Pull a Datum that came out of `ExprState::evalfunc` and convert it
/// to an `AxisValue`. The expected runtime type depends on the axis —
/// float8 for lat/lon/level, timestamptz for time.
unsafe fn datum_to_axis_value(
    axis: Axis,
    datum: pg_sys::Datum,
    expr_state: *mut pg_sys::ExprState,
) -> Option<AxisValue> {
    let expr_oid = expr_type_oid(expr_state);
    match axis {
        Axis::Lat | Axis::Lon | Axis::Level => {
            if expr_oid == pg_sys::FLOAT8OID {
                Some(AxisValue::Float(f64::from_bits(datum.value() as u64)))
            } else if expr_oid == pg_sys::FLOAT4OID {
                let bits = datum.value() as u32;
                Some(AxisValue::Float(f32::from_bits(bits) as f64))
            } else {
                // Fall back via numeric → float8 SPI cast for ints /
                // numerics — covers the common s.lat as float4/float8
                // case for stations tables.
                let s = pg_sys::Datum::from(datum.value()).value().to_string();
                s.parse::<f64>().ok().map(AxisValue::Float)
            }
        }
        Axis::Time => {
            if expr_oid == pg_sys::TIMESTAMPTZOID {
                let pg_micros = datum.value() as i64;
                const PG_TO_UNIX_SECONDS: i64 = 946_684_800;
                let unix_micros = pg_micros + PG_TO_UNIX_SECONDS * 1_000_000;
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(unix_micros)?;
                Some(AxisValue::Time(dt.to_rfc3339()))
            } else {
                None
            }
        }
    }
}

/// Return the OID of the type a top-level expression produces. Walks
/// the Expr node tree at the head of `expr_state.expr`.
unsafe fn expr_type_oid(expr_state: *mut pg_sys::ExprState) -> pg_sys::Oid {
    if expr_state.is_null() {
        return pg_sys::InvalidOid;
    }
    let expr = (*expr_state).expr;
    if expr.is_null() {
        return pg_sys::InvalidOid;
    }
    pg_sys::exprType(expr as *const pg_sys::Node)
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
        refresh_rows(node, state);
    }

    let rows = state.rows.as_ref().unwrap();
    if state.current >= rows.len() {
        return slot; // empty slot = end of scan
    }

    let row = &rows[state.current];
    state.current += 1;

    let scan_rel = (*node).ss.ss_currentRelation;
    let relid = (*scan_rel).rd_id;

    let tupdesc = (*slot).tts_tupleDescriptor;
    let natts = (*tupdesc).natts as usize;

    // The foreign-table columns are user-declared. We match them by
    // *position*: (lat, lon, level, time, value) — same as
    // pgx.fetch's SRF row order. Users get an error from PG if the
    // declared column type doesn't match the value we insert.
    for i in 0..natts {
        let isnull_slot = (*slot).tts_isnull.add(i);
        let value_slot = (*slot).tts_values.add(i);
        let attno = (i + 1) as i16;
        let type_oid = pg_sys::get_atttype(relid, attno);
        match i {
            0 => write_f64(value_slot, isnull_slot, row.0, type_oid),
            1 => write_f64(value_slot, isnull_slot, row.1, type_oid),
            2 => write_f64(value_slot, isnull_slot, row.2, type_oid),
            3 => {
                if let Some(ts) = row.3 {
                    *value_slot = pgrx::datum::IntoDatum::into_datum(ts)
                        .unwrap_or(pg_sys::Datum::from(0_usize));
                    *isnull_slot = false;
                } else {
                    *isnull_slot = true;
                }
            }
            4 => {
                *value_slot = pgrx::datum::IntoDatum::into_datum(row.4)
                    .unwrap_or(pg_sys::Datum::from(0_usize));
                *isnull_slot = false;
            }
            _ => {
                // Extra user-declared columns we don't fill — return NULL.
                *isnull_slot = true;
            }
        }
    }

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

// =============================================================================
// OPTIONS reading
// =============================================================================

/// Read the required (`dataset`, `variable`) OPTIONS from the foreign
/// table's catalog row. Errors out cleanly when missing.
unsafe fn read_required_options(relid: pg_sys::Oid) -> Result<(String, String), String> {
    let ft = pg_sys::GetForeignTable(relid);
    if ft.is_null() {
        return Err(format!("foreign table relid {:?} not found", relid));
    }
    let options_list = (*ft).options;
    let mut dataset: Option<String> = None;
    let mut variable: Option<String> = None;
    iterate_defelem_list(options_list, |name, value| match name {
        "dataset" => dataset = Some(value.to_string()),
        "variable" => variable = Some(value.to_string()),
        _ => {} // unknown options ignored (server-level options pass through here too)
    });
    match (dataset, variable) {
        (Some(d), Some(v)) => Ok((d, v)),
        (None, _) => Err("OPTIONS missing 'dataset'".to_string()),
        (_, None) => Err("OPTIONS missing 'variable'".to_string()),
    }
}

/// Walk a `List*` of `DefElem*` (FDW options) and invoke `f(name,
/// value)` for each entry that has a string-valued payload.
unsafe fn iterate_defelem_list(list: *mut pg_sys::List, mut f: impl FnMut(&str, &str)) {
    if list.is_null() {
        return;
    }
    let n = (*list).length as isize;
    let elements = (*list).elements;
    for i in 0..n {
        let lc = elements.offset(i);
        let de = (*lc).ptr_value as *mut pg_sys::DefElem;
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
        let arg = (*de).arg;
        if arg.is_null() {
            continue;
        }
        let value = defelem_string_value(arg);
        if let Some(v) = value {
            f(name, &v);
        }
    }
}

unsafe fn defelem_string_value(node: *mut pg_sys::Node) -> Option<String> {
    if node.is_null() {
        return None;
    }
    // FDW OPTIONS strings are typically `T_String` Value nodes (pg <= 14)
    // or `T_String` Value-equivalents (pg >= 15 use `String`).
    let tag = (*node).type_;
    if tag == pg_sys::NodeTag::T_String {
        let s = node as *mut pg_sys::String;
        let sval = (*s).sval;
        if sval.is_null() {
            return None;
        }
        return CStr::from_ptr(sval).to_str().ok().map(String::from);
    }
    None
}

// =============================================================================
// Slot-writing helpers
// =============================================================================

unsafe fn write_f64(
    value_slot: *mut pg_sys::Datum,
    isnull_slot: *mut bool,
    value: Option<f64>,
    type_oid: pg_sys::Oid,
) {
    match value {
        Some(v) => {
            // float8 columns: store directly as Datum. For other declared
            // column types (e.g. numeric) we round-trip via the input
            // function so PG's coercion handles it.
            if type_oid == pg_sys::FLOAT8OID {
                *value_slot =
                    pgrx::datum::IntoDatum::into_datum(v).unwrap_or(pg_sys::Datum::from(0_usize));
            } else {
                let s = v.to_string();
                let cstr = std::ffi::CString::new(s).unwrap_or_default();
                let mut typinput = pg_sys::InvalidOid;
                let mut typioparam = pg_sys::InvalidOid;
                pg_sys::getTypeInputInfo(type_oid, &mut typinput, &mut typioparam);
                *value_slot =
                    pg_sys::OidInputFunctionCall(typinput, cstr.as_ptr() as *mut _, typioparam, -1);
            }
            *isnull_slot = false;
        }
        None => {
            *isnull_slot = true;
        }
    }
}
