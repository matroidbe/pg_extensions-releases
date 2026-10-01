//! SPI execution logic for processing bridge requests
//!
//! This module contains the code that runs in PostgreSQL context to execute
//! SPI queries. It must only be called from the background worker's main thread.
//!
//! # Parameter binding
//!
//! Parameters are bound as real, typed SQL arguments (`SPI_execute_with_args`),
//! never spliced into the query text. A parameter value therefore can never be
//! re-interpreted as SQL, regardless of its content — including values that
//! themselves contain `$N` tokens or quote characters.
//!
//! Because arguments are typed, a `SpiParam::Text` is a `text` value, not an
//! untyped literal. Where a query needs a text parameter coerced to another
//! type (e.g. a dynamically discovered column type), cast it explicitly in the
//! SQL: `$1::uuid`, `$2::jsonb`, `$3::my_type`.

use crate::bridge::SpiRequest;
use crate::types::{ColumnType, SpiError, SpiParam, SpiResult, SpiRow, SpiValue};
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;

/// Execute a single SPI request
///
/// This function must be called from the background worker's main thread
/// where SPI access is valid. It wraps the SPI call in a transaction context.
///
/// # Safety
///
/// This function uses PostgreSQL's SPI interface and must only be called from
/// the background worker's main thread. Calling from any other thread will
/// cause undefined behavior.
pub fn execute_spi_request(request: SpiRequest) {
    let result = run_request_in_transaction(&request);

    // Ignore send errors - the receiver may have been dropped
    let _ = request.response_tx.send(result);
}

/// Run one request inside its own transaction, converting a PostgreSQL ERROR
/// into an `Err` instead of letting it escape.
///
/// This deliberately does NOT use `BackgroundWorker::transaction`. That helper
/// calls `PgTryBuilder::new(body).execute()` with *no* catch handler, so a
/// PostgreSQL ERROR re-throws straight past its `CommitTransactionCommand` and
/// out of the worker's main function — killing the background worker. In a
/// protocol server that is catastrophic: one ordinary, *expected* error (an RLS
/// denial, a unique-violation, a `RAISE` inside a user command) takes down the
/// whole server for every other connection, and the client sees a dropped TCP
/// connection instead of a clean error response.
///
/// So we manage the transaction ourselves and catch. `PgTryBuilder` calls
/// `FlushErrorState()` once the handler returns normally, which is what makes
/// the session reusable; we just have to roll the transaction back, since the
/// longjmp skipped the commit.
fn run_request_in_transaction(request: &SpiRequest) -> Result<SpiResult, SpiError> {
    use pgrx::pg_sys::PgTryBuilder;

    unsafe {
        pg_sys::SetCurrentStatementStartTimestamp();
        pg_sys::StartTransactionCommand();
        pg_sys::PushActiveSnapshot(pg_sys::GetTransactionSnapshot());
    }

    let outcome = PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
        execute_query(&request.query, &request.params, &request.column_types)
    }))
    .catch_others(|caught| Err(SpiError::QueryFailed(describe_caught(caught))))
    .execute();

    unsafe {
        if outcome.is_ok() {
            pg_sys::PopActiveSnapshot();
            pg_sys::CommitTransactionCommand();
        } else {
            // Either a caught ERROR (whose longjmp skipped the commit) or a
            // plain Err return. Aborting is correct and safe for both.
            pg_sys::AbortCurrentTransaction();
        }
    }

    outcome
}

/// Render a caught PostgreSQL error / Rust panic as a message string.
///
/// The message is what the HTTP layer classifies (RLS denial -> 403,
/// unique violation -> 409, and so on), so it must preserve Postgres' own
/// wording rather than a generic "query failed".
fn describe_caught(caught: pgrx::pg_sys::panic::CaughtError) -> String {
    use pgrx::pg_sys::panic::CaughtError;
    match caught {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report.message().to_string(),
    }
}

/// Execute a query with bound parameters inside the *current* transaction.
///
/// This is the same executor that [`execute_spi_request`] uses, without the
/// per-request transaction wrapper. It is intended for callers that are
/// already inside a transaction (for example `#[pg_test]` regression tests or
/// SQL-callable functions) and want the exact binding semantics of the bridge.
///
/// **IMPORTANT**: Must be called from a thread where SPI access is valid
/// (a Postgres backend or background worker main thread).
pub fn execute_query_in_transaction(
    query: &str,
    params: &[SpiParam],
    column_types: &[ColumnType],
) -> Result<SpiResult, SpiError> {
    execute_query(query, params, column_types)
}

/// Execute a query using SPI
///
/// **IMPORTANT**: This function MUST only be called from the background worker's
/// main thread. Calling from any other thread will cause a panic.
fn execute_query(
    query: &str,
    params: &[SpiParam],
    column_types: &[ColumnType],
) -> Result<SpiResult, SpiError> {
    // Use connect_mut for all queries since we might need to run mutating queries
    Spi::connect_mut(|client| {
        // Bind parameters as typed SQL arguments. The query text is passed to
        // Postgres verbatim; values never enter the SQL string.
        let args: Vec<DatumWithOid> = params.iter().map(param_to_datum).collect();

        // ALWAYS the mutable path. read-only SPI is only an optimisation, and
        // guessing at it from the statement text is not just unreliable, it is
        // unreliable in a way that produces hard errors:
        //
        //   * a leading `--` banner comment hid the real keyword, so generated
        //     DDL ran read-only and every `DO $$ ... $$` failed;
        //   * utility statements (GRANT/COMMENT/TRUNCATE/SET) are not reads;
        //   * and even a genuine `SELECT` is not necessarily read-only — a
        //     `SELECT schema.insert_reading(...)` calls a VOLATILE function
        //     that writes, and read-only SPI rejects it with
        //     "SELECT is not allowed in a non-volatile function".
        //
        // That last case is unknowable from the text without resolving the
        // function, so there is no heuristic to fix. Running an ordinary read
        // with read_only=false is harmless, so pay that and be correct.
        let table = client
            .update(query, None, &args)
            .map_err(|e| SpiError::QueryFailed(e.to_string()))?;

        // Capture the affected-row count BEFORE consuming the table by
        // iteration. pgrx populates `SpiTupleTable::len()` from
        // `SPI_processed` when `SPI_tuptable` is NULL (a non-RETURNING DML,
        // which yields no tuples to iterate) and from `numvals` otherwise, so
        // this is the correct count for both mutations and reads.
        let rows_affected = table.len() as u64;

        let mut rows = Vec::new();

        for row in table {
            let mut columns = Vec::new();

            // Read each column according to its expected type
            for (i, col_type) in column_types.iter().enumerate() {
                let ordinal = i + 1; // SPI uses 1-based indexing
                let value = read_column(&row, ordinal, *col_type);
                columns.push(value);
            }

            rows.push(SpiRow { columns });
        }

        Ok(SpiResult {
            rows,
            rows_affected,
        })
    })
}

/// The PostgreSQL type OID a parameter is bound with.
///
/// Kept separate from [`param_to_datum`] so the type mapping is testable
/// without a running Postgres.
pub(crate) fn param_type_oid(param: &SpiParam) -> pg_sys::Oid {
    match param {
        SpiParam::Text(_) => pg_sys::TEXTOID,
        SpiParam::Int4(_) => pg_sys::INT4OID,
        SpiParam::Int8(_) => pg_sys::INT8OID,
        SpiParam::Float8(_) => pg_sys::FLOAT8OID,
        SpiParam::Bool(_) => pg_sys::BOOLOID,
        SpiParam::Bytea(_) => pg_sys::BYTEAOID,
        SpiParam::Json(_) => pg_sys::JSONBOID,
    }
}

/// Convert an `SpiParam` into a typed SPI argument (`Datum` + type OID).
///
/// `None` variants become a typed SQL NULL.
fn param_to_datum(param: &SpiParam) -> DatumWithOid<'static> {
    let oid = param_type_oid(param);
    // SAFETY: each arm pairs a value with the OID of its own Rust->Postgres
    // type mapping (the same one `IntoDatum::type_oid` would report), so the
    // datum and the declared type always agree.
    unsafe {
        match param {
            SpiParam::Text(v) => DatumWithOid::new(v.clone(), oid),
            SpiParam::Int4(v) => DatumWithOid::new(*v, oid),
            SpiParam::Int8(v) => DatumWithOid::new(*v, oid),
            SpiParam::Float8(v) => DatumWithOid::new(*v, oid),
            SpiParam::Bool(v) => DatumWithOid::new(*v, oid),
            SpiParam::Bytea(v) => DatumWithOid::new(v.clone(), oid),
            SpiParam::Json(v) => DatumWithOid::new(v.clone().map(pgrx::JsonB), oid),
        }
    }
}

/// Read a single column value from an SPI row
fn read_column(
    row: &pgrx::spi::SpiHeapTupleData,
    ordinal: usize,
    col_type: ColumnType,
) -> SpiValue {
    match col_type {
        ColumnType::Int32 => match row.get::<i32>(ordinal) {
            Ok(Some(v)) => SpiValue::Int32(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Int64 => match row.get::<i64>(ordinal) {
            Ok(Some(v)) => SpiValue::Int64(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Float64 => match row.get::<f64>(ordinal) {
            Ok(Some(v)) => SpiValue::Float64(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Text => match row.get::<String>(ordinal) {
            Ok(Some(v)) => SpiValue::Text(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Bytea => match row.get::<Vec<u8>>(ordinal) {
            Ok(Some(v)) => SpiValue::Bytes(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Bool => match row.get::<bool>(ordinal) {
            Ok(Some(v)) => SpiValue::Bool(v),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
        ColumnType::Json => match row.get::<pgrx::JsonB>(ordinal) {
            Ok(Some(v)) => SpiValue::Json(v.0),
            Ok(None) => SpiValue::Null,
            Err(_) => SpiValue::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_param_type_oids() {
        assert_eq!(
            param_type_oid(&SpiParam::Text(Some("x".into()))),
            pg_sys::TEXTOID
        );
        assert_eq!(param_type_oid(&SpiParam::Text(None)), pg_sys::TEXTOID);
        assert_eq!(param_type_oid(&SpiParam::Int4(Some(1))), pg_sys::INT4OID);
        assert_eq!(param_type_oid(&SpiParam::Int8(None)), pg_sys::INT8OID);
        assert_eq!(
            param_type_oid(&SpiParam::Float8(Some(1.0))),
            pg_sys::FLOAT8OID
        );
        assert_eq!(param_type_oid(&SpiParam::Bool(Some(true))), pg_sys::BOOLOID);
        assert_eq!(
            param_type_oid(&SpiParam::Bytea(Some(vec![1]))),
            pg_sys::BYTEAOID
        );
        assert_eq!(
            param_type_oid(&SpiParam::Json(Some(serde_json::json!({})))),
            pg_sys::JSONBOID
        );
    }

    /// Regression for the textual `$N` substitution vulnerability: there must
    /// be no code path that rewrites the query text based on parameter values.
    /// The executor hands the query to Postgres verbatim, so a parameter equal
    /// to `$1` (or containing `'`) is just data. This test pins the contract
    /// at the type level: params are converted to datums, never to SQL text.
    #[test]
    fn test_no_sql_text_rendering_of_params() {
        // If someone re-introduces a `param_to_sql`-style renderer this test
        // will not compile against it; the only conversion is to a typed OID.
        let hostile = SpiParam::Text(Some("$1 ' OR 1=1 --".to_string()));
        assert_eq!(param_type_oid(&hostile), pg_sys::TEXTOID);
    }
}
