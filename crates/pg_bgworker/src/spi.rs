//! SPI calls that take a transaction ID only when they write a row.
//!
//! pgrx's `Spi::get_one*`, `Spi::run*` and `SpiClient::update` call
//! `GetCurrentTransactionId()` before they run anything. So a worker poll that
//! only reads — or an `UPDATE … RETURNING` job claim that matches nothing —
//! still commits a transaction with an XID: one WAL commit record and one XID
//! burnt per poll. pg_streaming's four executors polling every 10 ms that way
//! wrote ~400 empty commits a second on an idle server.
//!
//! These run a single statement through a plan prepared as mutating, which
//! pgrx executes with `read_only = false` (DML and VOLATILE functions work as
//! usual) but without that call: PostgreSQL assigns the XID itself the moment
//! a row is written, and never when nothing is.
//!
//! A script of several statements still goes through `update`: a prepared
//! plan parse-analyzes every statement before running the first, so
//! `CREATE TABLE t …; INSERT INTO t …` fails on `t`. Scripts are DDL that
//! writes anyway.

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult, SpiTupleTable};
use pgrx::PgOid;

/// Run `query` on `client`, returning at most `limit` rows (`None`: all).
pub fn execute<'conn>(
    client: &mut SpiClient<'conn>,
    query: &str,
    limit: Option<i64>,
    args: &[DatumWithOid<'_>],
) -> SpiResult<SpiTupleTable<'conn>> {
    let limit = limit.map(|l| l as _);
    if statement_count(query) > 1 {
        return client.update(query, limit, args);
    }
    let oids: Vec<PgOid> = args.iter().map(|a| PgOid::from(a.oid())).collect();
    let stmt = client.prepare_mut(query, &oids)?;
    client.select(&stmt, limit, args)
}

/// How many statements `query` holds (raw parse only; a syntax error raises
/// as executing it would)
fn statement_count(query: &str) -> usize {
    let Ok(cquery) = std::ffi::CString::new(query) else {
        return 1; // an interior NUL: let SPI report it
    };
    // SAFETY: raw_parser reads the NUL-terminated string and returns a List
    // palloc'd in the current (SPI) memory context, or NIL
    let list =
        unsafe { pg_sys::raw_parser(cquery.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT) };
    if list.is_null() {
        0
    } else {
        unsafe { (*list).length as usize }
    }
}

/// `Spi::get_one_with_args` without the up-front XID
pub fn get_one_with_args<A: FromDatum + IntoDatum>(
    query: &str,
    args: &[DatumWithOid<'_>],
) -> SpiResult<Option<A>> {
    Spi::connect_mut(|client| execute(client, query, Some(1), args)?.first().get_one())
}

/// `Spi::get_one` without the up-front XID
pub fn get_one<A: FromDatum + IntoDatum>(query: &str) -> SpiResult<Option<A>> {
    get_one_with_args(query, &[])
}

/// `Spi::run_with_args` without the up-front XID
pub fn run_with_args(query: &str, args: &[DatumWithOid<'_>]) -> SpiResult<()> {
    Spi::connect_mut(|client| execute(client, query, None, args).map(|_| ()))
}

/// `Spi::run` without the up-front XID
pub fn run(query: &str) -> SpiResult<()> {
    run_with_args(query, &[])
}

/// Whether the current transaction holds an XID (it will write a commit
/// record). For tests asserting an idle worker round writes nothing.
pub fn xid_assigned() -> bool {
    unsafe { pg_sys::GetCurrentTransactionIdIfAny() != pg_sys::InvalidTransactionId }
}
