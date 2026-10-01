//! Run arbitrary connector code inside a PostgreSQL subtransaction.
//!
//! Connectors write through SPI, and a failing write raises a PostgreSQL error
//! rather than returning a Rust `Err`. pgrx's FFI boundary turns that ereport
//! longjmp into a Rust panic, so `PgTryBuilder` *can* catch it — but catching
//! alone is not enough: the surrounding transaction is left aborted and every
//! later SPI call in it fails too. Without a subtransaction the only correct
//! thing to do is let the error escape, which kills the executor's tick and
//! restarts the worker straight back into the same record: a crash-loop.
//!
//! So error-handling paths that must not take the pipeline down with them —
//! writing a failed record to the dead-letter output, above all — run here
//! instead. This is the same mechanism PL/pgSQL's `EXCEPTION` block uses, in
//! the same order:
//!
//! ```text
//! BeginInternalSubTransaction(NULL)
//!   PG_TRY:    body; ReleaseCurrentSubTransaction()
//!   PG_CATCH:  FlushErrorState(); RollbackAndReleaseCurrentSubTransaction()
//! restore CurrentMemoryContext and CurrentResourceOwner either way
//! ```
//!
//! Where the record itself is the thing being isolated, prefer
//! `pgstreams.call_guarded` — running the subtransaction inside PL/pgSQL gets
//! this bookkeeping right for free. This module exists for the cases that
//! can't be expressed as a SQL string, i.e. calls into a `dyn OutputConnector`.

use pgrx::pg_sys;
use pgrx::prelude::*;
use std::panic::AssertUnwindSafe;

/// Run `f` inside a subtransaction.
///
/// Returns `Ok` with `f`'s value if it completed, or `Err(message)` if it
/// raised — in which case the subtransaction has been rolled back and the
/// *enclosing* transaction is still usable, so the caller can carry on.
///
/// Anything `f` wrote is rolled back on error. Non-transactional side effects
/// (`dblink`, the `http` extension, `NOTIFY`) are not, exactly as for any other
/// subtransaction.
pub fn in_subtransaction<F, R>(f: F) -> Result<R, String>
where
    F: FnOnce() -> R,
{
    unsafe {
        // Save what the rollback path has to put back by hand. Postgres
        // restores neither for us.
        let old_context = pg_sys::CurrentMemoryContext;
        let old_owner = pg_sys::CurrentResourceOwner;

        pg_sys::BeginInternalSubTransaction(std::ptr::null());

        // BeginInternalSubTransaction leaves us in the subtransaction's own
        // context, which is freed on release. Switch back so anything `f`
        // allocates outlives the subtransaction.
        pg_sys::MemoryContextSwitchTo(old_context);

        PgTryBuilder::new(AssertUnwindSafe(|| {
            let result = f();
            pg_sys::ReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
            Ok(result)
        }))
        .catch_others(move |caught| {
            // Order matters and mirrors PL/pgSQL exec_stmt_block: leave the
            // error context first, clear the error state, only then roll back.
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::FlushErrorState();
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(old_context);
            pg_sys::CurrentResourceOwner = old_owner;
            Err(describe(caught))
        })
        .execute()
    }
}

/// [`in_subtransaction`] for a body that already returns `Result<_, String>`,
/// flattening "the body raised" and "the body returned an error" into one.
///
/// This is the form most call sites want: connectors report some failures as
/// `Err` and raise others, and to the caller the difference is noise.
pub fn in_subtransaction_flat<F, T>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
{
    in_subtransaction(f).and_then(|inner| inner)
}

/// Render a caught error as a message, keeping the SQLSTATE where there is one.
fn describe(caught: pgrx::pg_sys::panic::CaughtError) -> String {
    use pgrx::pg_sys::panic::CaughtError;
    match caught {
        CaughtError::PostgresError(e) | CaughtError::ErrorReport(e) => {
            format!("{} [{:?}]", e.message(), e.sql_error_code())
        }
        CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::in_subtransaction;
    use pgrx::prelude::*;

    #[pg_test]
    fn test_returns_value_on_success() {
        let result = in_subtransaction(|| 42);
        assert_eq!(result, Ok(42));
    }

    #[pg_test]
    fn test_commits_writes_on_success() {
        Spi::run("CREATE TABLE sx (n int)").unwrap();

        let result = in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (1)").unwrap();
        });
        assert!(result.is_ok());

        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM sx").unwrap();
        assert_eq!(count, Some(1), "a released subtransaction keeps its writes");
    }

    #[pg_test]
    fn test_catches_error_and_rolls_back() {
        Spi::run("CREATE TABLE sx (n int)").unwrap();

        let result = in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (1)").unwrap();
            // Same shape as a dead-letter table missing a column.
            Spi::run("INSERT INTO sx (nope) VALUES (2)").unwrap();
        });

        let err = result.expect_err("a raising body must come back as Err");
        assert!(err.contains("nope"), "unexpected message: {}", err);

        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM sx").unwrap();
        assert_eq!(count, Some(0), "the earlier insert must roll back too");
    }

    /// The point of the whole module: the enclosing transaction survives, so
    /// the caller can keep working instead of taking the worker down.
    #[pg_test]
    fn test_enclosing_transaction_stays_usable() {
        Spi::run("CREATE TABLE sx (n int)").unwrap();

        let result = in_subtransaction(|| {
            Spi::run("INSERT INTO sx (nope) VALUES (1)").unwrap();
        });
        assert!(result.is_err());

        // Would fail with "current transaction is aborted" if the error had
        // been caught without a subtransaction.
        Spi::run("INSERT INTO sx VALUES (99)").unwrap();
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM sx").unwrap();
        assert_eq!(count, Some(1));
    }

    /// Repeated failures must not accumulate error state (`errordata_stack_size`
    /// grows without FlushErrorState and eventually panics Postgres itself).
    #[pg_test]
    fn test_many_consecutive_failures() {
        Spi::run("CREATE TABLE sx (n int)").unwrap();

        for i in 0..50 {
            let result = in_subtransaction(|| {
                Spi::run("INSERT INTO sx (nope) VALUES (1)").unwrap();
            });
            assert!(result.is_err(), "iteration {} should have failed", i);
        }

        Spi::run("INSERT INTO sx VALUES (1)").unwrap();
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM sx").unwrap();
        assert_eq!(count, Some(1));
    }

    #[pg_test]
    fn test_alternating_success_and_failure() {
        Spi::run("CREATE TABLE sx (n int)").unwrap();

        for i in 0..10 {
            let _ = in_subtransaction(|| {
                Spi::run("INSERT INTO sx (nope) VALUES (1)").unwrap();
            });
            let ok = in_subtransaction(|| {
                Spi::run_with_args("INSERT INTO sx VALUES ($1)", &[i.into()]).unwrap();
            });
            assert!(ok.is_ok(), "iteration {} should have succeeded", i);
        }

        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM sx").unwrap();
        assert_eq!(count, Some(10));
    }

    #[pg_test]
    fn test_catches_rust_panic() {
        let result: Result<(), String> = in_subtransaction(|| panic!("boom"));
        let err = result.expect_err("a Rust panic must come back as Err");
        assert!(err.contains("boom"), "unexpected message: {}", err);

        // And the transaction is still usable afterwards.
        Spi::run("SELECT 1").unwrap();
    }

    #[pg_test]
    fn test_nested_subtransactions() {
        Spi::run("CREATE TABLE sx (n int)").unwrap();

        let result = in_subtransaction(|| {
            Spi::run("INSERT INTO sx VALUES (1)").unwrap();
            let inner = in_subtransaction(|| {
                Spi::run("INSERT INTO sx (nope) VALUES (2)").unwrap();
            });
            assert!(inner.is_err());
            Spi::run("INSERT INTO sx VALUES (3)").unwrap();
        });

        assert!(result.is_ok());
        let sum = Spi::get_one::<i64>("SELECT COALESCE(sum(n), 0)::bigint FROM sx").unwrap();
        assert_eq!(sum, Some(4), "outer writes kept, inner rolled back");
    }
}
