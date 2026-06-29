//! Per-transaction balance tracking.
//!
//! Earlier versions ran the balance check as a SQL query inside the
//! `XACT_EVENT_PRE_COMMIT` callback. That doesn't work: by the time
//! PRE_COMMIT fires, the snapshot has been torn down, so any SPI call
//! errors with `cannot execute SQL without an outer snapshot or portal`.
//! The pgrx test harness rolls back instead of committing, so the bug was
//! invisible to in-extension tests but bit every real psql / application
//! use of pg_ledger.
//!
//! The new design is incremental: every journal-entry insert calls
//! `record_entry(debit, credit)`, which adds to a per-session running
//! total. `RegisterSubXactCallback` maintains a stack frame per
//! SAVEPOINT so a rolled-back sub-transaction's entries are dropped, and
//! committed sub-transactions merge into their parent. PRE_COMMIT just
//! reads the final totals and compares — no SPI required.
//!
//! Closes matroidbe/pg_extensions#85.

use pgrx::prelude::*;
use std::cell::RefCell;

// One stack frame per active (sub)transaction. The bottom frame is the
// top-level xact; pushes happen on `SUBXACT_EVENT_START_SUB`, pops on
// `COMMIT_SUB` (merge into parent) or `ABORT_SUB` (discard). Each frame
// holds the running (debit, credit) sums for entries created in that scope.
thread_local! {
    static BALANCE_STACK: RefCell<Vec<(f64, f64)>> = RefCell::new(vec![(0.0, 0.0)]);
}

/// Record one journal-entry line's debit + credit contribution. Called
/// from `journal::create_journal_line_internal` right after the row is
/// successfully inserted.
pub fn record_entry(debit: f64, credit: f64) {
    BALANCE_STACK.with(|s| {
        let mut s = s.borrow_mut();
        let top = s.last_mut().expect("pg_ledger: balance stack underflow");
        top.0 += debit;
        top.1 += credit;
    });
}

/// True if any journal entry has been recorded in the current transaction
/// (across every level of the stack).
pub fn has_ledger_activity() -> bool {
    BALANCE_STACK.with(|s| s.borrow().iter().any(|(d, c)| *d != 0.0 || *c != 0.0))
}

/// Reset to a single empty frame. Called from the top-level COMMIT/ABORT
/// callbacks — the session may keep running with fresh state.
pub fn clear_ledger_activity() {
    BALANCE_STACK.with(|s| {
        s.replace(vec![(0.0, 0.0)]);
    });
}

/// Push a new stack frame at the start of a SAVEPOINT.
pub fn push_subxact() {
    BALANCE_STACK.with(|s| s.borrow_mut().push((0.0, 0.0)));
}

/// Pop the top frame and merge its sums into its parent. Called on
/// `SUBXACT_EVENT_COMMIT_SUB` (i.e. `RELEASE SAVEPOINT`).
pub fn pop_subxact_commit() {
    BALANCE_STACK.with(|s| {
        let mut s = s.borrow_mut();
        let popped = s
            .pop()
            .expect("pg_ledger: subxact stack underflow on commit");
        if let Some(parent) = s.last_mut() {
            parent.0 += popped.0;
            parent.1 += popped.1;
        } else {
            // Should not happen — top-level commit goes through
            // clear_ledger_activity. Restore an empty frame to keep the
            // stack invariant.
            s.push((0.0, 0.0));
        }
    });
}

/// Pop the top frame and discard its sums. Called on
/// `SUBXACT_EVENT_ABORT_SUB` (i.e. `ROLLBACK TO SAVEPOINT`).
pub fn pop_subxact_abort() {
    BALANCE_STACK.with(|s| {
        let mut s = s.borrow_mut();
        s.pop()
            .expect("pg_ledger: subxact stack underflow on abort");
        if s.is_empty() {
            s.push((0.0, 0.0));
        }
    });
}

/// Sum of (debit, credit) across all frames currently on the stack.
fn current_totals() -> (f64, f64) {
    BALANCE_STACK.with(|s| {
        s.borrow()
            .iter()
            .fold((0.0, 0.0), |(d, c), (a, b)| (d + a, c + b))
    })
}

/// Validate that debits equal credits for the current transaction. Called
/// from `XACT_EVENT_PRE_COMMIT`. No SPI involved — reads the in-memory
/// accumulators.
pub fn validate_transaction_balance() {
    let (total_debits, total_credits) = current_totals();
    let diff = (total_debits - total_credits).abs();
    if diff > 0.00005 {
        pgrx::error!(
            "pg_ledger: unbalanced transaction - debits ({:.4}) != credits ({:.4}), difference: {:.4}",
            total_debits,
            total_credits,
            total_debits - total_credits
        );
    }
}

/// Manually check the current transaction's balance. Useful for tests and
/// debugging from SQL.
#[pg_extern]
pub fn check_balance() {
    validate_transaction_balance();
}

// ---------------------------------------------------------------------------
// Back-compat shim for older callers that flip a boolean. The new canonical
// path is `record_entry(debit, credit)` inside `create_journal_line_internal`.
// ---------------------------------------------------------------------------

/// No-op. Retained so that existing call sites in triggers.rs / journal.rs
/// keep compiling during the refactor — balance tracking now happens inside
/// `record_entry`.
pub fn mark_ledger_activity() {}
