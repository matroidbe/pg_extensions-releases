use pgrx::prelude::*;
use std::cell::Cell;

/// Manually check if the current transaction is balanced.
/// Useful for testing and debugging.
#[pg_extern]
pub fn check_balance() {
    validate_transaction_balance();
}

thread_local! {
    static TXN_HAS_LEDGER_ENTRIES: Cell<bool> = const { Cell::new(false) };
}

/// Mark that the current transaction has created ledger entries.
/// Called from journal.rs when entries are created.
pub fn mark_ledger_activity() {
    TXN_HAS_LEDGER_ENTRIES.with(|f| f.set(true));
}

/// Check if the current transaction has any ledger activity.
pub fn has_ledger_activity() -> bool {
    TXN_HAS_LEDGER_ENTRIES.with(|f| f.get())
}

/// Clear the ledger activity flag (called on COMMIT/ABORT).
pub fn clear_ledger_activity() {
    TXN_HAS_LEDGER_ENTRIES.with(|f| f.set(false));
}

/// Validate that debits equal credits for the current transaction.
/// Called at PRE_COMMIT if ledger activity was detected.
pub fn validate_transaction_balance() {
    let result = Spi::get_two::<f64, f64>(
        r#"
        SELECT COALESCE(SUM(debit), 0)::float8,
               COALESCE(SUM(credit), 0)::float8
        FROM pgledger.journal_entry je
        JOIN pgledger.journal j ON je.journal_id = j.id
        WHERE j.xact_id = pg_current_xact_id()
        "#,
    );

    match result {
        Ok((Some(total_debits), Some(total_credits))) => {
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
        Ok(_) => {
            // No entries found — nothing to validate
        }
        Err(e) => {
            pgrx::warning!("pg_ledger: failed to validate balance: {}", e);
        }
    }
}
