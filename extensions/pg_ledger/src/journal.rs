use pgrx::prelude::*;

/// Create a journal header and return its ID (internal API for triggers/rules).
pub fn create_journal_header_internal(
    source_table: Option<&str>,
    source_id: Option<&str>,
    description: Option<&str>,
) -> i64 {
    Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pgledger.journal (xact_id, source_table, source_id, description)
        VALUES (pg_current_xact_id(), $1, $2, $3)
        RETURNING id
        "#,
        &[source_table.into(), source_id.into(), description.into()],
    )
    .expect("failed to create journal header")
    .expect("no id returned from journal insert")
}

/// Create a single journal entry line (internal API for triggers/rules).
pub fn create_journal_line_internal(
    journal_id: i64,
    account: &str,
    debit: f64,
    credit: f64,
    currency: &str,
) {
    Spi::run_with_args(
        r#"
        INSERT INTO pgledger.journal_entry (journal_id, account, debit, credit, currency)
        VALUES ($1, $2, $3, $4, $5)
        "#,
        &[
            journal_id.into(),
            account.into(),
            debit.into(),
            credit.into(),
            currency.into(),
        ],
    )
    .expect("failed to create journal entry");
}

/// Record a debit entry in the journal.
#[pg_extern]
pub fn debit(
    account: &str,
    amount: f64,
    description: default!(Option<&str>, "NULL"),
    currency: default!(Option<&str>, "NULL"),
) {
    if amount < 0.0 {
        pgrx::error!("debit amount must be non-negative");
    }
    if amount == 0.0 {
        return;
    }

    let currency = currency.unwrap_or("USD");
    let journal_id = create_journal_header_internal(None, None, description);
    create_journal_line_internal(journal_id, account, amount, 0.0, currency);
    crate::balance::mark_ledger_activity();
}

/// Record a credit entry in the journal.
#[pg_extern]
pub fn credit(
    account: &str,
    amount: f64,
    description: default!(Option<&str>, "NULL"),
    currency: default!(Option<&str>, "NULL"),
) {
    if amount < 0.0 {
        pgrx::error!("credit amount must be non-negative");
    }
    if amount == 0.0 {
        return;
    }

    let currency = currency.unwrap_or("USD");
    let journal_id = create_journal_header_internal(None, None, description);
    create_journal_line_internal(journal_id, account, 0.0, amount, currency);
    crate::balance::mark_ledger_activity();
}

/// Record a journal entry with explicit side ('debit' or 'credit').
#[pg_extern]
pub fn entry(
    account: &str,
    amount: f64,
    side: &str,
    description: default!(Option<&str>, "NULL"),
    currency: default!(Option<&str>, "NULL"),
) {
    match side.to_lowercase().as_str() {
        "debit" => debit(account, amount, description, currency),
        "credit" => credit(account, amount, description, currency),
        _ => pgrx::error!("side must be 'debit' or 'credit', got '{}'", side),
    }
}

/// Create a reversal of an existing journal entry.
/// Swaps all debits and credits, preserving the full audit trail.
#[pg_extern]
pub fn reverse(journal_id: i64, reason: default!(Option<&str>, "NULL")) -> i64 {
    let description = reason.unwrap_or("Reversal");

    // Verify original journal exists
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgledger.journal WHERE id = $1)",
        &[journal_id.into()],
    )
    .expect("failed to check journal existence")
    .unwrap_or(false);

    if !exists {
        pgrx::error!("journal entry {} does not exist", journal_id);
    }

    // Create reversal header
    let reversal_id = create_journal_header_internal(
        None,
        Some(&format!("reversal:{}", journal_id)),
        Some(description),
    );

    // Read original entries and create reversed versions
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
            SELECT account, debit::float8, credit::float8, currency
            FROM pgledger.journal_entry
            WHERE journal_id = $1
            "#,
                None,
                &[journal_id.into()],
            )
            .unwrap();

        for row in result {
            let account: String = row.get_by_name("account").unwrap().unwrap();
            let orig_debit: f64 = row.get_by_name("debit").unwrap().unwrap_or(0.0);
            let orig_credit: f64 = row.get_by_name("credit").unwrap().unwrap_or(0.0);
            let currency: String = row
                .get_by_name("currency")
                .unwrap()
                .unwrap_or_else(|| "USD".to_string());

            // Swap debit and credit for reversal
            create_journal_line_internal(reversal_id, &account, orig_credit, orig_debit, &currency);
        }
    });

    reversal_id
}
