use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;

/// Get the balance for a specific account.
#[pg_extern]
pub fn account_balance(
    account: &str,
    as_of: default!(Option<TimestampWithTimeZone>, "NULL"),
    currency: default!(Option<&str>, "NULL"),
) -> pgrx::AnyNumeric {
    let currency = currency.unwrap_or("USD");

    let result = match as_of {
        Some(ts) => Spi::get_one_with_args::<pgrx::AnyNumeric>(
            r#"
            SELECT COALESCE(SUM(je.debit) - SUM(je.credit), 0)::numeric
            FROM pgledger.journal_entry je
            JOIN pgledger.journal j ON je.journal_id = j.id
            WHERE je.account = $1 AND je.currency = $2 AND j.posted_at <= $3
            "#,
            &[account.into(), currency.into(), ts.into()],
        ),
        None => Spi::get_one_with_args::<pgrx::AnyNumeric>(
            r#"
            SELECT COALESCE(SUM(je.debit) - SUM(je.credit), 0)::numeric
            FROM pgledger.journal_entry je
            WHERE je.account = $1 AND je.currency = $2
            "#,
            &[account.into(), currency.into()],
        ),
    };

    result
        .expect("failed to query account balance")
        .unwrap_or_else(|| pgrx::AnyNumeric::try_from(0.0_f64).unwrap())
}

type TrialBalanceRow = (String, pgrx::AnyNumeric, pgrx::AnyNumeric, pgrx::AnyNumeric);

fn read_trial_balance_rows(result: pgrx::spi::SpiTupleTable) -> Vec<TrialBalanceRow> {
    let mut rows = Vec::new();
    for row in result {
        let account: String = row.get_by_name("account").unwrap().unwrap();
        let total_debit: pgrx::AnyNumeric = row.get_by_name("total_debit").unwrap().unwrap();
        let total_credit: pgrx::AnyNumeric = row.get_by_name("total_credit").unwrap().unwrap();
        let balance: pgrx::AnyNumeric = row.get_by_name("balance").unwrap().unwrap();
        rows.push((account, total_debit, total_credit, balance));
    }
    rows
}

/// Return a trial balance: per-account totals of debits, credits, and net balance.
#[pg_extern]
pub fn trial_balance(
    as_of: default!(Option<TimestampWithTimeZone>, "NULL"),
    account_like: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(account, String),
        name!(total_debit, pgrx::AnyNumeric),
        name!(total_credit, pgrx::AnyNumeric),
        name!(balance, pgrx::AnyNumeric),
    ),
> {
    let rows = Spi::connect(|client| {
        let base_query = |where_clause: &str| {
            format!(
                r#"
                SELECT je.account,
                       COALESCE(SUM(je.debit), 0)::numeric AS total_debit,
                       COALESCE(SUM(je.credit), 0)::numeric AS total_credit,
                       COALESCE(SUM(je.debit) - SUM(je.credit), 0)::numeric AS balance
                FROM pgledger.journal_entry je
                {}
                GROUP BY je.account
                ORDER BY je.account
                "#,
                where_clause
            )
        };

        match (&as_of, &account_like) {
            (Some(ts), Some(pattern)) => {
                let q = base_query("JOIN pgledger.journal j ON je.journal_id = j.id WHERE j.posted_at <= $1 AND je.account LIKE $2");
                read_trial_balance_rows(
                    client
                        .select(&q, None, &[(*ts).into(), (*pattern).into()])
                        .unwrap(),
                )
            }
            (Some(ts), None) => {
                let q = base_query(
                    "JOIN pgledger.journal j ON je.journal_id = j.id WHERE j.posted_at <= $1",
                );
                read_trial_balance_rows(client.select(&q, None, &[(*ts).into()]).unwrap())
            }
            (None, Some(pattern)) => {
                let q = base_query("WHERE je.account LIKE $1");
                read_trial_balance_rows(client.select(&q, None, &[(*pattern).into()]).unwrap())
            }
            (None, None) => {
                let q = base_query("");
                read_trial_balance_rows(client.select(&q, None, &[]).unwrap())
            }
        }
    });

    TableIterator::new(rows)
}

/// Return journal entries with optional filtering.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn journal_entries(
    from_date: default!(Option<TimestampWithTimeZone>, "NULL"),
    to_date: default!(Option<TimestampWithTimeZone>, "NULL"),
    account_filter: default!(Option<&str>, "NULL"),
    source_table_filter: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(journal_id, i64),
        name!(posted_at, TimestampWithTimeZone),
        name!(account, String),
        name!(debit, pgrx::AnyNumeric),
        name!(credit, pgrx::AnyNumeric),
        name!(currency, String),
        name!(description, Option<String>),
        name!(source_table, Option<String>),
    ),
> {
    let mut rows = Vec::new();

    // Build the query with dynamic SQL to avoid heterogeneous arg types
    let mut conditions = Vec::new();

    if let Some(from) = from_date {
        let formatted = format!("'{}'", from);
        conditions.push(format!("j.posted_at >= {}", formatted));
    }
    if let Some(to) = to_date {
        let formatted = format!("'{}'", to);
        conditions.push(format!("j.posted_at <= {}", formatted));
    }
    if let Some(acct) = account_filter {
        conditions.push(format!("je.account = '{}'", acct.replace('\'', "''")));
    }
    if let Some(src) = source_table_filter {
        conditions.push(format!("j.source_table = '{}'", src.replace('\'', "''")));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query = format!(
        r#"
        SELECT j.id AS journal_id, j.posted_at, je.account,
               je.debit, je.credit, je.currency,
               j.description, j.source_table
        FROM pgledger.journal_entry je
        JOIN pgledger.journal j ON je.journal_id = j.id
        {}
        ORDER BY j.posted_at DESC, j.id DESC, je.id
        "#,
        where_clause
    );

    Spi::connect(|client| {
        let result = client.select(&query, None, &[]).unwrap();

        for row in result {
            let journal_id: i64 = row.get_by_name("journal_id").unwrap().unwrap();
            let posted_at: TimestampWithTimeZone = row.get_by_name("posted_at").unwrap().unwrap();
            let account: String = row.get_by_name("account").unwrap().unwrap();
            let debit: pgrx::AnyNumeric = row.get_by_name("debit").unwrap().unwrap();
            let credit: pgrx::AnyNumeric = row.get_by_name("credit").unwrap().unwrap();
            let currency: String = row.get_by_name("currency").unwrap().unwrap();
            let description: Option<String> = row.get_by_name("description").unwrap();
            let source_table: Option<String> = row.get_by_name("source_table").unwrap();
            rows.push((
                journal_id,
                posted_at,
                account,
                debit,
                credit,
                currency,
                description,
                source_table,
            ));
        }
    });

    TableIterator::new(rows)
}
