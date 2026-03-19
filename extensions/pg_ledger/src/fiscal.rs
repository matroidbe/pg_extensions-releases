use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;

/// Open a new fiscal period.
#[pg_extern]
pub fn open_period(period: &str) {
    Spi::run_with_args(
        r#"
        INSERT INTO pgledger.fiscal_period (period, status)
        VALUES ($1, 'open')
        ON CONFLICT (period) DO UPDATE SET status = 'open', closed_at = NULL, closed_by = NULL
        "#,
        &[period.into()],
    )
    .expect("failed to open fiscal period");
}

/// Close a fiscal period. Prevents new journal entries in this period.
#[pg_extern]
pub fn close_period(period: &str) {
    let updated = Spi::get_one_with_args::<String>(
        r#"
        UPDATE pgledger.fiscal_period
        SET status = 'closed', closed_at = now(), closed_by = current_user
        WHERE period = $1
        RETURNING period
        "#,
        &[period.into()],
    );

    match updated {
        Ok(Some(_)) => {}
        _ => {
            // Period doesn't exist yet — create it as closed
            Spi::run_with_args(
                r#"
                INSERT INTO pgledger.fiscal_period (period, status, closed_at, closed_by)
                VALUES ($1, 'closed', now(), current_user)
                "#,
                &[period.into()],
            )
            .expect("failed to close fiscal period");
        }
    }
}

/// Get the status of all fiscal periods.
#[pg_extern]
pub fn period_status() -> TableIterator<
    'static,
    (
        name!(period, String),
        name!(status, String),
        name!(closed_at, Option<TimestampWithTimeZone>),
        name!(closed_by, Option<String>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT period, status, closed_at, closed_by FROM pgledger.fiscal_period ORDER BY period",
                None,
                &[],
            )
            .unwrap();

        for row in result {
            let period: String = row.get_by_name("period").unwrap().unwrap();
            let status: String = row.get_by_name("status").unwrap().unwrap();
            let closed_at: Option<TimestampWithTimeZone> = row.get_by_name("closed_at").unwrap();
            let closed_by: Option<String> = row.get_by_name("closed_by").unwrap();
            rows.push((period, status, closed_at, closed_by));
        }
    });

    TableIterator::new(rows)
}
