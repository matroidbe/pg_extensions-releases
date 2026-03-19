use pgrx::prelude::*;

use crate::calendar::get_calendar_id;

/// Add a date exception (override) to a calendar.
#[pg_extern]
pub fn add_exception(
    calendar_name: &str,
    exception_date: pgrx::datum::Date,
    is_working: bool,
    reason: default!(Option<&str>, "NULL"),
) {
    let cal_id = get_calendar_id(calendar_name);

    Spi::run_with_args(
        r#"
        INSERT INTO pgcalendar.exception (calendar_id, exception_date, is_working, reason)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (calendar_id, exception_date)
        DO UPDATE SET is_working = EXCLUDED.is_working, reason = EXCLUDED.reason
        "#,
        &[
            cal_id.into(),
            exception_date.into(),
            is_working.into(),
            reason.into(),
        ],
    )
    .expect("failed to add exception");
}

/// Remove a date exception from a calendar.
#[pg_extern]
pub fn remove_exception(calendar_name: &str, exception_date: pgrx::datum::Date) {
    let cal_id = get_calendar_id(calendar_name);
    let deleted = Spi::get_one_with_args::<i32>(
        "DELETE FROM pgcalendar.exception WHERE calendar_id = $1 AND exception_date = $2 RETURNING id",
        &[cal_id.into(), exception_date.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("pg_calendar: no exception on that date for this calendar"),
    }
}
