use pgrx::prelude::*;

use crate::calendar::get_calendar_id;

/// Check if a given date is a working day in the calendar.
/// Priority: exception > holiday > weekly pattern.
#[pg_extern(immutable)]
pub fn is_working_day(calendar_name: &str, check_date: pgrx::datum::Date) -> bool {
    let cal_id = get_calendar_id(calendar_name);
    check_working(cal_id, check_date)
}

/// Add N working days to a date. Negative N subtracts working days.
#[pg_extern(immutable)]
pub fn add_working_days(
    calendar_name: &str,
    start_date: pgrx::datum::Date,
    days: i32,
) -> pgrx::datum::Date {
    let cal_id = get_calendar_id(calendar_name);

    if days == 0 {
        return start_date;
    }

    let step: i32 = if days > 0 { 1 } else { -1 };
    let mut remaining = days.abs();
    let mut current = date_to_text(start_date);

    while remaining > 0 {
        current = add_pg_days(&current, step);
        let d = text_to_date(&current);
        if check_working(cal_id, d) {
            remaining -= 1;
        }
    }

    text_to_date(&current)
}

/// Count working days between two dates (exclusive of start, inclusive of end when forward).
#[pg_extern(immutable)]
pub fn working_days_between(
    calendar_name: &str,
    start_date: pgrx::datum::Date,
    end_date: pgrx::datum::Date,
) -> i32 {
    let cal_id = get_calendar_id(calendar_name);

    let start_str = date_to_text(start_date);
    let end_str = date_to_text(end_date);

    if start_str == end_str {
        return 0;
    }

    let forward = start_str < end_str;
    let (from, to) = if forward {
        (&start_str, &end_str)
    } else {
        (&end_str, &start_str)
    };

    let mut count = 0i32;
    let mut current = from.clone();
    loop {
        current = add_pg_days(&current, 1);
        if current > *to {
            break;
        }
        let d = text_to_date(&current);
        if check_working(cal_id, d) {
            count += 1;
        }
    }

    if forward {
        count
    } else {
        -count
    }
}

/// Find the next working day after the given date.
#[pg_extern(immutable)]
pub fn next_working_day(calendar_name: &str, from_date: pgrx::datum::Date) -> pgrx::datum::Date {
    let cal_id = get_calendar_id(calendar_name);
    let mut current = date_to_text(from_date);

    for _ in 0..365 {
        current = add_pg_days(&current, 1);
        let d = text_to_date(&current);
        if check_working(cal_id, d) {
            return d;
        }
    }

    pgrx::error!("pg_calendar: no working day found within 365 days");
}

/// Find the previous working day before the given date.
#[pg_extern(immutable)]
pub fn prev_working_day(calendar_name: &str, from_date: pgrx::datum::Date) -> pgrx::datum::Date {
    let cal_id = get_calendar_id(calendar_name);
    let mut current = date_to_text(from_date);

    for _ in 0..365 {
        current = add_pg_days(&current, -1);
        let d = text_to_date(&current);
        if check_working(cal_id, d) {
            return d;
        }
    }

    pgrx::error!("pg_calendar: no working day found within 365 days");
}

// =============================================================================
// Internal helpers
// =============================================================================

/// Check if a date is a working day for a calendar (by id).
fn check_working(cal_id: i32, check_date: pgrx::datum::Date) -> bool {
    // 1. Check exception first (highest priority)
    let exception = Spi::get_one_with_args::<bool>(
        r#"
        SELECT is_working FROM pgcalendar.exception
        WHERE calendar_id = $1 AND exception_date = $2
        "#,
        &[cal_id.into(), check_date.into()],
    );
    if let Ok(Some(is_working)) = exception {
        return is_working;
    }

    // 2. Check holiday (non-working override)
    let is_holiday = Spi::get_one_with_args::<bool>(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM pgcalendar.holiday
            WHERE calendar_id = $1
              AND (
                  holiday_date = $2
                  OR (recurring AND EXTRACT(MONTH FROM holiday_date) = EXTRACT(MONTH FROM $2::date)
                                AND EXTRACT(DAY FROM holiday_date) = EXTRACT(DAY FROM $2::date))
              )
        )
        "#,
        &[cal_id.into(), check_date.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if is_holiday {
        return false;
    }

    // 3. Fall back to weekly pattern
    Spi::get_one_with_args::<bool>(
        r#"
        SELECT is_working FROM pgcalendar.weekly_pattern
        WHERE calendar_id = $1 AND day_of_week = EXTRACT(DOW FROM $2::date)::smallint
        "#,
        &[cal_id.into(), check_date.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false)
}

/// Convert a Date to text representation for arithmetic.
fn date_to_text(d: pgrx::datum::Date) -> String {
    Spi::get_one_with_args::<String>("SELECT $1::date::text", &[d.into()])
        .unwrap()
        .unwrap()
}

/// Convert text date back to Date.
fn text_to_date(s: &str) -> pgrx::datum::Date {
    Spi::get_one_with_args::<pgrx::datum::Date>("SELECT $1::date", &[s.into()])
        .unwrap()
        .unwrap()
}

/// Add days to a date (as text) using PostgreSQL date arithmetic.
fn add_pg_days(date_str: &str, days: i32) -> String {
    Spi::get_one_with_args::<String>(
        "SELECT ($1::date + $2)::text",
        &[date_str.into(), days.into()],
    )
    .unwrap()
    .unwrap()
}
