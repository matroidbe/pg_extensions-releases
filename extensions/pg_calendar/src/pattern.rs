use pgrx::prelude::*;

use crate::calendar::get_calendar_id;

/// Set a single weekday's working status.
#[pg_extern]
pub fn set_weekday(calendar_name: &str, day_of_week: i32, is_working: bool) {
    if !(0..=6).contains(&day_of_week) {
        pgrx::error!(
            "pg_calendar: day_of_week must be 0-6 (Sun-Sat), got {}",
            day_of_week
        );
    }
    let cal_id = get_calendar_id(calendar_name);
    let dow = day_of_week as i16;

    Spi::run_with_args(
        r#"
        UPDATE pgcalendar.weekly_pattern
        SET is_working = $3
        WHERE calendar_id = $1 AND day_of_week = $2
        "#,
        &[cal_id.into(), dow.into(), is_working.into()],
    )
    .expect("failed to update weekday");
}

/// Set working days from a comma-separated list of day numbers (e.g., "1,2,3,4,5" for Mon-Fri).
#[pg_extern]
pub fn set_working_days(calendar_name: &str, days_csv: &str) {
    let cal_id = get_calendar_id(calendar_name);

    // First, set all days to non-working
    Spi::run_with_args(
        "UPDATE pgcalendar.weekly_pattern SET is_working = false WHERE calendar_id = $1",
        &[cal_id.into()],
    )
    .expect("failed to reset weekly pattern");

    // Parse CSV and set working days
    for part in days_csv.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let dow: i16 = trimmed.parse().unwrap_or_else(|_| {
            pgrx::error!(
                "pg_calendar: invalid day number '{}', expected 0-6",
                trimmed
            );
        });
        if !(0..=6).contains(&dow) {
            pgrx::error!(
                "pg_calendar: day_of_week must be 0-6 (Sun-Sat), got {}",
                dow
            );
        }

        Spi::run_with_args(
            r#"
            UPDATE pgcalendar.weekly_pattern
            SET is_working = true
            WHERE calendar_id = $1 AND day_of_week = $2
            "#,
            &[cal_id.into(), dow.into()],
        )
        .expect("failed to set working day");
    }
}

/// Show the 7-day weekly pattern for a calendar.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn weekly_pattern(
    calendar_name: &str,
) -> TableIterator<
    'static,
    (
        name!(day_of_week, i16),
        name!(day_name, String),
        name!(is_working, bool),
    ),
> {
    let cal_id = get_calendar_id(calendar_name);
    let day_names = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
    ];

    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT day_of_week, is_working
                FROM pgcalendar.weekly_pattern
                WHERE calendar_id = $1
                ORDER BY day_of_week
                "#,
                None,
                &[cal_id.into()],
            )
            .unwrap();

        for row in result {
            let dow: i16 = row.get_by_name("day_of_week").unwrap().unwrap();
            let is_working: bool = row.get_by_name("is_working").unwrap().unwrap();
            rows.push((dow, day_names[dow as usize].to_string(), is_working));
        }
    });

    TableIterator::new(rows)
}
