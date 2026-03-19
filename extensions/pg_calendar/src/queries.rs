use pgrx::prelude::*;

use crate::calendar::get_calendar_id;

/// Get calendar summary information.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn calendar_info(
    calendar_name: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(description, Option<String>),
        name!(working_days, String),
        name!(holiday_count, i64),
        name!(exception_count, i64),
    ),
> {
    let cal_id = get_calendar_id(calendar_name);
    let day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    let mut rows = Vec::new();
    Spi::connect(|client| {
        // Get calendar name/description
        let cal_row = client
            .select(
                "SELECT name, description FROM pgcalendar.calendar WHERE id = $1",
                None,
                &[cal_id.into()],
            )
            .unwrap();

        let mut name = String::new();
        let mut description: Option<String> = None;
        for row in cal_row {
            name = row.get_by_name("name").unwrap().unwrap();
            description = row.get_by_name("description").unwrap();
        }

        // Get working days pattern
        let pattern = client
            .select(
                r#"
                SELECT day_of_week FROM pgcalendar.weekly_pattern
                WHERE calendar_id = $1 AND is_working = true
                ORDER BY day_of_week
                "#,
                None,
                &[cal_id.into()],
            )
            .unwrap();

        let mut working: Vec<String> = Vec::new();
        for row in pattern {
            let dow: i16 = row.get_by_name("day_of_week").unwrap().unwrap();
            working.push(day_names[dow as usize].to_string());
        }
        let working_str = if working.is_empty() {
            "none".to_string()
        } else {
            working.join(", ")
        };

        // Count holidays
        let holiday_count: i64 = client
            .select(
                "SELECT COUNT(*) FROM pgcalendar.holiday WHERE calendar_id = $1",
                None,
                &[cal_id.into()],
            )
            .unwrap()
            .first()
            .get::<i64>(1)
            .unwrap()
            .unwrap_or(0);

        // Count exceptions
        let exception_count: i64 = client
            .select(
                "SELECT COUNT(*) FROM pgcalendar.exception WHERE calendar_id = $1",
                None,
                &[cal_id.into()],
            )
            .unwrap()
            .first()
            .get::<i64>(1)
            .unwrap()
            .unwrap_or(0);

        rows.push((
            name,
            description,
            working_str,
            holiday_count,
            exception_count,
        ));
    });

    TableIterator::new(rows)
}

/// Show date range with working status for each day.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn calendar_days(
    calendar_name: &str,
    from_date: pgrx::datum::Date,
    to_date: pgrx::datum::Date,
) -> TableIterator<
    'static,
    (
        name!(day_date, String),
        name!(day_name, String),
        name!(is_working, bool),
        name!(note, Option<String>),
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
        // Generate date series
        let dates = client
            .select(
                "SELECT d::date::text AS dt, EXTRACT(DOW FROM d)::int AS dow FROM generate_series($1::date, $2::date, '1 day'::interval) d",
                None,
                &[from_date.into(), to_date.into()],
            )
            .unwrap();

        let mut date_list: Vec<(String, i32)> = Vec::new();
        for row in dates {
            let dt: String = row.get_by_name("dt").unwrap().unwrap();
            let dow: i32 = row.get_by_name("dow").unwrap().unwrap();
            date_list.push((dt, dow));
        }

        for (dt, dow) in &date_list {
            // Check exception
            let exception = client
                .select(
                    "SELECT is_working, reason FROM pgcalendar.exception WHERE calendar_id = $1 AND exception_date = $2::date",
                    None,
                    &[cal_id.into(), dt.as_str().into()],
                )
                .unwrap();

            let mut found_exception = false;
            for row in exception {
                let is_working: bool = row.get_by_name("is_working").unwrap().unwrap();
                let reason: Option<String> = row.get_by_name("reason").unwrap();
                let note = reason.or_else(|| Some("exception".to_string()));
                rows.push((
                    dt.clone(),
                    day_names[*dow as usize].to_string(),
                    is_working,
                    note,
                ));
                found_exception = true;
            }
            if found_exception {
                continue;
            }

            // Check holiday
            let holiday = client
                .select(
                    r#"
                    SELECT name FROM pgcalendar.holiday
                    WHERE calendar_id = $1
                      AND (holiday_date = $2::date
                           OR (recurring AND EXTRACT(MONTH FROM holiday_date) = EXTRACT(MONTH FROM $2::date)
                                         AND EXTRACT(DAY FROM holiday_date) = EXTRACT(DAY FROM $2::date)))
                    LIMIT 1
                    "#,
                    None,
                    &[cal_id.into(), dt.as_str().into()],
                )
                .unwrap();

            let mut found_holiday = false;
            for row in holiday {
                let name: String = row.get_by_name("name").unwrap().unwrap();
                rows.push((
                    dt.clone(),
                    day_names[*dow as usize].to_string(),
                    false,
                    Some(name),
                ));
                found_holiday = true;
            }
            if found_holiday {
                continue;
            }

            // Weekly pattern
            let dow_i16 = *dow as i16;
            let wp = client
                .select(
                    "SELECT is_working FROM pgcalendar.weekly_pattern WHERE calendar_id = $1 AND day_of_week = $2",
                    None,
                    &[cal_id.into(), dow_i16.into()],
                )
                .unwrap();

            let mut is_working = false;
            for row in wp {
                is_working = row.get_by_name("is_working").unwrap().unwrap();
            }

            rows.push((
                dt.clone(),
                day_names[*dow as usize].to_string(),
                is_working,
                None,
            ));
        }
    });

    TableIterator::new(rows)
}
