use pgrx::prelude::*;

use crate::calendar::get_calendar_id;

/// Add a holiday to a calendar.
#[pg_extern]
pub fn add_holiday(
    calendar_name: &str,
    holiday_name: &str,
    holiday_date: pgrx::datum::Date,
    recurring: default!(Option<bool>, "false"),
) {
    let cal_id = get_calendar_id(calendar_name);
    let rec = recurring.unwrap_or(false);

    Spi::run_with_args(
        r#"
        INSERT INTO pgcalendar.holiday (calendar_id, name, holiday_date, recurring)
        VALUES ($1, $2, $3, $4)
        "#,
        &[
            cal_id.into(),
            holiday_name.into(),
            holiday_date.into(),
            rec.into(),
        ],
    )
    .expect("failed to add holiday");
}

/// Remove a holiday by name from a calendar.
#[pg_extern]
pub fn remove_holiday(calendar_name: &str, holiday_name: &str) {
    let cal_id = get_calendar_id(calendar_name);
    let deleted = Spi::get_one_with_args::<String>(
        "DELETE FROM pgcalendar.holiday WHERE calendar_id = $1 AND name = $2 RETURNING name",
        &[cal_id.into(), holiday_name.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!(
            "pg_calendar: holiday '{}' does not exist in this calendar",
            holiday_name
        ),
    }
}

/// List holidays for a calendar, optionally filtered by year.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn list_holidays(
    calendar_name: &str,
    year: default!(Option<i32>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(holiday_date, String),
        name!(recurring, bool),
    ),
> {
    let cal_id = get_calendar_id(calendar_name);

    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = match year {
            Some(y) => client
                .select(
                    r#"
                    SELECT name, holiday_date::text, recurring
                    FROM pgcalendar.holiday
                    WHERE calendar_id = $1 AND EXTRACT(YEAR FROM holiday_date) = $2
                    ORDER BY holiday_date
                    "#,
                    None,
                    &[cal_id.into(), y.into()],
                )
                .unwrap(),
            None => client
                .select(
                    r#"
                    SELECT name, holiday_date::text, recurring
                    FROM pgcalendar.holiday
                    WHERE calendar_id = $1
                    ORDER BY holiday_date
                    "#,
                    None,
                    &[cal_id.into()],
                )
                .unwrap(),
        };

        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let date: String = row.get_by_name("holiday_date").unwrap().unwrap();
            let recurring: bool = row.get_by_name("recurring").unwrap().unwrap();
            rows.push((name, date, recurring));
        }
    });

    TableIterator::new(rows)
}
