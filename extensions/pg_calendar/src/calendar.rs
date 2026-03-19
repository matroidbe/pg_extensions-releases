use pgrx::prelude::*;

/// Create a new calendar. Initializes all 7 weekdays as non-working.
#[pg_extern]
pub fn create_calendar(name: &str, description: default!(Option<&str>, "NULL")) -> i32 {
    let cal_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgcalendar.calendar (name, description)
        VALUES ($1, $2)
        RETURNING id
        "#,
        &[name.into(), description.into()],
    )
    .expect("failed to create calendar")
    .expect("no id returned from calendar insert");

    // Initialize 7 weekdays as non-working
    for dow in 0..7i16 {
        Spi::run_with_args(
            r#"
            INSERT INTO pgcalendar.weekly_pattern (calendar_id, day_of_week, is_working)
            VALUES ($1, $2, false)
            "#,
            &[cal_id.into(), dow.into()],
        )
        .expect("failed to initialize weekly pattern");
    }

    cal_id
}

/// Drop a calendar by name (cascades to pattern, holidays, exceptions).
#[pg_extern]
pub fn drop_calendar(name: &str) {
    let deleted = Spi::get_one_with_args::<String>(
        "DELETE FROM pgcalendar.calendar WHERE name = $1 RETURNING name",
        &[name.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("pg_calendar: calendar '{}' does not exist", name),
    }
}

/// Clone a calendar (pattern + holidays + exceptions).
#[pg_extern]
pub fn clone_calendar(
    source_name: &str,
    new_name: &str,
    description: default!(Option<&str>, "NULL"),
) -> i32 {
    let source_id = get_calendar_id(source_name);

    let new_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgcalendar.calendar (name, description)
        VALUES ($1, $2)
        RETURNING id
        "#,
        &[new_name.into(), description.into()],
    )
    .expect("failed to create cloned calendar")
    .expect("no id returned");

    // Clone weekly pattern
    Spi::run_with_args(
        r#"
        INSERT INTO pgcalendar.weekly_pattern (calendar_id, day_of_week, is_working)
        SELECT $2, day_of_week, is_working
        FROM pgcalendar.weekly_pattern WHERE calendar_id = $1
        "#,
        &[source_id.into(), new_id.into()],
    )
    .expect("failed to clone weekly pattern");

    // Clone holidays
    Spi::run_with_args(
        r#"
        INSERT INTO pgcalendar.holiday (calendar_id, name, holiday_date, recurring)
        SELECT $2, name, holiday_date, recurring
        FROM pgcalendar.holiday WHERE calendar_id = $1
        "#,
        &[source_id.into(), new_id.into()],
    )
    .expect("failed to clone holidays");

    // Clone exceptions
    Spi::run_with_args(
        r#"
        INSERT INTO pgcalendar.exception (calendar_id, exception_date, is_working, reason)
        SELECT $2, exception_date, is_working, reason
        FROM pgcalendar.exception WHERE calendar_id = $1
        "#,
        &[source_id.into(), new_id.into()],
    )
    .expect("failed to clone exceptions");

    new_id
}

/// Look up calendar ID by name.
pub fn get_calendar_id(name: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgcalendar.calendar WHERE name = $1",
        &[name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| {
        pgrx::error!("pg_calendar: calendar '{}' does not exist", name);
    })
}
