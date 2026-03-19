use pgrx::prelude::*;

use crate::calendar::get_calendar_id;

/// Seed an ISO standard calendar (Mon-Fri working).
#[pg_extern]
pub fn seed_iso() -> i32 {
    let exists =
        Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pgcalendar.calendar WHERE name = 'iso')")
            .unwrap_or(Some(false))
            .unwrap_or(false);

    if exists {
        return get_calendar_id("iso");
    }

    let cal_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgcalendar.calendar (name, description)
        VALUES ($1, $2)
        RETURNING id
        "#,
        &["iso".into(), "ISO standard: Monday through Friday".into()],
    )
    .expect("failed to create iso calendar")
    .expect("no id returned");

    for dow in 0..7i16 {
        let is_working = (1..=5).contains(&dow);
        Spi::run_with_args(
            r#"
            INSERT INTO pgcalendar.weekly_pattern (calendar_id, day_of_week, is_working)
            VALUES ($1, $2, $3)
            "#,
            &[cal_id.into(), dow.into(), is_working.into()],
        )
        .expect("failed to set weekly pattern");
    }

    cal_id
}

/// Seed a Middle East calendar (Sun-Thu working).
#[pg_extern]
pub fn seed_middle_east() -> i32 {
    let exists = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgcalendar.calendar WHERE name = 'middle_east')",
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if exists {
        return get_calendar_id("middle_east");
    }

    let cal_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgcalendar.calendar (name, description)
        VALUES ($1, $2)
        RETURNING id
        "#,
        &[
            "middle_east".into(),
            "Middle East: Sunday through Thursday".into(),
        ],
    )
    .expect("failed to create middle_east calendar")
    .expect("no id returned");

    for dow in 0..7i16 {
        let is_working = dow <= 4;
        Spi::run_with_args(
            r#"
            INSERT INTO pgcalendar.weekly_pattern (calendar_id, day_of_week, is_working)
            VALUES ($1, $2, $3)
            "#,
            &[cal_id.into(), dow.into(), is_working.into()],
        )
        .expect("failed to set weekly pattern");
    }

    cal_id
}

/// Seed a six-day calendar (Mon-Sat working).
#[pg_extern]
pub fn seed_six_day() -> i32 {
    let exists = Spi::get_one::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgcalendar.calendar WHERE name = 'six_day')",
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if exists {
        return get_calendar_id("six_day");
    }

    let cal_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgcalendar.calendar (name, description)
        VALUES ($1, $2)
        RETURNING id
        "#,
        &[
            "six_day".into(),
            "Six-day week: Monday through Saturday".into(),
        ],
    )
    .expect("failed to create six_day calendar")
    .expect("no id returned");

    for dow in 0..7i16 {
        let is_working = (1..=6).contains(&dow);
        Spi::run_with_args(
            r#"
            INSERT INTO pgcalendar.weekly_pattern (calendar_id, day_of_week, is_working)
            VALUES ($1, $2, $3)
            "#,
            &[cal_id.into(), dow.into(), is_working.into()],
        )
        .expect("failed to set weekly pattern");
    }

    cal_id
}
