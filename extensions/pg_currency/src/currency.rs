use pgrx::prelude::*;

/// Add a currency to the master table.
#[pg_extern]
pub fn add_currency(
    code: &str,
    name: &str,
    symbol: default!(Option<&str>, "NULL"),
    decimal_places: default!(Option<i32>, "2"),
    active: default!(Option<bool>, "true"),
) {
    let dec = decimal_places.unwrap_or(2) as i16;
    let act = active.unwrap_or(true);

    Spi::run_with_args(
        r#"
        INSERT INTO pgcurrency.currency (code, name, symbol, decimal_places, active)
        VALUES (UPPER($1), $2, $3, $4, $5)
        "#,
        &[
            code.into(),
            name.into(),
            symbol.into(),
            dec.into(),
            act.into(),
        ],
    )
    .expect("failed to add currency");
}

/// Remove a currency (CASCADE deletes rates).
#[pg_extern]
pub fn drop_currency(code: &str) {
    let deleted = Spi::get_one_with_args::<String>(
        "DELETE FROM pgcurrency.currency WHERE code = UPPER($1) RETURNING code",
        &[code.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!(
            "pg_currency: currency '{}' does not exist",
            code.to_uppercase()
        ),
    }
}

/// Enable or disable a currency.
#[pg_extern]
pub fn set_active(code: &str, active: bool) {
    let updated = Spi::get_one_with_args::<String>(
        "UPDATE pgcurrency.currency SET active = $2 WHERE code = UPPER($1) RETURNING code",
        &[code.into(), active.into()],
    );
    match updated {
        Ok(Some(_)) => {}
        _ => pgrx::error!(
            "pg_currency: currency '{}' does not exist",
            code.to_uppercase()
        ),
    }
}

/// Internal helper: get decimal places for a currency code.
pub fn get_decimal_places(code: &str) -> i32 {
    Spi::get_one_with_args::<i16>(
        "SELECT decimal_places FROM pgcurrency.currency WHERE code = UPPER($1)",
        &[code.into()],
    )
    .unwrap_or_else(|e| pgrx::error!("pg_currency: {}", e))
    .unwrap_or_else(|| pgrx::error!("pg_currency: currency '{}' does not exist", code)) as i32
}
