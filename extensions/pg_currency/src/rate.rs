use pgrx::prelude::*;

/// Set (or update) an exchange rate. UPSERT on (currency_code, valid_from, rate_type).
#[pg_extern]
pub fn set_rate(
    currency_code: &str,
    rate: pgrx::AnyNumeric,
    valid_from: default!(Option<pgrx::datum::Date>, "NULL"),
    rate_type: default!(Option<&str>, "NULL"),
    source: default!(Option<&str>, "NULL"),
) {
    Spi::run_with_args(
        r#"
        INSERT INTO pgcurrency.exchange_rate (currency_code, rate, valid_from, rate_type, source)
        VALUES (UPPER($1), $2, COALESCE($3, CURRENT_DATE), $4, $5)
        ON CONFLICT (currency_code, valid_from, COALESCE(rate_type, ''))
        DO UPDATE SET rate = EXCLUDED.rate, source = EXCLUDED.source
        "#,
        &[
            currency_code.into(),
            rate.into(),
            valid_from.into(),
            rate_type.into(),
            source.into(),
        ],
    )
    .expect("failed to set exchange rate");
}

/// Internal: look up the most recent rate on or before `as_of` for a currency.
pub fn lookup_rate(
    currency_code: &str,
    as_of: pgrx::datum::Date,
    rate_type: Option<&str>,
) -> pgrx::AnyNumeric {
    let result = Spi::get_one_with_args::<pgrx::AnyNumeric>(
        r#"
        SELECT rate FROM pgcurrency.exchange_rate
        WHERE currency_code = UPPER($1)
          AND valid_from <= $2
          AND (rate_type IS NOT DISTINCT FROM $3)
        ORDER BY valid_from DESC
        LIMIT 1
        "#,
        &[currency_code.into(), as_of.into(), rate_type.into()],
    );
    match result {
        Ok(Some(r)) => r,
        _ => pgrx::error!(
            "pg_currency: no rate found for '{}' on or before {}",
            currency_code.to_uppercase(),
            as_of
        ),
    }
}

/// Get the exchange rate between two currencies.
/// Uses base-relative triangulation: rate = to_rate / from_rate.
#[pg_extern]
pub fn get_rate(
    from_currency: &str,
    to_currency: &str,
    as_of: default!(Option<pgrx::datum::Date>, "NULL"),
    rate_type: default!(Option<&str>, "NULL"),
) -> pgrx::AnyNumeric {
    let from_upper = from_currency.to_uppercase();
    let to_upper = to_currency.to_uppercase();

    if from_upper == to_upper {
        return Spi::get_one::<pgrx::AnyNumeric>("SELECT 1::numeric")
            .unwrap()
            .unwrap();
    }

    let as_of_date = as_of.unwrap_or_else(|| {
        Spi::get_one::<pgrx::datum::Date>("SELECT CURRENT_DATE")
            .unwrap()
            .unwrap()
    });

    let base = crate::get_base_currency();

    let from_rate = if from_upper == base {
        Spi::get_one::<pgrx::AnyNumeric>("SELECT 1::numeric")
            .unwrap()
            .unwrap()
    } else {
        lookup_rate(&from_upper, as_of_date, rate_type)
    };

    let to_rate = if to_upper == base {
        Spi::get_one::<pgrx::AnyNumeric>("SELECT 1::numeric")
            .unwrap()
            .unwrap()
    } else {
        lookup_rate(&to_upper, as_of_date, rate_type)
    };

    // Triangulate: to_rate / from_rate
    Spi::get_one_with_args::<pgrx::AnyNumeric>(
        "SELECT ($1 / $2)::numeric",
        &[to_rate.into(), from_rate.into()],
    )
    .unwrap()
    .unwrap()
}
