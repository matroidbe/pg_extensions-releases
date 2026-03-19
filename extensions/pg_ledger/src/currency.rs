use pgrx::datum::Date;
use pgrx::prelude::*;

/// Set an exchange rate between two currencies.
#[pg_extern]
pub fn set_exchange_rate(
    from_currency: &str,
    to_currency: &str,
    rate: f64,
    valid_from: default!(Option<Date>, "NULL"),
    valid_to: default!(Option<Date>, "NULL"),
) {
    if rate <= 0.0 {
        pgrx::error!("exchange rate must be positive");
    }

    Spi::run_with_args(
        r#"
        INSERT INTO pgledger.exchange_rate (from_currency, to_currency, rate, valid_from, valid_to)
        VALUES ($1, $2, $3, COALESCE($4, CURRENT_DATE), $5)
        ON CONFLICT (from_currency, to_currency, valid_from)
        DO UPDATE SET rate = EXCLUDED.rate, valid_to = EXCLUDED.valid_to
        "#,
        &[
            from_currency.into(),
            to_currency.into(),
            rate.into(),
            valid_from.into(),
            valid_to.into(),
        ],
    )
    .expect("failed to set exchange rate");
}

/// Convert an amount from one currency to another using stored exchange rates.
#[pg_extern]
pub fn convert(
    amount: f64,
    from_currency: &str,
    to_currency: &str,
    as_of: default!(Option<Date>, "NULL"),
) -> f64 {
    if from_currency == to_currency {
        return amount;
    }

    let rate = Spi::get_one_with_args::<f64>(
        r#"
        SELECT rate::float8
        FROM pgledger.exchange_rate
        WHERE from_currency = $1
          AND to_currency = $2
          AND valid_from <= COALESCE($3, CURRENT_DATE)
          AND (valid_to IS NULL OR valid_to >= COALESCE($3, CURRENT_DATE))
        ORDER BY valid_from DESC
        LIMIT 1
        "#,
        &[from_currency.into(), to_currency.into(), as_of.into()],
    );

    match rate {
        Ok(Some(r)) => amount * r,
        _ => pgrx::error!(
            "no exchange rate found for {} -> {} as of {}",
            from_currency,
            to_currency,
            as_of
                .map(|d: Date| d.to_string())
                .unwrap_or_else(|| "today".to_string())
        ),
    }
}
