use pgrx::prelude::*;

use crate::currency::get_decimal_places;
use crate::rate::lookup_rate;

/// Round a numeric amount to the currency's decimal places.
#[pg_extern]
pub fn round_to_currency(amount: pgrx::AnyNumeric, currency_code: &str) -> pgrx::AnyNumeric {
    let decimals = get_decimal_places(currency_code);
    Spi::get_one_with_args::<pgrx::AnyNumeric>(
        "SELECT ROUND($1, $2)",
        &[amount.into(), decimals.into()],
    )
    .unwrap()
    .unwrap()
}

/// Convert an amount from one currency to another using base-relative triangulation.
///
/// Algorithm: result = amount * (to_rate / from_rate)
/// Where rates are relative to the configured base currency.
#[pg_extern]
pub fn convert(
    amount: pgrx::AnyNumeric,
    from_currency: &str,
    to_currency: &str,
    as_of: default!(Option<pgrx::datum::Date>, "NULL"),
    rate_type: default!(Option<&str>, "NULL"),
    round: default!(Option<bool>, "true"),
) -> pgrx::AnyNumeric {
    let from_upper = from_currency.to_uppercase();
    let to_upper = to_currency.to_uppercase();

    // Identity conversion
    if from_upper == to_upper {
        return amount;
    }

    let as_of_date = as_of.unwrap_or_else(|| {
        Spi::get_one::<pgrx::datum::Date>("SELECT CURRENT_DATE")
            .unwrap()
            .unwrap()
    });

    let base = crate::get_base_currency();

    let one = Spi::get_one::<pgrx::AnyNumeric>("SELECT 1::numeric")
        .unwrap()
        .unwrap();

    let from_rate = if from_upper == base {
        one.clone()
    } else {
        lookup_rate(&from_upper, as_of_date, rate_type)
    };

    let to_rate = if to_upper == base {
        one
    } else {
        lookup_rate(&to_upper, as_of_date, rate_type)
    };

    // result = amount * to_rate / from_rate
    let result = Spi::get_one_with_args::<pgrx::AnyNumeric>(
        "SELECT ($1 * $2 / $3)::numeric",
        &[amount.into(), to_rate.into(), from_rate.into()],
    )
    .unwrap()
    .unwrap();

    if round.unwrap_or(true) {
        round_to_currency(result, &to_upper)
    } else {
        result
    }
}
