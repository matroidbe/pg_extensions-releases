use pgrx::prelude::*;

/// List currencies (optionally active only).
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn list_currencies(
    active_only: default!(Option<bool>, "false"),
) -> TableIterator<
    'static,
    (
        name!(code, String),
        name!(name, String),
        name!(symbol, Option<String>),
        name!(decimal_places, i16),
        name!(active, bool),
    ),
> {
    let filter = if active_only.unwrap_or(false) {
        "WHERE active = true"
    } else {
        ""
    };
    let query = format!(
        "SELECT code, name, symbol, decimal_places, active FROM pgcurrency.currency {} ORDER BY code",
        filter
    );

    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client.select(&query, None, &[]).unwrap();
        for row in result {
            let code: String = row.get_by_name("code").unwrap().unwrap();
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let symbol: Option<String> = row.get_by_name("symbol").unwrap();
            let decimal_places: i16 = row.get_by_name("decimal_places").unwrap().unwrap();
            let active: bool = row.get_by_name("active").unwrap().unwrap();
            rows.push((code, name, symbol, decimal_places, active));
        }
    });

    TableIterator::new(rows)
}

/// Show all rates as of a given date (most recent on or before).
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn rates_as_of(
    as_of: default!(Option<pgrx::datum::Date>, "NULL"),
    rate_type: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(currency_code, String),
        name!(rate, pgrx::AnyNumeric),
        name!(valid_from, pgrx::datum::Date),
        name!(source, Option<String>),
    ),
> {
    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT DISTINCT ON (currency_code)
                    currency_code, rate, valid_from, source
                FROM pgcurrency.exchange_rate
                WHERE valid_from <= COALESCE($1, CURRENT_DATE)
                  AND (rate_type IS NOT DISTINCT FROM $2)
                ORDER BY currency_code, valid_from DESC
                "#,
                None,
                &[as_of.into(), rate_type.into()],
            )
            .unwrap();
        for row in result {
            let currency_code: String = row.get_by_name("currency_code").unwrap().unwrap();
            let rate: pgrx::AnyNumeric = row.get_by_name("rate").unwrap().unwrap();
            let valid_from: pgrx::datum::Date = row.get_by_name("valid_from").unwrap().unwrap();
            let source: Option<String> = row.get_by_name("source").unwrap();
            rows.push((currency_code, rate, valid_from, source));
        }
    });

    TableIterator::new(rows)
}

/// Show rate history for a currency code (optionally bounded by date range).
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn rate_history(
    currency_code: &str,
    from_date: default!(Option<pgrx::datum::Date>, "NULL"),
    to_date: default!(Option<pgrx::datum::Date>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(rate, pgrx::AnyNumeric),
        name!(valid_from, pgrx::datum::Date),
        name!(rate_type, Option<String>),
        name!(source, Option<String>),
    ),
> {
    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT rate, valid_from, rate_type, source
                FROM pgcurrency.exchange_rate
                WHERE currency_code = UPPER($1)
                  AND ($2::date IS NULL OR valid_from >= $2)
                  AND ($3::date IS NULL OR valid_from <= $3)
                ORDER BY valid_from DESC
                "#,
                None,
                &[currency_code.into(), from_date.into(), to_date.into()],
            )
            .unwrap();
        for row in result {
            let rate: pgrx::AnyNumeric = row.get_by_name("rate").unwrap().unwrap();
            let valid_from: pgrx::datum::Date = row.get_by_name("valid_from").unwrap().unwrap();
            let rate_type: Option<String> = row.get_by_name("rate_type").unwrap();
            let source: Option<String> = row.get_by_name("source").unwrap();
            rows.push((rate, valid_from, rate_type, source));
        }
    });

    TableIterator::new(rows)
}
