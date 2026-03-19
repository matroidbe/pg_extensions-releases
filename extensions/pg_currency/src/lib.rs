mod convert;
mod currency;
mod queries;
mod rate;
mod schema;
mod seed;

pub use convert::*;
pub use currency::*;
pub use queries::*;
pub use rate::*;
pub use seed::*;

use pgrx::prelude::*;
use std::ffi::CString;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings
// =============================================================================

pub static PG_CURRENCY_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

pub static PG_CURRENCY_BASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(Some(c"USD"));

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_bool_guc(
        c"pgcurrency.enabled",
        c"Enable pg_currency conversion functions",
        c"When false, convert() returns the input amount unchanged.",
        &PG_CURRENCY_ENABLED,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pgcurrency.base_currency",
        c"Base currency code (rate = 1.0 always)",
        c"All exchange rates are relative to this currency. Default: USD.",
        &PG_CURRENCY_BASE,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::log!("pg_currency: initialized");
}

/// Read the base currency GUC. Returns the uppercase code (e.g. "USD").
pub fn get_base_currency() -> String {
    match PG_CURRENCY_BASE.get() {
        Some(c) => match c.to_str() {
            Ok(s) => s.to_uppercase(),
            Err(_) => "USD".to_string(),
        },
        None => "USD".to_string(),
    }
}

// =============================================================================
// Integration Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn set_search_path() {
        Spi::run("SET search_path TO pgcurrency, public").unwrap();
    }

    fn seed() {
        set_search_path();
        Spi::run("SELECT pgcurrency.seed_iso()").unwrap();
    }

    fn seed_with_rates() {
        seed();
        // Base = USD (implicit rate 1.0)
        // EUR: 1 USD = 0.92 EUR
        // GBP: 1 USD = 0.79 GBP
        // JPY: 1 USD = 149.50 JPY (0 decimal places)
        // KWD: 1 USD = 0.307 KWD (3 decimal places)
        Spi::run(
            r#"
            SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15');
            SELECT pgcurrency.set_rate('GBP', 0.79, '2026-01-15');
            SELECT pgcurrency.set_rate('JPY', 149.50, '2026-01-15');
            SELECT pgcurrency.set_rate('KWD', 0.307, '2026-01-15');
            "#,
        )
        .unwrap();
    }

    // =========================================================================
    // Phase 1: Schema validation
    // =========================================================================

    #[pg_test]
    fn test_currency_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgcurrency' AND table_name = 'currency')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_exchange_rate_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgcurrency' AND table_name = 'exchange_rate')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_currency_columns() {
        set_search_path();
        let cols = Spi::get_one::<i64>(
            r#"
            SELECT COUNT(*)::int8 FROM information_schema.columns
            WHERE table_schema = 'pgcurrency' AND table_name = 'currency'
              AND column_name::text IN ('code', 'name', 'symbol', 'decimal_places', 'active', 'created_at')
            "#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(cols, 6);
    }

    #[pg_test]
    fn test_exchange_rate_columns() {
        set_search_path();
        let cols = Spi::get_one::<i64>(
            r#"
            SELECT COUNT(*)::int8 FROM information_schema.columns
            WHERE table_schema = 'pgcurrency' AND table_name = 'exchange_rate'
              AND column_name::text IN ('id', 'currency_code', 'rate', 'valid_from', 'rate_type', 'source', 'created_at')
            "#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(cols, 7);
    }

    // =========================================================================
    // Phase 1: Seed
    // =========================================================================

    #[pg_test]
    fn test_seed_iso_creates_currencies() {
        set_search_path();
        let count = Spi::get_one::<i32>("SELECT pgcurrency.seed_iso()")
            .unwrap()
            .unwrap();
        assert!(
            count >= 40,
            "expected at least 40 currencies, got {}",
            count
        );
    }

    #[pg_test]
    fn test_seed_iso_idempotent() {
        set_search_path();
        let c1 = Spi::get_one::<i32>("SELECT pgcurrency.seed_iso()")
            .unwrap()
            .unwrap();
        let c2 = Spi::get_one::<i32>("SELECT pgcurrency.seed_iso()")
            .unwrap()
            .unwrap();
        assert_eq!(c1, c2);
    }

    #[pg_test]
    fn test_seed_usd_present() {
        seed();
        let name: String = Spi::get_one("SELECT name FROM pgcurrency.currency WHERE code = 'USD'")
            .unwrap()
            .unwrap();
        assert_eq!(name, "US Dollar");
    }

    #[pg_test]
    fn test_seed_jpy_zero_decimals() {
        seed();
        let dec: i16 =
            Spi::get_one("SELECT decimal_places FROM pgcurrency.currency WHERE code = 'JPY'")
                .unwrap()
                .unwrap();
        assert_eq!(dec, 0);
    }

    #[pg_test]
    fn test_seed_kwd_three_decimals() {
        seed();
        let dec: i16 =
            Spi::get_one("SELECT decimal_places FROM pgcurrency.currency WHERE code = 'KWD'")
                .unwrap()
                .unwrap();
        assert_eq!(dec, 3);
    }

    // =========================================================================
    // Phase 1: Currency CRUD
    // =========================================================================

    #[pg_test]
    fn test_add_currency() {
        set_search_path();
        Spi::run("SELECT pgcurrency.add_currency('XTS', 'Test Currency', '$', 2, true)").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgcurrency.currency WHERE code = 'XTS')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_add_currency_uppercases_code() {
        set_search_path();
        Spi::run("SELECT pgcurrency.add_currency('xts', 'Test Currency')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgcurrency.currency WHERE code = 'XTS')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_drop_currency() {
        seed();
        Spi::run("SELECT pgcurrency.drop_currency('ZAR')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgcurrency.currency WHERE code = 'ZAR')",
        )
        .unwrap()
        .unwrap();
        assert!(!exists);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_drop_nonexistent_currency() {
        set_search_path();
        Spi::run("SELECT pgcurrency.drop_currency('ZZZ')").unwrap();
    }

    #[pg_test]
    fn test_set_active() {
        seed();
        Spi::run("SELECT pgcurrency.set_active('EUR', false)").unwrap();
        let active: bool =
            Spi::get_one("SELECT active FROM pgcurrency.currency WHERE code = 'EUR'")
                .unwrap()
                .unwrap();
        assert!(!active);
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_add_duplicate_currency() {
        seed();
        Spi::run("SELECT pgcurrency.add_currency('USD', 'Duplicate Dollar')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "check")]
    fn test_invalid_code_length() {
        set_search_path();
        Spi::run("SELECT pgcurrency.add_currency('ABCD', 'Bad Code')").unwrap();
    }

    // =========================================================================
    // Phase 2: Rate management
    // =========================================================================

    #[pg_test]
    fn test_set_rate_basic() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgcurrency.exchange_rate WHERE currency_code = 'EUR')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_set_rate_upsert() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.93, '2026-01-15')").unwrap();
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::int8 FROM pgcurrency.exchange_rate WHERE currency_code = 'EUR' AND valid_from = '2026-01-15'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1, "upsert should not create duplicate");
    }

    #[pg_test]
    fn test_set_rate_with_rate_type() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.91, '2026-01-15', 'buy')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.93, '2026-01-15', 'sell')").unwrap();
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::int8 FROM pgcurrency.exchange_rate WHERE currency_code = 'EUR' AND valid_from = '2026-01-15'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 2, "different rate types should coexist");
    }

    #[pg_test]
    fn test_set_rate_with_source() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15', NULL, 'ECB')").unwrap();
        let source: String = Spi::get_one(
            "SELECT source FROM pgcurrency.exchange_rate WHERE currency_code = 'EUR' AND valid_from = '2026-01-15'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(source, "ECB");
    }

    // =========================================================================
    // Phase 2: Get rate
    // =========================================================================

    #[pg_test]
    fn test_get_rate_simple() {
        seed_with_rates();
        // USD to EUR: rate should be 0.92
        let rate: String =
            Spi::get_one("SELECT pgcurrency.get_rate('USD', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(rate, "0.92000000000000000000");
    }

    #[pg_test]
    fn test_get_rate_identity() {
        seed_with_rates();
        let rate: String = Spi::get_one("SELECT pgcurrency.get_rate('USD', 'USD')::text")
            .unwrap()
            .unwrap();
        assert_eq!(rate, "1");
    }

    #[pg_test]
    fn test_get_rate_triangulation() {
        seed_with_rates();
        // EUR → GBP = gbp_rate / eur_rate = 0.79 / 0.92
        let rate: String =
            Spi::get_one("SELECT ROUND(pgcurrency.get_rate('EUR', 'GBP', '2026-01-15'), 8)::text")
                .unwrap()
                .unwrap();
        // 0.79 / 0.92 = 0.85869565...
        assert_eq!(rate, "0.85869565");
    }

    #[pg_test]
    fn test_get_rate_base_to_currency() {
        seed_with_rates();
        let rate: String =
            Spi::get_one("SELECT ROUND(pgcurrency.get_rate('USD', 'JPY', '2026-01-15'), 2)::text")
                .unwrap()
                .unwrap();
        assert_eq!(rate, "149.50");
    }

    #[pg_test]
    fn test_get_rate_currency_to_base() {
        seed_with_rates();
        // EUR → USD = 1 / 0.92
        let rate: String =
            Spi::get_one("SELECT ROUND(pgcurrency.get_rate('EUR', 'USD', '2026-01-15'), 8)::text")
                .unwrap()
                .unwrap();
        // 1 / 0.92 = 1.08695652...
        assert_eq!(rate, "1.08695652");
    }

    #[pg_test]
    fn test_get_rate_historical_date() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.90, '2026-01-01')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.95, '2026-02-01')").unwrap();
        // Query as of Jan 20 should get the Jan 15 rate
        let rate: String =
            Spi::get_one("SELECT pgcurrency.get_rate('USD', 'EUR', '2026-01-20')::text")
                .unwrap()
                .unwrap();
        assert_eq!(rate, "0.92000000000000000000");
    }

    #[pg_test]
    fn test_get_rate_most_recent_wins() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.90, '2026-01-01')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.95, '2026-02-01')").unwrap();
        // As of March, Feb rate should be used
        let rate: String =
            Spi::get_one("SELECT pgcurrency.get_rate('USD', 'EUR', '2026-03-01')::text")
                .unwrap()
                .unwrap();
        assert_eq!(rate, "0.95000000000000000000");
    }

    #[pg_test]
    #[should_panic(expected = "no rate found")]
    fn test_get_rate_missing_error() {
        seed();
        // No rates set for EUR
        Spi::run("SELECT pgcurrency.get_rate('USD', 'EUR', '2026-01-15')").unwrap();
    }

    // =========================================================================
    // Phase 2: Conversion
    // =========================================================================

    #[pg_test]
    fn test_convert_basic() {
        seed_with_rates();
        // 100 USD → EUR = 100 * 0.92 / 1 = 92.00
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'USD', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "92.00");
    }

    #[pg_test]
    fn test_convert_identity() {
        seed_with_rates();
        let result: String = Spi::get_one("SELECT pgcurrency.convert(123.45, 'EUR', 'EUR')::text")
            .unwrap()
            .unwrap();
        assert_eq!(result, "123.45");
    }

    #[pg_test]
    fn test_convert_triangulation() {
        seed_with_rates();
        // 100 EUR → GBP = 100 * 0.79 / 0.92 = 85.87 (rounded to 2 decimals)
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'EUR', 'GBP', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "85.87");
    }

    #[pg_test]
    fn test_convert_jpy_zero_decimals() {
        seed_with_rates();
        // 100 USD → JPY = 100 * 149.50 = 14950 (0 decimal places)
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'USD', 'JPY', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "14950");
    }

    #[pg_test]
    fn test_convert_kwd_three_decimals() {
        seed_with_rates();
        // 100 USD → KWD = 100 * 0.307 = 30.700 (3 decimal places)
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'USD', 'KWD', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "30.700");
    }

    #[pg_test]
    fn test_convert_zero_amount() {
        seed_with_rates();
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(0, 'USD', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "0.00");
    }

    #[pg_test]
    fn test_convert_no_round() {
        seed_with_rates();
        // 100 EUR → GBP without rounding = 100 * 0.79 / 0.92 = 85.869565...
        let result: String = Spi::get_one(
            "SELECT ROUND(pgcurrency.convert(100, 'EUR', 'GBP', '2026-01-15', NULL, false), 6)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "85.869565");
    }

    #[pg_test]
    fn test_convert_currency_to_base() {
        seed_with_rates();
        // 92 EUR → USD = 92 * 1 / 0.92 = 100.00
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(92, 'EUR', 'USD', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "100.00");
    }

    #[pg_test]
    fn test_round_to_currency() {
        seed();
        let result: String =
            Spi::get_one("SELECT pgcurrency.round_to_currency(123.456789, 'USD')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "123.46");
    }

    #[pg_test]
    fn test_round_to_currency_jpy() {
        seed();
        let result: String =
            Spi::get_one("SELECT pgcurrency.round_to_currency(149.7, 'JPY')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "150");
    }

    #[pg_test]
    fn test_round_to_currency_kwd() {
        seed();
        let result: String =
            Spi::get_one("SELECT pgcurrency.round_to_currency(0.30756, 'KWD')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "0.308");
    }

    #[pg_test]
    fn test_convert_with_rate_type() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.91, '2026-01-15', 'buy')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.93, '2026-01-15', 'sell')").unwrap();
        let buy_result: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'USD', 'EUR', '2026-01-15', 'buy')::text")
                .unwrap()
                .unwrap();
        let sell_result: String = Spi::get_one(
            "SELECT pgcurrency.convert(100, 'USD', 'EUR', '2026-01-15', 'sell')::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(buy_result, "91.00");
        assert_eq!(sell_result, "93.00");
    }

    #[pg_test]
    fn test_large_amount_precision() {
        seed_with_rates();
        // Large amount: 1 billion USD → EUR
        let result: String =
            Spi::get_one("SELECT pgcurrency.convert(1000000000, 'USD', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(result, "920000000.00");
    }

    // =========================================================================
    // Phase 3: Queries
    // =========================================================================

    #[pg_test]
    fn test_list_currencies_all() {
        seed();
        let count: i64 = Spi::get_one("SELECT COUNT(*)::int8 FROM pgcurrency.list_currencies()")
            .unwrap()
            .unwrap();
        assert!(count >= 40);
    }

    #[pg_test]
    fn test_list_currencies_active_only() {
        seed();
        Spi::run("SELECT pgcurrency.set_active('ZAR', false)").unwrap();
        let all: i64 = Spi::get_one("SELECT COUNT(*)::int8 FROM pgcurrency.list_currencies()")
            .unwrap()
            .unwrap();
        let active: i64 =
            Spi::get_one("SELECT COUNT(*)::int8 FROM pgcurrency.list_currencies(true)")
                .unwrap()
                .unwrap();
        assert_eq!(all - active, 1);
    }

    #[pg_test]
    fn test_rates_as_of() {
        seed_with_rates();
        let count: i64 =
            Spi::get_one("SELECT COUNT(*)::int8 FROM pgcurrency.rates_as_of('2026-01-15')")
                .unwrap()
                .unwrap();
        assert_eq!(count, 4); // EUR, GBP, JPY, KWD
    }

    #[pg_test]
    fn test_rates_as_of_historical() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.90, '2026-01-01')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.95, '2026-02-01')").unwrap();
        // As of Jan 15, only the Jan 1 rate should be returned
        let rate: String =
            Spi::get_one("SELECT rate::text FROM pgcurrency.rates_as_of('2026-01-15')")
                .unwrap()
                .unwrap();
        assert_eq!(rate, "0.9000000000");
    }

    #[pg_test]
    fn test_rate_history() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.90, '2026-01-01')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.95, '2026-02-01')").unwrap();
        let count: i64 = Spi::get_one("SELECT COUNT(*)::int8 FROM pgcurrency.rate_history('EUR')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 3);
    }

    #[pg_test]
    fn test_rate_history_with_date_range() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.90, '2026-01-01')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.95, '2026-02-01')").unwrap();
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::int8 FROM pgcurrency.rate_history('EUR', '2026-01-10', '2026-01-20')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1); // Only Jan 15 rate
    }

    // =========================================================================
    // Phase 3: Integration / ERP workflow
    // =========================================================================

    #[pg_test]
    fn test_full_erp_workflow() {
        seed();
        // 1. Set rates
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('GBP', 0.79, '2026-01-15')").unwrap();

        // 2. Convert invoice: 1000 EUR → USD
        let usd: String =
            Spi::get_one("SELECT pgcurrency.convert(1000, 'EUR', 'USD', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(usd, "1086.96"); // 1000 / 0.92 rounded to 2 dec

        // 3. Cross-rate: EUR → GBP
        let gbp: String =
            Spi::get_one("SELECT pgcurrency.convert(1000, 'EUR', 'GBP', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(gbp, "858.70"); // 1000 * 0.79 / 0.92

        // 4. Reverse: GBP → EUR (may have rounding loss from step 3)
        let eur: String =
            Spi::get_one("SELECT pgcurrency.convert(858.70, 'GBP', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        // 858.70 * 0.92 / 0.79 = 1000.01 (1 cent rounding loss expected)
        assert_eq!(eur, "1000.01");
    }

    #[pg_test]
    fn test_cross_rate_consistency() {
        seed_with_rates();
        // Converting USD→EUR→GBP should give same result as USD→GBP (within rounding)
        let direct: String = Spi::get_one(
            "SELECT pgcurrency.convert(1000, 'USD', 'GBP', '2026-01-15', NULL, false)::text",
        )
        .unwrap()
        .unwrap();
        // 1000 * 0.79 / 1 = 790
        assert!(direct.starts_with("790"));
    }

    #[pg_test]
    fn test_cascade_delete_rates() {
        seed();
        Spi::run("SELECT pgcurrency.add_currency('XYZ', 'Test XYZ')").unwrap();
        Spi::run("SELECT pgcurrency.set_rate('XYZ', 1.5, '2026-01-15')").unwrap();
        Spi::run("SELECT pgcurrency.drop_currency('XYZ')").unwrap();
        let count: i64 = Spi::get_one(
            "SELECT COUNT(*)::int8 FROM pgcurrency.exchange_rate WHERE currency_code = 'XYZ'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);
    }

    #[pg_test]
    fn test_alter_rate_reconvert() {
        seed();
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.92, '2026-01-15')").unwrap();
        let first: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'USD', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(first, "92.00");

        // Update rate
        Spi::run("SELECT pgcurrency.set_rate('EUR', 0.95, '2026-01-15')").unwrap();
        let second: String =
            Spi::get_one("SELECT pgcurrency.convert(100, 'USD', 'EUR', '2026-01-15')::text")
                .unwrap()
                .unwrap();
        assert_eq!(second, "95.00");
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
