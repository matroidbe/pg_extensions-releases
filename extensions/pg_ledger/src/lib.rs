mod balance;
mod currency;
mod fiscal;
mod journal;
mod queries;
mod rules;
mod schema;
mod triggers;
mod types;

pub use balance::check_balance;
pub use currency::*;
pub use fiscal::*;
pub use journal::*;
pub use queries::*;
pub use rules::*;
pub use triggers::process_row;
pub use types::*;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings
// =============================================================================

pub static PG_LEDGER_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);
pub static PG_LEDGER_STRICT_MODE: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    pgrx::GucRegistry::define_bool_guc(
        c"pg_ledger.enabled",
        c"Enable pg_ledger balance validation",
        c"When false, skips balance validation at commit time.",
        &PG_LEDGER_ENABLED,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_bool_guc(
        c"pg_ledger.strict_mode",
        c"Error if no rules defined for ledger_amount mutations",
        c"When true, mutations on tables with ledger_amount columns without rules will error.",
        &PG_LEDGER_STRICT_MODE,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    // Register permanent transaction callback
    unsafe {
        pg_sys::RegisterXactCallback(Some(ledger_xact_callback), std::ptr::null_mut());
    }

    pgrx::log!("pg_ledger: initialized");
}

#[pg_guard]
unsafe extern "C-unwind" fn ledger_xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut std::os::raw::c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT => {
            if PG_LEDGER_ENABLED.get() && balance::has_ledger_activity() {
                balance::validate_transaction_balance();
            }
            balance::clear_ledger_activity();
        }
        pg_sys::XactEvent::XACT_EVENT_ABORT => {
            balance::clear_ledger_activity();
        }
        _ => {}
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
        Spi::run("SET search_path TO pgledger, public").unwrap();
    }

    // =========================================================================
    // Phase 1: LedgerAmount type tests
    // =========================================================================

    #[pg_test]
    fn test_ledger_amount_parse() {
        let result =
            Spi::get_one::<String>("SELECT '1500.00'::pgledger.ledgeramount::text").unwrap();
        assert_eq!(result.unwrap(), "1500.0000");
    }

    #[pg_test]
    fn test_ledger_amount_integer_input() {
        let result = Spi::get_one::<String>("SELECT '100'::pgledger.ledgeramount::text").unwrap();
        assert_eq!(result.unwrap(), "100.0000");
    }

    #[pg_test]
    fn test_ledger_amount_negative() {
        let result =
            Spi::get_one::<String>("SELECT '-42.50'::pgledger.ledgeramount::text").unwrap();
        assert_eq!(result.unwrap(), "-42.5000");
    }

    #[pg_test]
    fn test_ledger_amount_add() {
        set_search_path();
        let result =
            Spi::get_one::<String>("SELECT ('100.00'::ledgeramount + '50.25'::ledgeramount)::text")
                .unwrap();
        assert_eq!(result.unwrap(), "150.2500");
    }

    #[pg_test]
    fn test_ledger_amount_sub() {
        set_search_path();
        let result =
            Spi::get_one::<String>("SELECT ('100.00'::ledgeramount - '30.50'::ledgeramount)::text")
                .unwrap();
        assert_eq!(result.unwrap(), "69.5000");
    }

    #[pg_test]
    fn test_ledger_amount_mul_int() {
        set_search_path();
        let result = Spi::get_one::<String>("SELECT ('25.00'::ledgeramount * 4)::text").unwrap();
        assert_eq!(result.unwrap(), "100.0000");
    }

    #[pg_test]
    fn test_ledger_amount_int_mul() {
        set_search_path();
        let result = Spi::get_one::<String>("SELECT (3 * '10.00'::ledgeramount)::text").unwrap();
        assert_eq!(result.unwrap(), "30.0000");
    }

    #[pg_test]
    fn test_ledger_amount_mul_numeric() {
        set_search_path();
        let result =
            Spi::get_one::<String>("SELECT ('100.00'::ledgeramount * 0.21::float8)::text").unwrap();
        assert_eq!(result.unwrap(), "21.0000");
    }

    #[pg_test]
    fn test_ledger_amount_div_int() {
        set_search_path();
        let result = Spi::get_one::<String>("SELECT ('100.00'::ledgeramount / 3)::text").unwrap();
        assert_eq!(result.unwrap(), "33.3333");
    }

    #[pg_test]
    fn test_ledger_amount_unary_neg() {
        set_search_path();
        let result = Spi::get_one::<String>("SELECT (-'42.00'::ledgeramount)::text").unwrap();
        assert_eq!(result.unwrap(), "-42.0000");
    }

    #[pg_test]
    fn test_cast_float_to_ledger() {
        let result =
            Spi::get_one::<String>("SELECT (99.99::float8::pgledger.ledgeramount)::text").unwrap();
        assert_eq!(result.unwrap(), "99.9900");
    }

    #[pg_test]
    fn test_cast_int_to_ledger() {
        let result =
            Spi::get_one::<String>("SELECT (42::int4::pgledger.ledgeramount)::text").unwrap();
        assert_eq!(result.unwrap(), "42.0000");
    }

    #[pg_test]
    fn test_cast_ledger_to_float() {
        let result =
            Spi::get_one::<f64>("SELECT '100.5000'::pgledger.ledgeramount::float8").unwrap();
        assert!((result.unwrap() - 100.5).abs() < 0.0001);
    }

    #[pg_test]
    fn test_sum_aggregate() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE test_ledger_sum (amount ledgeramount)")?;
        Spi::run("INSERT INTO test_ledger_sum VALUES ('100.00'), ('200.00'), ('50.50')")?;
        let result =
            Spi::get_one::<String>("SELECT sum(amount)::text FROM test_ledger_sum").unwrap();
        assert_eq!(result.unwrap(), "350.5000");
        Ok(())
    }

    #[pg_test]
    fn test_min_aggregate() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE test_ledger_min (amount ledgeramount)")?;
        Spi::run("INSERT INTO test_ledger_min VALUES ('100.00'), ('50.00'), ('200.00')")?;
        let result =
            Spi::get_one::<String>("SELECT min(amount)::text FROM test_ledger_min").unwrap();
        assert_eq!(result.unwrap(), "50.0000");
        Ok(())
    }

    #[pg_test]
    fn test_max_aggregate() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE test_ledger_max (amount ledgeramount)")?;
        Spi::run("INSERT INTO test_ledger_max VALUES ('100.00'), ('50.00'), ('200.00')")?;
        let result =
            Spi::get_one::<String>("SELECT max(amount)::text FROM test_ledger_max").unwrap();
        assert_eq!(result.unwrap(), "200.0000");
        Ok(())
    }

    #[pg_test]
    fn test_comparison_operators() {
        set_search_path();
        let result =
            Spi::get_one::<bool>("SELECT '100.00'::ledgeramount > '50.00'::ledgeramount").unwrap();
        assert_eq!(result.unwrap(), true);

        let result =
            Spi::get_one::<bool>("SELECT '50.00'::ledgeramount = '50.0000'::ledgeramount").unwrap();
        assert_eq!(result.unwrap(), true);
    }

    #[pg_test]
    fn test_btree_index() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE test_ledger_idx (amount ledgeramount)")?;
        Spi::run("CREATE INDEX ON test_ledger_idx (amount)")?;
        Spi::run("INSERT INTO test_ledger_idx VALUES ('10.00'), ('20.00'), ('30.00')")?;
        let result = Spi::get_one::<String>(
            "SELECT amount::text FROM test_ledger_idx ORDER BY amount LIMIT 1",
        )
        .unwrap();
        assert_eq!(result.unwrap(), "10.0000");
        Ok(())
    }

    // =========================================================================
    // Phase 2: Schema + Journal API tests
    // =========================================================================

    #[pg_test]
    fn test_debit_creates_entry() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;

        let count = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);

        let debit_val = Spi::get_one::<f64>(
            "SELECT debit::float8 FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert!((debit_val - 100.0).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_credit_creates_entry() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;

        let credit_val = Spi::get_one::<f64>(
            "SELECT credit::float8 FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert!((credit_val - 100.0).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_entry_debit_side() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.entry('cash', 50.0, 'debit')")?;

        let debit_val = Spi::get_one::<f64>(
            "SELECT debit::float8 FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert!((debit_val - 50.0).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_entry_credit_side() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.entry('revenue', 75.0, 'credit')")?;

        let credit_val = Spi::get_one::<f64>(
            "SELECT credit::float8 FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert!((credit_val - 75.0).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_journal_header_fields() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0, 'test description')")?;

        let desc = Spi::get_one::<String>(
            "SELECT description FROM pgledger.journal ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(desc, "test description");

        let has_xact = Spi::get_one::<bool>(
            "SELECT xact_id IS NOT NULL FROM pgledger.journal ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert!(has_xact);
        Ok(())
    }

    #[pg_test]
    fn test_reverse_swaps_debit_credit() -> Result<(), spi::Error> {
        set_search_path();
        // Create original: debit cash 100
        Spi::run("SELECT pgledger.debit('cash', 100.0, 'original')")?;

        let journal_id =
            Spi::get_one::<i64>("SELECT id FROM pgledger.journal ORDER BY id DESC LIMIT 1")
                .unwrap()
                .unwrap();

        // Reverse it
        let reversal_id = Spi::get_one_with_args::<i64>(
            "SELECT pgledger.reverse($1, 'correcting error')",
            &[journal_id.into()],
        )
        .unwrap()
        .unwrap();

        assert!(reversal_id > journal_id);

        // Reversal should have credit (not debit) of 100
        let credit_val = Spi::get_one_with_args::<f64>(
            "SELECT credit::float8 FROM pgledger.journal_entry WHERE journal_id = $1",
            &[reversal_id.into()],
        )
        .unwrap()
        .unwrap();
        assert!((credit_val - 100.0).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    #[should_panic(expected = "immutable")]
    fn test_journal_immutability_update() {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)").unwrap();
        Spi::run("UPDATE pgledger.journal SET description = 'hacked'").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "immutable")]
    fn test_journal_entry_immutability_delete() {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)").unwrap();
        Spi::run("DELETE FROM pgledger.journal_entry").unwrap();
    }

    #[pg_test]
    fn test_default_currency() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;

        let currency = Spi::get_one::<String>(
            "SELECT currency FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(currency, "USD");
        Ok(())
    }

    #[pg_test]
    fn test_custom_currency() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0, NULL, 'EUR')")?;

        let currency = Spi::get_one::<String>(
            "SELECT currency FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(currency, "EUR");
        Ok(())
    }

    #[pg_test]
    fn test_account_stored_correctly() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('ar:customer-42', 250.0)")?;

        let account = Spi::get_one::<String>(
            "SELECT account FROM pgledger.journal_entry ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(account, "ar:customer-42");
        Ok(())
    }

    // =========================================================================
    // Phase 3: Balance validation tests
    // =========================================================================

    #[pg_test]
    fn test_balanced_transaction_passes() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;
        // Should not error
        Spi::run("SELECT pgledger.check_balance()")?;
        Ok(())
    }

    #[pg_test]
    #[should_panic(expected = "unbalanced")]
    fn test_unbalanced_transaction_errors() {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)").unwrap();
        // Only debit, no credit — should fail
        Spi::run("SELECT pgledger.check_balance()").unwrap();
    }

    #[pg_test]
    fn test_multiple_lines_balanced() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;
        Spi::run("SELECT pgledger.debit('tax-receivable', 21.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;
        Spi::run("SELECT pgledger.credit('tax-payable', 21.0)")?;
        Spi::run("SELECT pgledger.check_balance()")?;
        Ok(())
    }

    #[pg_test]
    #[should_panic(expected = "unbalanced")]
    fn test_off_by_small_amount_errors() {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)").unwrap();
        Spi::run("SELECT pgledger.credit('revenue', 99.9999)").unwrap();
        Spi::run("SELECT pgledger.check_balance()").unwrap();
    }

    #[pg_test]
    fn test_no_entries_passes() -> Result<(), spi::Error> {
        set_search_path();
        // No journal entries — check_balance should pass
        Spi::run("SELECT pgledger.check_balance()")?;
        Ok(())
    }

    // =========================================================================
    // Phase 4: Rules engine + trigger auto-installation tests
    // =========================================================================

    #[pg_test]
    fn test_create_rule_returns_id() -> Result<(), spi::Error> {
        set_search_path();
        // Create a test table
        Spi::run(
            "CREATE TABLE public.invoices (id serial, customer_id text, amount numeric(19,4))",
        )?;

        let rule_id = Spi::get_one::<i32>(
            r#"SELECT pgledger.create_rule(
                'public.invoices',
                'amount',
                '[{"side":"debit","account":"ar:{customer_id}"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )
        .unwrap()
        .unwrap();
        assert!(rule_id > 0);
        Ok(())
    }

    #[pg_test]
    #[should_panic(expected = "debit and one credit")]
    fn test_create_rule_requires_both_sides() {
        set_search_path();
        Spi::run("CREATE TABLE public.inv2 (id serial, amount numeric)").unwrap();
        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.inv2',
                'amount',
                '[{"side":"debit","account":"cash"}]'::jsonb
            )"#,
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "missing 'side'")]
    fn test_create_rule_validates_entry_format() {
        set_search_path();
        Spi::run("CREATE TABLE public.inv3 (id serial, amount numeric)").unwrap();
        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.inv3',
                'amount',
                '[{"account":"cash"}]'::jsonb
            )"#,
        )
        .unwrap();
    }

    #[pg_test]
    fn test_drop_rule() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.inv4 (id serial, amount numeric)")?;

        let rule_id = Spi::get_one::<i32>(
            r#"SELECT pgledger.create_rule(
                'public.inv4',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )
        .unwrap()
        .unwrap();

        Spi::run_with_args("SELECT pgledger.drop_rule($1)", &[rule_id.into()])?;

        let count = Spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM pgledger.rule WHERE id = $1",
            &[rule_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);
        Ok(())
    }

    #[pg_test]
    fn test_disable_enable_rule() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.inv5 (id serial, amount numeric)")?;

        let rule_id = Spi::get_one::<i32>(
            r#"SELECT pgledger.create_rule(
                'public.inv5',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )
        .unwrap()
        .unwrap();

        Spi::run_with_args("SELECT pgledger.disable_rule($1)", &[rule_id.into()])?;

        let active = Spi::get_one_with_args::<bool>(
            "SELECT active FROM pgledger.rule WHERE id = $1",
            &[rule_id.into()],
        )
        .unwrap()
        .unwrap();
        assert!(!active);

        Spi::run_with_args("SELECT pgledger.enable_rule($1)", &[rule_id.into()])?;

        let active = Spi::get_one_with_args::<bool>(
            "SELECT active FROM pgledger.rule WHERE id = $1",
            &[rule_id.into()],
        )
        .unwrap()
        .unwrap();
        assert!(active);
        Ok(())
    }

    #[pg_test]
    fn test_trigger_installed_on_create_rule() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.inv6 (id serial, customer_id text, amount numeric(19,4))")?;

        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.inv6',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )?;

        // Verify trigger exists
        let trigger_exists = Spi::get_one::<bool>(
            r#"SELECT EXISTS(
                SELECT 1 FROM pg_trigger t
                JOIN pg_class c ON t.tgrelid = c.oid
                WHERE c.relname = 'inv6'
                AND t.tgname LIKE 'pgledger_%'
            )"#,
        )
        .unwrap()
        .unwrap();
        assert!(trigger_exists);
        Ok(())
    }

    #[pg_test]
    fn test_insert_creates_journal_entries() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.orders (id serial, customer_id text, amount numeric(19,4))")?;

        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.orders',
                'amount',
                '[{"side":"debit","account":"ar:{customer_id}"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )?;

        // Insert a row — trigger should fire and create journal entries
        Spi::run("INSERT INTO public.orders (customer_id, amount) VALUES ('CUST-42', 500.0000)")?;

        // Check journal entries were created
        let entry_count = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(entry_count, 2); // one debit, one credit

        // Verify debit account has template resolved
        let debit_account = Spi::get_one::<String>(
            "SELECT account FROM pgledger.journal_entry WHERE debit > 0 ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(debit_account, "ar:CUST-42");

        // Verify credit account
        let credit_account = Spi::get_one::<String>(
            "SELECT account FROM pgledger.journal_entry WHERE credit > 0 ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(credit_account, "revenue");

        // Verify amounts
        let debit_amt = Spi::get_one::<f64>(
            "SELECT debit::float8 FROM pgledger.journal_entry WHERE debit > 0 ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert!((debit_amt - 500.0).abs() < 0.001);

        Ok(())
    }

    #[pg_test]
    fn test_delete_creates_reversal() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.sales (id serial, customer_id text, amount numeric(19,4))")?;

        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.sales',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )?;

        Spi::run("INSERT INTO public.sales (customer_id, amount) VALUES ('C1', 200.0000)")?;

        // Now delete — should create reversal entries (swapped debit/credit)
        Spi::run("DELETE FROM public.sales WHERE customer_id = 'C1'")?;

        // Should have 4 entries total: 2 from insert, 2 from delete reversal
        let entry_count = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(entry_count, 4);

        // The reversal should net to zero
        let total_debits = Spi::get_one::<f64>(
            "SELECT COALESCE(SUM(debit), 0)::float8 FROM pgledger.journal_entry",
        )
        .unwrap()
        .unwrap();
        let total_credits = Spi::get_one::<f64>(
            "SELECT COALESCE(SUM(credit), 0)::float8 FROM pgledger.journal_entry",
        )
        .unwrap()
        .unwrap();
        assert!((total_debits - total_credits).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_update_reverses_and_reapplies() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.items (id serial, customer_id text, amount numeric(19,4))")?;

        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.items',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )?;

        Spi::run("INSERT INTO public.items (customer_id, amount) VALUES ('C1', 100.0000)")?;

        // Update amount from 100 to 150
        Spi::run("UPDATE public.items SET amount = 150.0000 WHERE customer_id = 'C1'")?;

        // Should have 6 entries: 2 insert + 2 reversal of old + 2 new
        let entry_count = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(entry_count, 6);

        // Net debit should equal net credit (balanced)
        let total_debits = Spi::get_one::<f64>(
            "SELECT COALESCE(SUM(debit), 0)::float8 FROM pgledger.journal_entry",
        )
        .unwrap()
        .unwrap();
        let total_credits = Spi::get_one::<f64>(
            "SELECT COALESCE(SUM(credit), 0)::float8 FROM pgledger.journal_entry",
        )
        .unwrap()
        .unwrap();
        assert!((total_debits - total_credits).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_conditional_rule() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.payments (id serial, ptype text, amount numeric(19,4))")?;

        // Rule only applies when ptype = 'cash'
        Spi::run(
            r#"SELECT pgledger.create_rule(
                'public.payments',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb,
                '{ptype} = ''cash'''
            )"#,
        )?;

        // Insert with ptype='cash' — should create entries
        Spi::run("INSERT INTO public.payments (ptype, amount) VALUES ('cash', 100.0000)")?;

        let count_after_cash = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(count_after_cash, 2);

        // Insert with ptype='credit_card' — should NOT create entries (condition fails)
        Spi::run("INSERT INTO public.payments (ptype, amount) VALUES ('credit_card', 200.0000)")?;

        let count_after_cc = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(count_after_cc, 2); // same as before — no new entries
        Ok(())
    }

    #[pg_test]
    fn test_disabled_rule_not_applied() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("CREATE TABLE public.inv7 (id serial, amount numeric(19,4))")?;

        let rule_id = Spi::get_one::<i32>(
            r#"SELECT pgledger.create_rule(
                'public.inv7',
                'amount',
                '[{"side":"debit","account":"cash"},{"side":"credit","account":"revenue"}]'::jsonb
            )"#,
        )
        .unwrap()
        .unwrap();

        // Disable the rule
        Spi::run_with_args("SELECT pgledger.disable_rule($1)", &[rule_id.into()])?;

        // Insert — should NOT create journal entries (rule disabled)
        Spi::run("INSERT INTO public.inv7 (amount) VALUES (100.0000)")?;

        let count = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entry")
            .unwrap()
            .unwrap();
        assert_eq!(count, 0);
        Ok(())
    }

    // =========================================================================
    // Phase 5: Queries, fiscal periods, currency tests
    // =========================================================================

    #[pg_test]
    fn test_account_balance() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;
        Spi::run("SELECT pgledger.debit('cash', 50.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 50.0)")?;

        let balance = Spi::get_one::<f64>("SELECT pgledger.account_balance('cash')::float8")
            .unwrap()
            .unwrap();
        assert!((balance - 150.0).abs() < 0.001);

        let rev_balance = Spi::get_one::<f64>("SELECT pgledger.account_balance('revenue')::float8")
            .unwrap()
            .unwrap();
        // Revenue has only credits, so balance = 0 - 150 = -150
        assert!((rev_balance - (-150.0)).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_trial_balance_nets_to_zero() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;

        let net = Spi::get_one::<f64>(
            "SELECT COALESCE(SUM(balance), 0)::float8 FROM pgledger.trial_balance()",
        )
        .unwrap()
        .unwrap();
        assert!((net).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    fn test_trial_balance_account_filter() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('ar:cust1', 100.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;
        Spi::run("SELECT pgledger.debit('ar:cust2', 200.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 200.0)")?;

        let count =
            Spi::get_one::<i64>("SELECT count(*) FROM pgledger.trial_balance(NULL, 'ar:%')")
                .unwrap()
                .unwrap();
        assert_eq!(count, 2); // ar:cust1 and ar:cust2
        Ok(())
    }

    #[pg_test]
    fn test_journal_entries_returns_data() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0, 'test entry')")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;

        let count = Spi::get_one::<i64>("SELECT count(*) FROM pgledger.journal_entries()")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);
        Ok(())
    }

    #[pg_test]
    fn test_journal_entries_filter_by_account() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.debit('cash', 100.0)")?;
        Spi::run("SELECT pgledger.credit('revenue', 100.0)")?;

        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgledger.journal_entries(NULL, NULL, 'cash')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1);
        Ok(())
    }

    #[pg_test]
    fn test_open_close_period() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.open_period('2024-01')")?;

        let status = Spi::get_one::<String>(
            "SELECT status FROM pgledger.period_status() WHERE period = '2024-01'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "open");

        Spi::run("SELECT pgledger.close_period('2024-01')")?;

        let status = Spi::get_one::<String>(
            "SELECT status FROM pgledger.period_status() WHERE period = '2024-01'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "closed");
        Ok(())
    }

    #[pg_test]
    fn test_reopen_period() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.open_period('2024-02')")?;
        Spi::run("SELECT pgledger.close_period('2024-02')")?;
        Spi::run("SELECT pgledger.open_period('2024-02')")?;

        let status = Spi::get_one::<String>(
            "SELECT status FROM pgledger.period_status() WHERE period = '2024-02'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(status, "open");
        Ok(())
    }

    #[pg_test]
    fn test_set_exchange_rate() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.set_exchange_rate('USD', 'EUR', 0.85)")?;

        let rate = Spi::get_one::<f64>(
            "SELECT rate::float8 FROM pgledger.exchange_rate WHERE from_currency = 'USD' AND to_currency = 'EUR'",
        )
        .unwrap()
        .unwrap();
        assert!((rate - 0.85).abs() < 0.0001);
        Ok(())
    }

    #[pg_test]
    fn test_convert_currency() -> Result<(), spi::Error> {
        set_search_path();
        Spi::run("SELECT pgledger.set_exchange_rate('USD', 'EUR', 0.85)")?;

        let converted = Spi::get_one::<f64>("SELECT pgledger.convert(100.0, 'USD', 'EUR')")
            .unwrap()
            .unwrap();
        assert!((converted - 85.0).abs() < 0.01);
        Ok(())
    }

    #[pg_test]
    fn test_convert_same_currency() -> Result<(), spi::Error> {
        set_search_path();
        let converted = Spi::get_one::<f64>("SELECT pgledger.convert(100.0, 'USD', 'USD')")
            .unwrap()
            .unwrap();
        assert!((converted - 100.0).abs() < 0.001);
        Ok(())
    }

    #[pg_test]
    #[should_panic(expected = "no exchange rate found")]
    fn test_convert_missing_rate() {
        set_search_path();
        Spi::get_one::<f64>("SELECT pgledger.convert(100.0, 'USD', 'JPY')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "positive")]
    fn test_set_negative_exchange_rate() {
        set_search_path();
        Spi::run("SELECT pgledger.set_exchange_rate('USD', 'EUR', -0.85)").unwrap();
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
