mod definition;
pub mod format;
mod generate;
mod queries;
mod schema;
mod seed;

pub use definition::*;
pub use generate::*;
pub use queries::*;
pub use seed::*;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings
// =============================================================================

pub static PG_SEQUENCE_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_sequence.enabled",
        c"Enable pg_sequence document numbering functions",
        c"When false, sequence generation functions are disabled.",
        &PG_SEQUENCE_ENABLED,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::log!("pg_sequence: initialized");
}

// =============================================================================
// Integration Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn set_search_path() {
        Spi::run("SET search_path TO pgsequence, public").unwrap();
    }

    // =========================================================================
    // Phase 1: Schema validation
    // =========================================================================

    #[pg_test]
    fn test_sequence_def_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgsequence' AND table_name = 'sequence_def')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_counter_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgsequence' AND table_name = 'counter')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_sequence_def_has_expected_columns() {
        set_search_path();
        let count = Spi::get_one::<i64>(
            r#"SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'pgsequence' AND table_name = 'sequence_def'"#,
        )
        .unwrap()
        .unwrap();
        assert!(count >= 11); // id, name, prefix, suffix, format, counter_pad, start_value, increment, reset_policy, gap_free, description, created_at
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_counter_unique_constraint() {
        set_search_path();
        let id = Spi::get_one::<i32>(
            "INSERT INTO pgsequence.sequence_def (name) VALUES ('test_uniq') RETURNING id",
        )
        .unwrap()
        .unwrap();
        Spi::run_with_args(
            "INSERT INTO pgsequence.counter (sequence_id, scope_key, period_key, current_value) VALUES ($1, '', '', 1)",
            &[id.into()],
        )
        .unwrap();
        // Second insert with same composite key should panic
        Spi::run_with_args(
            "INSERT INTO pgsequence.counter (sequence_id, scope_key, period_key, current_value) VALUES ($1, '', '', 2)",
            &[id.into()],
        )
        .unwrap();
    }

    // =========================================================================
    // Phase 2: CRUD — create_sequence
    // =========================================================================

    #[pg_test]
    fn test_create_sequence_basic() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgsequence.create_sequence('test_basic')")
            .unwrap()
            .unwrap();
        assert!(id > 0);
    }

    #[pg_test]
    fn test_create_sequence_with_all_params() {
        set_search_path();
        let id = Spi::get_one::<i32>(
            "SELECT pgsequence.create_sequence('test_full', 'INV-', '-END', '{prefix}{year}-{counter}{suffix}', 5, 100, 2, 'yearly', true, 'Full test')",
        )
        .unwrap()
        .unwrap();
        assert!(id > 0);

        // Verify stored values
        let prefix = Spi::get_one_with_args::<String>(
            "SELECT prefix FROM pgsequence.sequence_def WHERE id = $1",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(prefix, "INV-");

        let pad = Spi::get_one_with_args::<i32>(
            "SELECT counter_pad FROM pgsequence.sequence_def WHERE id = $1",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(pad, 5);
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_create_sequence_duplicate_error() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('dup_seq')").unwrap();
        Spi::run("SELECT pgsequence.create_sequence('dup_seq')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "invalid reset_policy")]
    fn test_create_sequence_invalid_policy() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('bad_policy', '', '', '{counter}', 1, 1, 1, 'weekly')").unwrap();
    }

    // =========================================================================
    // Phase 2: CRUD — alter_sequence
    // =========================================================================

    #[pg_test]
    fn test_alter_sequence_prefix() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('alter_test', 'OLD-')").unwrap();
        Spi::run("SELECT pgsequence.alter_sequence('alter_test', 'NEW-')").unwrap();
        let prefix = Spi::get_one::<String>(
            "SELECT prefix FROM pgsequence.sequence_def WHERE name = 'alter_test'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(prefix, "NEW-");
    }

    // =========================================================================
    // Phase 2: CRUD — drop_sequence
    // =========================================================================

    #[pg_test]
    fn test_drop_sequence() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('to_drop')").unwrap();
        Spi::run("SELECT pgsequence.drop_sequence('to_drop')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgsequence.sequence_def WHERE name = 'to_drop')",
        )
        .unwrap()
        .unwrap();
        assert!(!exists);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_drop_nonexistent_error() {
        set_search_path();
        Spi::run("SELECT pgsequence.drop_sequence('no_such_seq')").unwrap();
    }

    // =========================================================================
    // Phase 2: Generation — next_val
    // =========================================================================

    #[pg_test]
    fn test_next_val_basic() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('nv_test')").unwrap();
        let v1 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, 1);
        let v2 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v2, 2);
    }

    #[pg_test]
    fn test_next_val_custom_start() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('nv_start', '', '', '{counter}', 1, 100)")
            .unwrap();
        let v1 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_start')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, 100);
        let v2 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_start')")
            .unwrap()
            .unwrap();
        assert_eq!(v2, 101);
    }

    #[pg_test]
    fn test_next_val_custom_increment() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('nv_inc', '', '', '{counter}', 1, 1, 5)")
            .unwrap();
        let v1 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_inc')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, 1);
        let v2 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_inc')")
            .unwrap()
            .unwrap();
        assert_eq!(v2, 6);
        let v3 = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_inc')")
            .unwrap()
            .unwrap();
        assert_eq!(v3, 11);
    }

    #[pg_test]
    fn test_next_val_multiple_calls() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('nv_multi')").unwrap();
        for expected in 1..=10 {
            let v = Spi::get_one::<i64>("SELECT pgsequence.next_val('nv_multi')")
                .unwrap()
                .unwrap();
            assert_eq!(v, expected);
        }
    }

    // =========================================================================
    // Phase 2: Generation — next_formatted
    // =========================================================================

    #[pg_test]
    fn test_next_formatted_basic() {
        set_search_path();
        Spi::run(
            "SELECT pgsequence.create_sequence('nf_test', 'INV-', '', '{prefix}{counter}', 5)",
        )
        .unwrap();
        let v1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('nf_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, "INV-00001");
    }

    #[pg_test]
    fn test_next_formatted_with_year() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('nf_year', 'INV-', '', '{prefix}{year}-{counter}', 5, 1, 1, 'yearly')")
            .unwrap();
        let v1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('nf_year')")
            .unwrap()
            .unwrap();
        // Should match INV-YYYY-00001 pattern
        let year = Spi::get_one::<String>("SELECT to_char(now(), 'YYYY')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, format!("INV-{}-00001", year));
    }

    // =========================================================================
    // Phase 2: Generation — current_val
    // =========================================================================

    #[pg_test]
    fn test_current_val_before_any_generation() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('cv_test')").unwrap();
        let v = Spi::get_one::<i64>("SELECT pgsequence.current_val('cv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 0); // No counter row yet
    }

    #[pg_test]
    fn test_current_val_after_generation() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('cv_after')").unwrap();
        Spi::run("SELECT pgsequence.next_val('cv_after')").unwrap();
        Spi::run("SELECT pgsequence.next_val('cv_after')").unwrap();
        let v = Spi::get_one::<i64>("SELECT pgsequence.current_val('cv_after')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 2);
    }

    // =========================================================================
    // Phase 2: Generation — preview
    // =========================================================================

    #[pg_test]
    fn test_preview_does_not_increment() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('pv_test', 'X-', '', '{prefix}{counter}', 3)")
            .unwrap();

        let p1 = Spi::get_one::<String>("SELECT pgsequence.preview('pv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(p1, "X-001"); // Preview shows what next would be (start_value=1)

        let p2 = Spi::get_one::<String>("SELECT pgsequence.preview('pv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(p2, "X-001"); // Still the same — no increment

        // Now actually generate
        let v1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('pv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, "X-001"); // First actual generation

        let p3 = Spi::get_one::<String>("SELECT pgsequence.preview('pv_test')")
            .unwrap()
            .unwrap();
        assert_eq!(p3, "X-002"); // Preview now shows next
    }

    // =========================================================================
    // Phase 2: Generation — reset_counter
    // =========================================================================

    #[pg_test]
    fn test_reset_counter() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('rc_test')").unwrap();
        Spi::run("SELECT pgsequence.next_val('rc_test')").unwrap();
        Spi::run("SELECT pgsequence.next_val('rc_test')").unwrap();
        Spi::run("SELECT pgsequence.next_val('rc_test')").unwrap();

        let v = Spi::get_one::<i64>("SELECT pgsequence.current_val('rc_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 3);

        // Reset to 0 (delete counter row)
        Spi::run("SELECT pgsequence.reset_counter('rc_test')").unwrap();
        let v = Spi::get_one::<i64>("SELECT pgsequence.current_val('rc_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 0);

        // Next val should start fresh
        let v = Spi::get_one::<i64>("SELECT pgsequence.next_val('rc_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 1);
    }

    #[pg_test]
    fn test_reset_counter_to_specific_value() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('rc_val')").unwrap();
        Spi::run("SELECT pgsequence.next_val('rc_val')").unwrap();

        // Reset to specific value
        Spi::run("SELECT pgsequence.reset_counter('rc_val', '', 500)").unwrap();
        let v = Spi::get_one::<i64>("SELECT pgsequence.current_val('rc_val')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 500);

        // Next val increments from 500
        let v = Spi::get_one::<i64>("SELECT pgsequence.next_val('rc_val')")
            .unwrap()
            .unwrap();
        assert_eq!(v, 501);
    }

    // =========================================================================
    // Phase 2: Scoped counters
    // =========================================================================

    #[pg_test]
    fn test_scoped_counters_independent() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('scope_test')").unwrap();

        let v1 = Spi::get_one::<i64>("SELECT pgsequence.next_val('scope_test', 'WH-A')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, 1);

        let v2 = Spi::get_one::<i64>("SELECT pgsequence.next_val('scope_test', 'WH-B')")
            .unwrap()
            .unwrap();
        assert_eq!(v2, 1); // Independent counter

        let v3 = Spi::get_one::<i64>("SELECT pgsequence.next_val('scope_test', 'WH-A')")
            .unwrap()
            .unwrap();
        assert_eq!(v3, 2); // WH-A continues from 1

        let v4 = Spi::get_one::<i64>("SELECT pgsequence.next_val('scope_test', 'WH-B')")
            .unwrap()
            .unwrap();
        assert_eq!(v4, 2); // WH-B continues from 1
    }

    #[pg_test]
    fn test_scoped_formatted() {
        set_search_path();
        Spi::run(
            "SELECT pgsequence.create_sequence('scope_fmt', '', '', '{scope}/DO-{counter}', 4)",
        )
        .unwrap();

        let v1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('scope_fmt', 'WH-A')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, "WH-A/DO-0001");
    }

    // =========================================================================
    // Phase 2: Period reset
    // =========================================================================

    #[pg_test]
    fn test_yearly_reset_different_periods() {
        set_search_path();
        Spi::run(
            "SELECT pgsequence.create_sequence('yr_test', '', '', '{counter}', 1, 1, 1, 'yearly')",
        )
        .unwrap();

        // Generate a value (creates counter for current year)
        let v1 = Spi::get_one::<i64>("SELECT pgsequence.next_val('yr_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, 1);

        // Simulate a different year by directly inserting a counter
        let seq_id =
            Spi::get_one::<i32>("SELECT id FROM pgsequence.sequence_def WHERE name = 'yr_test'")
                .unwrap()
                .unwrap();
        Spi::run_with_args(
            "INSERT INTO pgsequence.counter (sequence_id, scope_key, period_key, current_value) VALUES ($1, '', '2099', 99)",
            &[seq_id.into()],
        )
        .unwrap();

        // The 2099 counter should be independent
        let v = Spi::get_one_with_args::<i64>(
            "SELECT current_value FROM pgsequence.counter WHERE sequence_id = $1 AND period_key = '2099'",
            &[seq_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(v, 99);
    }

    // =========================================================================
    // Phase 3: Seeds
    // =========================================================================

    #[pg_test]
    fn test_seed_common_creates_sequences() {
        set_search_path();
        Spi::run("SELECT pgsequence.seed_common()").unwrap();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsequence.sequence_def")
            .unwrap()
            .unwrap();
        assert_eq!(count, 5);
    }

    #[pg_test]
    fn test_seed_common_idempotent() {
        set_search_path();
        Spi::run("SELECT pgsequence.seed_common()").unwrap();
        Spi::run("SELECT pgsequence.seed_common()").unwrap(); // Should not fail
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsequence.sequence_def")
            .unwrap()
            .unwrap();
        assert_eq!(count, 5);
    }

    #[pg_test]
    fn test_seed_invoice_sequence() {
        set_search_path();
        Spi::run("SELECT pgsequence.seed_common()").unwrap();
        let v1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('invoice')")
            .unwrap()
            .unwrap();
        let year = Spi::get_one::<String>("SELECT to_char(now(), 'YYYY')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, format!("INV-{}-00001", year));
    }

    // =========================================================================
    // Phase 3: Queries
    // =========================================================================

    #[pg_test]
    fn test_list_sequences() {
        set_search_path();
        Spi::run("SELECT pgsequence.seed_common()").unwrap();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsequence.list_sequences()")
            .unwrap()
            .unwrap();
        assert_eq!(count, 5);
    }

    #[pg_test]
    fn test_sequence_info() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('info_test', 'X-', '-Y', '{prefix}{counter}{suffix}', 3, 10, 2, 'monthly', true, 'Info test')")
            .unwrap();
        let name = Spi::get_one::<String>("SELECT name FROM pgsequence.sequence_info('info_test')")
            .unwrap()
            .unwrap();
        assert_eq!(name, "info_test");

        let gap_free =
            Spi::get_one::<bool>("SELECT gap_free FROM pgsequence.sequence_info('info_test')")
                .unwrap()
                .unwrap();
        assert!(gap_free);
    }

    #[pg_test]
    fn test_list_counters() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('lc_test')").unwrap();
        Spi::run("SELECT pgsequence.next_val('lc_test', 'A')").unwrap();
        Spi::run("SELECT pgsequence.next_val('lc_test', 'B')").unwrap();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsequence.list_counters('lc_test')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);
    }

    // =========================================================================
    // Phase 3: Full workflow
    // =========================================================================

    #[pg_test]
    fn test_full_erp_workflow() {
        set_search_path();
        Spi::run("SELECT pgsequence.seed_common()").unwrap();

        let year = Spi::get_one::<String>("SELECT to_char(now(), 'YYYY')")
            .unwrap()
            .unwrap();

        // Generate invoice numbers
        let inv1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('invoice')")
            .unwrap()
            .unwrap();
        let inv2 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('invoice')")
            .unwrap()
            .unwrap();
        assert_eq!(inv1, format!("INV-{}-00001", year));
        assert_eq!(inv2, format!("INV-{}-00002", year));

        // Generate delivery orders (never reset, no year)
        let do1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('delivery_order')")
            .unwrap()
            .unwrap();
        assert_eq!(do1, "DO-000001");

        // Generate PO for different scopes
        let po1 =
            Spi::get_one::<String>("SELECT pgsequence.next_formatted('purchase_order', 'DEPT-A')")
                .unwrap()
                .unwrap();
        let po2 =
            Spi::get_one::<String>("SELECT pgsequence.next_formatted('purchase_order', 'DEPT-B')")
                .unwrap()
                .unwrap();
        // Both should be 00001 for their scope
        assert!(po1.contains("00001"));
        assert!(po2.contains("00001"));

        // Verify counter info
        let counter_count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsequence.list_counters('purchase_order')")
                .unwrap()
                .unwrap();
        assert_eq!(counter_count, 2); // Two scopes

        // Preview and current
        let curr = Spi::get_one::<i64>("SELECT pgsequence.current_val('invoice')")
            .unwrap()
            .unwrap();
        assert_eq!(curr, 2);

        let preview = Spi::get_one::<String>("SELECT pgsequence.preview('invoice')")
            .unwrap()
            .unwrap();
        assert_eq!(preview, format!("INV-{}-00003", year));

        // Actual next should match preview
        let inv3 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('invoice')")
            .unwrap()
            .unwrap();
        assert_eq!(inv3, format!("INV-{}-00003", year));
    }

    #[pg_test]
    fn test_alter_then_generate() {
        set_search_path();
        Spi::run(
            "SELECT pgsequence.create_sequence('atg_test', 'OLD-', '', '{prefix}{counter}', 3)",
        )
        .unwrap();
        let v1 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('atg_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v1, "OLD-001");

        // Alter prefix
        Spi::run("SELECT pgsequence.alter_sequence('atg_test', 'NEW-')").unwrap();
        let v2 = Spi::get_one::<String>("SELECT pgsequence.next_formatted('atg_test')")
            .unwrap()
            .unwrap();
        assert_eq!(v2, "NEW-002"); // Counter continues, format changes
    }

    #[pg_test]
    fn test_drop_cascade_removes_counters() {
        set_search_path();
        Spi::run("SELECT pgsequence.create_sequence('cascade_test')").unwrap();
        Spi::run("SELECT pgsequence.next_val('cascade_test')").unwrap();
        Spi::run("SELECT pgsequence.next_val('cascade_test', 'scope1')").unwrap();

        let before = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsequence.counter")
            .unwrap()
            .unwrap();
        assert!(before >= 2);

        Spi::run("SELECT pgsequence.drop_sequence('cascade_test')").unwrap();

        // Counters should be gone via CASCADE
        let after = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgsequence.counter c JOIN pgsequence.sequence_def s ON c.sequence_id = s.id WHERE s.name = 'cascade_test'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(after, 0);
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
