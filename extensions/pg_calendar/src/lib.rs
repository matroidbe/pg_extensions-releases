mod calendar;
mod compute;
mod exception;
mod holiday;
mod pattern;
mod queries;
mod schema;
mod seed;

pub use calendar::*;
pub use compute::*;
pub use exception::*;
pub use holiday::*;
pub use pattern::*;
pub use queries::*;
pub use seed::*;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings
// =============================================================================

pub static PG_CALENDAR_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_calendar.enabled",
        c"Enable pg_calendar functions",
        c"When false, calendar functions are disabled.",
        &PG_CALENDAR_ENABLED,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::log!("pg_calendar: initialized");
}

// =============================================================================
// Integration Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn set_search_path() {
        Spi::run("SET search_path TO pgcalendar, public").unwrap();
    }

    fn create_iso() {
        set_search_path();
        Spi::run("SELECT pgcalendar.seed_iso()").unwrap();
    }

    // =========================================================================
    // Phase 1: Schema validation
    // =========================================================================

    #[pg_test]
    fn test_calendar_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgcalendar' AND table_name = 'calendar')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_weekly_pattern_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgcalendar' AND table_name = 'weekly_pattern')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_holiday_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgcalendar' AND table_name = 'holiday')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    #[pg_test]
    fn test_exception_table_exists() {
        set_search_path();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgcalendar' AND table_name = 'exception')",
        )
        .unwrap()
        .unwrap();
        assert!(exists);
    }

    // =========================================================================
    // Phase 1: Calendar CRUD
    // =========================================================================

    #[pg_test]
    fn test_create_calendar_returns_id() {
        set_search_path();
        let id =
            Spi::get_one::<i32>("SELECT pgcalendar.create_calendar('test_cal', 'A test calendar')")
                .unwrap()
                .unwrap();
        assert!(id > 0);
    }

    #[pg_test]
    fn test_create_calendar_initializes_7_weekdays() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgcalendar.create_calendar('init_test')")
            .unwrap()
            .unwrap();

        let count = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern WHERE calendar_id = $1",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 7);
    }

    #[pg_test]
    fn test_create_calendar_all_days_non_working() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgcalendar.create_calendar('non_working_test')")
            .unwrap()
            .unwrap();

        let working = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern WHERE calendar_id = $1 AND is_working = true",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(working, 0);
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_create_calendar_duplicate_error() {
        set_search_path();
        Spi::run("SELECT pgcalendar.create_calendar('dup_cal')").unwrap();
        Spi::run("SELECT pgcalendar.create_calendar('dup_cal')").unwrap();
    }

    #[pg_test]
    fn test_drop_calendar() {
        set_search_path();
        Spi::run("SELECT pgcalendar.create_calendar('to_drop')").unwrap();
        Spi::run("SELECT pgcalendar.drop_calendar('to_drop')").unwrap();
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgcalendar.calendar WHERE name = 'to_drop')",
        )
        .unwrap()
        .unwrap();
        assert!(!exists);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_drop_calendar_nonexistent_error() {
        set_search_path();
        Spi::run("SELECT pgcalendar.drop_calendar('no_such')").unwrap();
    }

    #[pg_test]
    fn test_drop_calendar_cascades() {
        set_search_path();
        Spi::run("SELECT pgcalendar.create_calendar('cascade_test')").unwrap();
        Spi::run("SELECT pgcalendar.set_working_days('cascade_test', '1,2,3,4,5')").unwrap();
        Spi::run("SELECT pgcalendar.drop_calendar('cascade_test')").unwrap();
        // Weekly pattern rows should also be gone
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern wp JOIN pgcalendar.calendar c ON wp.calendar_id = c.id WHERE c.name = 'cascade_test'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);
    }

    // =========================================================================
    // Phase 1: Clone calendar
    // =========================================================================

    #[pg_test]
    fn test_clone_calendar() {
        create_iso();
        let new_id = Spi::get_one::<i32>(
            "SELECT pgcalendar.clone_calendar('iso', 'iso_clone', 'Cloned ISO')",
        )
        .unwrap()
        .unwrap();
        assert!(new_id > 0);

        // Check working days match
        let working = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern WHERE calendar_id = $1 AND is_working = true",
            &[new_id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(working, 5); // Mon-Fri
    }

    // =========================================================================
    // Phase 2: Weekly pattern
    // =========================================================================

    #[pg_test]
    fn test_set_weekday() {
        set_search_path();
        Spi::run("SELECT pgcalendar.create_calendar('weekday_test')").unwrap();
        // Set Monday (1) as working
        Spi::run("SELECT pgcalendar.set_weekday('weekday_test', 1, true)").unwrap();

        let is_working = Spi::get_one::<bool>(
            "SELECT is_working FROM pgcalendar.weekly_pattern wp JOIN pgcalendar.calendar c ON wp.calendar_id = c.id WHERE c.name = 'weekday_test' AND day_of_week = 1",
        )
        .unwrap()
        .unwrap();
        assert!(is_working);
    }

    #[pg_test]
    #[should_panic(expected = "must be 0-6")]
    fn test_set_weekday_invalid_day() {
        set_search_path();
        Spi::run("SELECT pgcalendar.create_calendar('bad_day')").unwrap();
        Spi::run("SELECT pgcalendar.set_weekday('bad_day', 7, true)").unwrap();
    }

    #[pg_test]
    fn test_set_working_days_mon_fri() {
        set_search_path();
        Spi::run("SELECT pgcalendar.create_calendar('work_days_test')").unwrap();
        Spi::run("SELECT pgcalendar.set_working_days('work_days_test', '1,2,3,4,5')").unwrap();

        let working = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern wp JOIN pgcalendar.calendar c ON wp.calendar_id = c.id WHERE c.name = 'work_days_test' AND is_working = true",
        )
        .unwrap()
        .unwrap();
        assert_eq!(working, 5);
    }

    #[pg_test]
    fn test_weekly_pattern_output() {
        create_iso();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgcalendar.weekly_pattern('iso')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 7);
    }

    // =========================================================================
    // Phase 2: Holidays
    // =========================================================================

    #[pg_test]
    fn test_add_holiday() {
        create_iso();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Christmas', '2025-12-25'::date, true)")
            .unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.holiday h JOIN pgcalendar.calendar c ON h.calendar_id = c.id WHERE c.name = 'iso'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1);
    }

    #[pg_test]
    fn test_remove_holiday() {
        create_iso();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Test Holiday', '2025-07-04'::date)")
            .unwrap();
        Spi::run("SELECT pgcalendar.remove_holiday('iso', 'Test Holiday')").unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.holiday h JOIN pgcalendar.calendar c ON h.calendar_id = c.id WHERE c.name = 'iso'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_remove_holiday_nonexistent() {
        create_iso();
        Spi::run("SELECT pgcalendar.remove_holiday('iso', 'Nonexistent')").unwrap();
    }

    #[pg_test]
    fn test_list_holidays() {
        create_iso();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'New Year', '2025-01-01'::date, true)")
            .unwrap();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Christmas', '2025-12-25'::date, true)")
            .unwrap();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgcalendar.list_holidays('iso')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 2);
    }

    #[pg_test]
    fn test_list_holidays_by_year() {
        create_iso();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'H1', '2025-01-01'::date)").unwrap();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'H2', '2026-01-01'::date)").unwrap();
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pgcalendar.list_holidays('iso', 2025)")
                .unwrap()
                .unwrap();
        assert_eq!(count, 1);
    }

    // =========================================================================
    // Phase 2: Exceptions
    // =========================================================================

    #[pg_test]
    fn test_add_exception() {
        create_iso();
        // Make a Saturday working
        Spi::run(
            "SELECT pgcalendar.add_exception('iso', '2025-03-15'::date, true, 'Special Saturday')",
        )
        .unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.exception e JOIN pgcalendar.calendar c ON e.calendar_id = c.id WHERE c.name = 'iso'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1);
    }

    #[pg_test]
    fn test_add_exception_upsert() {
        create_iso();
        Spi::run(
            "SELECT pgcalendar.add_exception('iso', '2025-03-15'::date, true, 'First reason')",
        )
        .unwrap();
        // Update same date
        Spi::run("SELECT pgcalendar.add_exception('iso', '2025-03-15'::date, false, 'New reason')")
            .unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.exception e JOIN pgcalendar.calendar c ON e.calendar_id = c.id WHERE c.name = 'iso'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1); // Still 1 row, not 2
    }

    #[pg_test]
    fn test_remove_exception() {
        create_iso();
        Spi::run("SELECT pgcalendar.add_exception('iso', '2025-03-15'::date, true)").unwrap();
        Spi::run("SELECT pgcalendar.remove_exception('iso', '2025-03-15'::date)").unwrap();
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.exception e JOIN pgcalendar.calendar c ON e.calendar_id = c.id WHERE c.name = 'iso'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);
    }

    // =========================================================================
    // Phase 3: Core computation — is_working_day
    // =========================================================================

    #[pg_test]
    fn test_is_working_day_weekday() {
        create_iso();
        // 2025-03-10 is a Monday
        let result =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('iso', '2025-03-10'::date)")
                .unwrap()
                .unwrap();
        assert!(result);
    }

    #[pg_test]
    fn test_is_working_day_weekend() {
        create_iso();
        // 2025-03-15 is a Saturday
        let result =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('iso', '2025-03-15'::date)")
                .unwrap()
                .unwrap();
        assert!(!result);
    }

    #[pg_test]
    fn test_is_working_day_holiday_override() {
        create_iso();
        // 2025-03-10 is Monday (working), add as holiday
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Test', '2025-03-10'::date)").unwrap();
        let result =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('iso', '2025-03-10'::date)")
                .unwrap()
                .unwrap();
        assert!(!result); // Holiday overrides weekly pattern
    }

    #[pg_test]
    fn test_is_working_day_exception_overrides_holiday() {
        create_iso();
        // Add holiday on Monday
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Holiday', '2025-03-10'::date)").unwrap();
        // Add exception making it working despite holiday
        Spi::run("SELECT pgcalendar.add_exception('iso', '2025-03-10'::date, true, 'Override')")
            .unwrap();
        let result =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('iso', '2025-03-10'::date)")
                .unwrap()
                .unwrap();
        assert!(result); // Exception wins over holiday
    }

    #[pg_test]
    fn test_is_working_day_exception_overrides_weekend() {
        create_iso();
        // Make Saturday working via exception
        Spi::run("SELECT pgcalendar.add_exception('iso', '2025-03-15'::date, true, 'Special')")
            .unwrap();
        let result =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('iso', '2025-03-15'::date)")
                .unwrap()
                .unwrap();
        assert!(result); // Exception makes weekend working
    }

    #[pg_test]
    fn test_is_working_day_recurring_holiday() {
        create_iso();
        // Add recurring Christmas
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Christmas', '2025-12-25'::date, true)")
            .unwrap();
        // Check 2026-12-25 (recurring should match month+day)
        let result =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('iso', '2026-12-25'::date)")
                .unwrap()
                .unwrap();
        assert!(!result); // Recurring holiday
    }

    // =========================================================================
    // Phase 3: add_working_days
    // =========================================================================

    #[pg_test]
    fn test_add_working_days_simple() {
        create_iso();
        // 2025-03-10 (Mon) + 5 working days = 2025-03-17 (Mon)
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.add_working_days('iso', '2025-03-10'::date, 5)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-17");
    }

    #[pg_test]
    fn test_add_working_days_crosses_weekend() {
        create_iso();
        // 2025-03-13 (Thu) + 2 working days = skip Sat/Sun = 2025-03-17 (Mon)
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.add_working_days('iso', '2025-03-13'::date, 2)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-17");
    }

    #[pg_test]
    fn test_add_working_days_negative() {
        create_iso();
        // 2025-03-17 (Mon) - 5 working days = 2025-03-10 (Mon)
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.add_working_days('iso', '2025-03-17'::date, -5)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-10");
    }

    #[pg_test]
    fn test_add_working_days_zero() {
        create_iso();
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.add_working_days('iso', '2025-03-10'::date, 0)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-10");
    }

    #[pg_test]
    fn test_add_working_days_with_holiday() {
        create_iso();
        // Add holiday on Wed 2025-03-12
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Hol', '2025-03-12'::date)").unwrap();
        // Mon + 4 working days: Tue, (skip Wed holiday), Thu, Fri, Mon = 2025-03-17
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.add_working_days('iso', '2025-03-10'::date, 4)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-17");
    }

    // =========================================================================
    // Phase 3: working_days_between
    // =========================================================================

    #[pg_test]
    fn test_working_days_between_one_week() {
        create_iso();
        // Mon to next Mon = 5 working days
        let result = Spi::get_one::<i32>(
            "SELECT pgcalendar.working_days_between('iso', '2025-03-10'::date, '2025-03-17'::date)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, 5);
    }

    #[pg_test]
    fn test_working_days_between_same_day() {
        create_iso();
        let result = Spi::get_one::<i32>(
            "SELECT pgcalendar.working_days_between('iso', '2025-03-10'::date, '2025-03-10'::date)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, 0);
    }

    #[pg_test]
    fn test_working_days_between_reverse() {
        create_iso();
        // Reversed range returns negative
        let result = Spi::get_one::<i32>(
            "SELECT pgcalendar.working_days_between('iso', '2025-03-17'::date, '2025-03-10'::date)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, -5);
    }

    #[pg_test]
    fn test_working_days_between_with_holiday() {
        create_iso();
        Spi::run("SELECT pgcalendar.add_holiday('iso', 'Hol', '2025-03-12'::date)").unwrap();
        // Mon to next Mon with 1 holiday = 4 working days
        let result = Spi::get_one::<i32>(
            "SELECT pgcalendar.working_days_between('iso', '2025-03-10'::date, '2025-03-17'::date)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, 4);
    }

    // =========================================================================
    // Phase 3: next/prev working day
    // =========================================================================

    #[pg_test]
    fn test_next_working_day_from_friday() {
        create_iso();
        // 2025-03-14 is Friday, next working = Monday 2025-03-17
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.next_working_day('iso', '2025-03-14'::date)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-17");
    }

    #[pg_test]
    fn test_next_working_day_from_weekday() {
        create_iso();
        // 2025-03-10 (Mon), next = Tue 2025-03-11
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.next_working_day('iso', '2025-03-10'::date)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-11");
    }

    #[pg_test]
    fn test_prev_working_day_from_monday() {
        create_iso();
        // 2025-03-17 (Mon), prev = Fri 2025-03-14
        let result = Spi::get_one::<String>(
            "SELECT pgcalendar.prev_working_day('iso', '2025-03-17'::date)::text",
        )
        .unwrap()
        .unwrap();
        assert_eq!(result, "2025-03-14");
    }

    // =========================================================================
    // Phase 4: Seeds
    // =========================================================================

    #[pg_test]
    fn test_seed_iso() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgcalendar.seed_iso()")
            .unwrap()
            .unwrap();
        assert!(id > 0);

        // Should have Mon-Fri working
        let working = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern WHERE calendar_id = $1 AND is_working = true",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(working, 5);
    }

    #[pg_test]
    fn test_seed_iso_idempotent() {
        set_search_path();
        Spi::run("SELECT pgcalendar.seed_iso()").unwrap();
        Spi::run("SELECT pgcalendar.seed_iso()").unwrap(); // Should not error
    }

    #[pg_test]
    fn test_seed_middle_east() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgcalendar.seed_middle_east()")
            .unwrap()
            .unwrap();

        // Should have Sun-Thu working (5 days)
        let working = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern WHERE calendar_id = $1 AND is_working = true",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(working, 5);
    }

    #[pg_test]
    fn test_seed_six_day() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgcalendar.seed_six_day()")
            .unwrap()
            .unwrap();

        // Should have Mon-Sat working (6 days)
        let working = Spi::get_one_with_args::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.weekly_pattern WHERE calendar_id = $1 AND is_working = true",
            &[id.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(working, 6);
    }

    // =========================================================================
    // Phase 4: Queries
    // =========================================================================

    #[pg_test]
    fn test_calendar_info() {
        create_iso();
        let count = Spi::get_one::<i64>("SELECT COUNT(*) FROM pgcalendar.calendar_info('iso')")
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
    }

    #[pg_test]
    fn test_calendar_days() {
        create_iso();
        // One week: 7 days
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.calendar_days('iso', '2025-03-10'::date, '2025-03-16'::date)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 7);
    }

    #[pg_test]
    fn test_calendar_days_working_count() {
        create_iso();
        // One week Mon-Sun: 5 working days
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgcalendar.calendar_days('iso', '2025-03-10'::date, '2025-03-16'::date) WHERE is_working = true",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 5);
    }

    // =========================================================================
    // Phase 4: Full workflow test
    // =========================================================================

    #[pg_test]
    fn test_full_workflow() {
        set_search_path();

        // Create calendar
        Spi::run("SELECT pgcalendar.create_calendar('erp_cal', 'ERP Working Calendar')").unwrap();

        // Set Mon-Fri
        Spi::run("SELECT pgcalendar.set_working_days('erp_cal', '1,2,3,4,5')").unwrap();

        // Add holidays
        Spi::run("SELECT pgcalendar.add_holiday('erp_cal', 'New Year', '2025-01-01'::date, true)")
            .unwrap();
        Spi::run("SELECT pgcalendar.add_holiday('erp_cal', 'Christmas', '2025-12-25'::date, true)")
            .unwrap();

        // Add exception: make a Saturday working
        Spi::run(
            "SELECT pgcalendar.add_exception('erp_cal', '2025-03-15'::date, true, 'Inventory day')",
        )
        .unwrap();

        // Verify
        let is_monday =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('erp_cal', '2025-03-10'::date)")
                .unwrap()
                .unwrap();
        assert!(is_monday);

        let is_saturday_exception =
            Spi::get_one::<bool>("SELECT pgcalendar.is_working_day('erp_cal', '2025-03-15'::date)")
                .unwrap()
                .unwrap();
        assert!(is_saturday_exception);

        // Add working days
        let delivery = Spi::get_one::<String>(
            "SELECT pgcalendar.add_working_days('erp_cal', '2025-03-10'::date, 10)::text",
        )
        .unwrap()
        .unwrap();
        // Mon + 10 working days with Saturday exception = should land on Thu 2025-03-20
        // Mon(11), Tue(12), Wed(13), Thu(14), Fri(15=Sat exception counts next working)
        // Actually: start Mon 10, count: Tue11, Wed12, Thu13, Fri14, Sat15(exception=working),
        // Mon17, Tue18, Wed19, Thu20, Fri21 = 10 days → Fri21
        assert_eq!(delivery, "2025-03-21");
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
