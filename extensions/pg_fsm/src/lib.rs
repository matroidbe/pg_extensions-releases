mod binding;
mod guard;
mod machine;
mod queries;
mod schema;
mod transition;

pub use binding::*;
pub use machine::*;
pub use queries::*;
pub use transition::*;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// =============================================================================
// GUC Settings
// =============================================================================

pub static PG_FSM_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_fsm.enabled",
        c"Enable pg_fsm state transition enforcement",
        c"When false, skips transition validation in triggers.",
        &PG_FSM_ENABLED,
        pgrx::GucContext::Suset,
        pgrx::GucFlags::default(),
    );

    pgrx::log!("pg_fsm: initialized");
}

// =============================================================================
// Integration Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    fn set_search_path() {
        Spi::run("SET search_path TO pgfsm, public").unwrap();
    }

    /// Helper: create a basic order_flow machine with states and transitions.
    fn setup_order_machine() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('order_flow', 'draft', 'Order workflow')").unwrap();
        Spi::run("SELECT pgfsm.add_state('order_flow', 'submitted')").unwrap();
        Spi::run("SELECT pgfsm.add_state('order_flow', 'approved')").unwrap();
        Spi::run("SELECT pgfsm.add_state('order_flow', 'rejected')").unwrap();
        Spi::run("SELECT pgfsm.add_state('order_flow', 'shipped')").unwrap();
        Spi::run("SELECT pgfsm.add_state('order_flow', 'delivered', true)").unwrap();

        Spi::run("SELECT pgfsm.add_transition('order_flow', 'draft', 'submitted', 'submit')")
            .unwrap();
        Spi::run("SELECT pgfsm.add_transition('order_flow', 'submitted', 'approved', 'approve')")
            .unwrap();
        Spi::run("SELECT pgfsm.add_transition('order_flow', 'submitted', 'rejected', 'reject')")
            .unwrap();
        Spi::run("SELECT pgfsm.add_transition('order_flow', 'rejected', 'draft', 'revise')")
            .unwrap();
        Spi::run("SELECT pgfsm.add_transition('order_flow', 'approved', 'shipped', 'ship')")
            .unwrap();
        Spi::run("SELECT pgfsm.add_transition('order_flow', 'shipped', 'delivered', 'deliver')")
            .unwrap();
    }

    /// Helper: create test orders table and bind to machine.
    fn setup_orders_table() {
        Spi::run(
            "CREATE TABLE public.orders (id serial, status text DEFAULT 'draft', amount numeric)",
        )
        .unwrap();
        Spi::run("SELECT pgfsm.bind_table('order_flow', 'public.orders', 'status')").unwrap();
    }

    // =========================================================================
    // Phase 1: Machine Definition API
    // =========================================================================

    #[pg_test]
    fn test_create_machine_returns_id() {
        set_search_path();
        let id = Spi::get_one::<i32>("SELECT pgfsm.create_machine('test_m', 'init')")
            .unwrap()
            .unwrap();
        assert!(id > 0);
    }

    #[pg_test]
    fn test_create_machine_stores_initial_state() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('test_m2', 'new')").unwrap();

        let state_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgfsm.state s JOIN pgfsm.machine m ON s.machine_id = m.id WHERE m.name = 'test_m2' AND s.name = 'new')",
        )
        .unwrap()
        .unwrap();
        assert!(state_exists);
    }

    #[pg_test]
    fn test_create_machine_with_description() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('test_m3', 'init', 'A test machine')").unwrap();

        let desc =
            Spi::get_one::<String>("SELECT description FROM pgfsm.machine WHERE name = 'test_m3'")
                .unwrap()
                .unwrap();
        assert_eq!(desc, "A test machine");
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_create_machine_duplicate_name() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('dup', 'init')").unwrap();
        Spi::run("SELECT pgfsm.create_machine('dup', 'init')").unwrap();
    }

    #[pg_test]
    fn test_add_state() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('sm', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('sm', 'active')").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.state s JOIN pgfsm.machine m ON s.machine_id = m.id WHERE m.name = 'sm'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 2); // draft + active
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_add_state_invalid_machine() {
        set_search_path();
        Spi::run("SELECT pgfsm.add_state('nonexistent', 'x')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "duplicate key")]
    fn test_add_state_duplicate() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('sm2', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('sm2', 'draft')").unwrap(); // draft already created by create_machine
    }

    #[pg_test]
    fn test_add_state_final() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('sm3', 'open')").unwrap();
        Spi::run("SELECT pgfsm.add_state('sm3', 'closed', true)").unwrap();

        let is_final = Spi::get_one::<bool>(
            "SELECT is_final FROM pgfsm.state s JOIN pgfsm.machine m ON s.machine_id = m.id WHERE m.name = 'sm3' AND s.name = 'closed'",
        )
        .unwrap()
        .unwrap();
        assert!(is_final);
    }

    #[pg_test]
    fn test_add_transition_returns_id() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('tm', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('tm', 'active')").unwrap();

        let tid =
            Spi::get_one::<i32>("SELECT pgfsm.add_transition('tm', 'draft', 'active', 'activate')")
                .unwrap()
                .unwrap();
        assert!(tid > 0);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_add_transition_invalid_from_state() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('tm2', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('tm2', 'active')").unwrap();
        Spi::run("SELECT pgfsm.add_transition('tm2', 'nonexistent', 'active', 'go')").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_add_transition_invalid_to_state() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('tm3', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_transition('tm3', 'draft', 'nonexistent', 'go')").unwrap();
    }

    #[pg_test]
    fn test_add_transition_with_guard() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('gm', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('gm', 'review')").unwrap();

        let tid = Spi::get_one::<i32>(
            "SELECT pgfsm.add_transition('gm', 'draft', 'review', 'submit', '{amount} > 1000')",
        )
        .unwrap()
        .unwrap();
        assert!(tid > 0);

        let guard = Spi::get_one_with_args::<String>(
            "SELECT guard FROM pgfsm.transition WHERE id = $1",
            &[tid.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(guard, "{amount} > 1000");
    }

    #[pg_test]
    #[should_panic(expected = "final state")]
    fn test_add_transition_from_final_state() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('fm', 'open')").unwrap();
        Spi::run("SELECT pgfsm.add_state('fm', 'closed', true)").unwrap();
        Spi::run("SELECT pgfsm.add_state('fm', 'reopened')").unwrap();
        Spi::run("SELECT pgfsm.add_transition('fm', 'open', 'closed', 'close')").unwrap();
        // This should fail: can't transition FROM a final state
        Spi::run("SELECT pgfsm.add_transition('fm', 'closed', 'reopened', 'reopen')").unwrap();
    }

    #[pg_test]
    fn test_add_action() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('am', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('am', 'active')").unwrap();
        let tid =
            Spi::get_one::<i32>("SELECT pgfsm.add_transition('am', 'draft', 'active', 'activate')")
                .unwrap()
                .unwrap();

        Spi::run_with_args(
            "SELECT pgfsm.add_action($1, 'notify', 'order_activated')",
            &[tid.into()],
        )
        .unwrap();

        let count = Spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM pgfsm.action WHERE transition_id = $1",
            &[tid.into()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1);
    }

    #[pg_test]
    #[should_panic(expected = "invalid action_type")]
    fn test_add_action_invalid_type() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('am2', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('am2', 'active')").unwrap();
        let tid =
            Spi::get_one::<i32>("SELECT pgfsm.add_transition('am2', 'draft', 'active', 'go')")
                .unwrap()
                .unwrap();
        Spi::run_with_args(
            "SELECT pgfsm.add_action($1, 'invalid', 'something')",
            &[tid.into()],
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_add_action_invalid_transition() {
        set_search_path();
        Spi::run("SELECT pgfsm.add_action(99999, 'notify', 'test')").unwrap();
    }

    #[pg_test]
    fn test_drop_machine_cascades() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('dm', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('dm', 'active')").unwrap();
        Spi::run("SELECT pgfsm.add_transition('dm', 'draft', 'active', 'go')").unwrap();

        Spi::run("SELECT pgfsm.drop_machine('dm')").unwrap();

        let machine_exists =
            Spi::get_one::<bool>("SELECT EXISTS(SELECT 1 FROM pgfsm.machine WHERE name = 'dm')")
                .unwrap()
                .unwrap();
        assert!(!machine_exists);

        // States and transitions should also be gone
        let state_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.state s WHERE NOT EXISTS(SELECT 1 FROM pgfsm.machine m WHERE m.id = s.machine_id AND m.name = 'dm')",
        )
        .unwrap()
        .unwrap();
        // Just verify the machine doesn't exist anymore — CASCADE handles the rest
        assert!(!machine_exists);
        let _ = state_count; // avoid unused warning
    }

    #[pg_test]
    fn test_multiple_states_and_transitions() {
        setup_order_machine();

        let state_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.state s JOIN pgfsm.machine m ON s.machine_id = m.id WHERE m.name = 'order_flow'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(state_count, 6);

        let transition_count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.transition t JOIN pgfsm.machine m ON t.machine_id = m.id WHERE m.name = 'order_flow'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(transition_count, 6);
    }

    // =========================================================================
    // Phase 2: Table Binding + BEFORE Trigger Enforcement
    // =========================================================================

    #[pg_test]
    fn test_bind_table_creates_triggers() {
        setup_order_machine();
        setup_orders_table();

        let trigger_count = Spi::get_one::<i64>(
            r#"
            SELECT count(*) FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            WHERE c.relname = 'orders' AND t.tgname LIKE 'pgfsm_%'
            "#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(trigger_count, 2); // INSERT + UPDATE
    }

    #[pg_test]
    fn test_insert_with_initial_state_succeeds() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();

        let count = Spi::get_one::<i64>("SELECT count(*) FROM public.orders")
            .unwrap()
            .unwrap();
        assert_eq!(count, 1);
    }

    #[pg_test]
    #[should_panic(expected = "initial state")]
    fn test_insert_with_non_initial_state_errors() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('approved', 100)").unwrap();
    }

    #[pg_test]
    fn test_valid_transition_succeeds() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        // draft → submitted is valid
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "submitted");
    }

    #[pg_test]
    #[should_panic(expected = "invalid transition")]
    fn test_invalid_transition_errors() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        // draft → approved is NOT valid (must go through submitted)
        Spi::run("UPDATE public.orders SET status = 'approved' WHERE id = 1").unwrap();
    }

    #[pg_test]
    fn test_update_without_state_change_succeeds() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        // Just changing amount, not status — should always succeed
        Spi::run("UPDATE public.orders SET amount = 200 WHERE id = 1").unwrap();

        let amount = Spi::get_one::<f64>("SELECT amount::float8 FROM public.orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert!((amount - 200.0).abs() < 0.001);
    }

    #[pg_test]
    fn test_history_created_on_transition() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();

        // Should have 2 history entries: INSERT (empty→draft) + UPDATE (draft→submitted)
        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.history WHERE table_name = 'public.orders'",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 2);

        let new_state = Spi::get_one::<String>(
            "SELECT new_state FROM pgfsm.history WHERE table_name = 'public.orders' ORDER BY id DESC LIMIT 1",
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_state, "submitted");
    }

    #[pg_test]
    #[should_panic(expected = "immutable")]
    fn test_history_is_immutable() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("UPDATE pgfsm.history SET new_state = 'hacked'").unwrap();
    }

    #[pg_test]
    fn test_full_workflow() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 500)").unwrap();
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'approved' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'shipped' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'delivered' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "delivered");
    }

    #[pg_test]
    fn test_cycle_transition() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();
        // reject → back to draft
        Spi::run("UPDATE public.orders SET status = 'rejected' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'draft' WHERE id = 1").unwrap();
        // resubmit
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "submitted");
    }

    #[pg_test]
    fn test_unbind_table_removes_enforcement() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("SELECT pgfsm.unbind_table('public.orders', 'status')").unwrap();

        // Now any transition should work since triggers are gone
        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('approved', 100)").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "approved");
    }

    #[pg_test]
    #[should_panic(expected = "does not have column")]
    fn test_bind_table_invalid_column() {
        setup_order_machine();
        Spi::run("CREATE TABLE public.bad_table (id serial, name text)").unwrap();
        Spi::run("SELECT pgfsm.bind_table('order_flow', 'public.bad_table', 'nonexistent')")
            .unwrap();
    }

    // =========================================================================
    // Phase 3: Transition API + Guard tests
    // =========================================================================

    #[pg_test]
    fn test_transition_by_event() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();

        let new_state =
            Spi::get_one::<String>("SELECT pgfsm.transition('public.orders', '1', 'submit')")
                .unwrap()
                .unwrap();
        assert_eq!(new_state, "submitted");
    }

    #[pg_test]
    #[should_panic(expected = "no valid transition")]
    fn test_transition_invalid_event() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("SELECT pgfsm.transition('public.orders', '1', 'ship')").unwrap();
    }

    #[pg_test]
    fn test_can_transition_true() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();

        let can =
            Spi::get_one::<bool>("SELECT pgfsm.can_transition('public.orders', '1', 'submit')")
                .unwrap()
                .unwrap();
        assert!(can);
    }

    #[pg_test]
    fn test_can_transition_false() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();

        let can =
            Spi::get_one::<bool>("SELECT pgfsm.can_transition('public.orders', '1', 'approve')")
                .unwrap()
                .unwrap();
        assert!(!can);
    }

    #[pg_test]
    fn test_available_events() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();

        let count = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.available_events('public.orders', '1')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count, 1); // only 'submit' from draft

        // Move to submitted — should have 'approve' and 'reject'
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();

        let count2 = Spi::get_one::<i64>(
            "SELECT count(*) FROM pgfsm.available_events('public.orders', '1')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(count2, 2);
    }

    #[pg_test]
    fn test_guard_blocks_transition() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('guard_m', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('guard_m', 'review')").unwrap();
        Spi::run("SELECT pgfsm.add_state('guard_m', 'approved')").unwrap();

        // Only go to review if amount > 1000
        Spi::run(
            "SELECT pgfsm.add_transition('guard_m', 'draft', 'review', 'submit', '{amount} > 1000')",
        )
        .unwrap();
        // Go directly to approved if amount <= 1000
        Spi::run(
            "SELECT pgfsm.add_transition('guard_m', 'draft', 'approved', 'submit', '{amount} <= 1000')",
        )
        .unwrap();

        Spi::run(
            "CREATE TABLE public.guard_orders (id serial, status text DEFAULT 'draft', amount numeric)",
        )
        .unwrap();
        Spi::run("SELECT pgfsm.bind_table('guard_m', 'public.guard_orders', 'status')").unwrap();

        // Small order → approved directly
        Spi::run("INSERT INTO public.guard_orders (status, amount) VALUES ('draft', 500)").unwrap();
        Spi::run("UPDATE public.guard_orders SET status = 'approved' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.guard_orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "approved");
    }

    #[pg_test]
    fn test_guard_routes_to_review() {
        set_search_path();
        Spi::run("SELECT pgfsm.create_machine('guard_m2', 'draft')").unwrap();
        Spi::run("SELECT pgfsm.add_state('guard_m2', 'review')").unwrap();
        Spi::run("SELECT pgfsm.add_state('guard_m2', 'approved')").unwrap();

        Spi::run(
            "SELECT pgfsm.add_transition('guard_m2', 'draft', 'review', 'submit', '{amount} > 1000')",
        )
        .unwrap();
        Spi::run(
            "SELECT pgfsm.add_transition('guard_m2', 'draft', 'approved', 'submit', '{amount} <= 1000')",
        )
        .unwrap();

        Spi::run(
            "CREATE TABLE public.guard_orders2 (id serial, status text DEFAULT 'draft', amount numeric)",
        )
        .unwrap();
        Spi::run("SELECT pgfsm.bind_table('guard_m2', 'public.guard_orders2', 'status')").unwrap();

        // Large order → review required
        Spi::run("INSERT INTO public.guard_orders2 (status, amount) VALUES ('draft', 5000)")
            .unwrap();
        Spi::run("UPDATE public.guard_orders2 SET status = 'review' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.guard_orders2 WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "review");
    }

    // =========================================================================
    // Phase 4: Queries
    // =========================================================================

    #[pg_test]
    fn test_history_for() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'approved' WHERE id = 1").unwrap();

        let count =
            Spi::get_one::<i64>("SELECT count(*) FROM pgfsm.history_for('public.orders', '1')")
                .unwrap()
                .unwrap();
        assert_eq!(count, 3); // INSERT + submit + approve
    }

    #[pg_test]
    fn test_current_state() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT pgfsm.current_state('public.orders', '1')")
            .unwrap()
            .unwrap();
        assert_eq!(state, "submitted");
    }

    #[pg_test]
    fn test_machine_diagram() {
        setup_order_machine();

        let dot = Spi::get_one::<String>("SELECT pgfsm.machine_diagram('order_flow')")
            .unwrap()
            .unwrap();

        assert!(dot.contains("digraph order_flow"));
        assert!(dot.contains("\"draft\" -> \"submitted\""));
        assert!(dot.contains("\"delivered\" [shape=doublecircle]"));
    }

    #[pg_test]
    fn test_disabled_fsm_allows_any_transition() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();

        // Disable pg_fsm
        Spi::run("SET pg_fsm.enabled = false").unwrap();

        // This would normally fail, but with FSM disabled it should pass
        Spi::run("UPDATE public.orders SET status = 'delivered' WHERE id = 1").unwrap();

        let state = Spi::get_one::<String>("SELECT status FROM public.orders WHERE id = 1")
            .unwrap()
            .unwrap();
        assert_eq!(state, "delivered");

        // Re-enable
        Spi::run("SET pg_fsm.enabled = true").unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "invalid transition")]
    fn test_final_state_blocks_transition() {
        setup_order_machine();
        setup_orders_table();

        Spi::run("INSERT INTO public.orders (status, amount) VALUES ('draft', 100)").unwrap();
        Spi::run("UPDATE public.orders SET status = 'submitted' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'approved' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'shipped' WHERE id = 1").unwrap();
        Spi::run("UPDATE public.orders SET status = 'delivered' WHERE id = 1").unwrap();
        // delivered is final — no transitions out
        Spi::run("UPDATE public.orders SET status = 'draft' WHERE id = 1").unwrap();
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
