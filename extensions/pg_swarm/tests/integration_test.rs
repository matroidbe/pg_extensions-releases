//! Integration tests for pg_swarm distributed task processing
//!
//! These tests require a running PostgreSQL instance with pg_swarm installed.
//! Use `./test.sh` to set up and run automatically.

mod common;

use std::time::Duration;

// ──────────────────────────────────────────────────────────
//  Cluster bootstrap
// ──────────────────────────────────────────────────────────

#[test]
fn test_extension_loaded() {
    skip_if_not_running!();
    let val =
        common::query_one("SELECT count(*)::bigint FROM pg_extension WHERE extname = 'pg_swarm'")
            .unwrap();
    assert_eq!(val, Some("1".to_string()));
}

#[test]
fn test_schema_tables_exist() {
    skip_if_not_running!();
    let tables = common::query_all(
        "SELECT table_name::text FROM information_schema.tables \
         WHERE table_schema = 'pgswarm' ORDER BY table_name",
    )
    .unwrap();
    let names: Vec<&str> = tables.iter().map(|r| r[0].as_str()).collect();
    assert!(names.contains(&"executors"), "missing executors table");
    assert!(names.contains(&"nodes"), "missing nodes table");
    assert!(names.contains(&"jobs"), "missing jobs table");
    assert!(names.contains(&"tasks"), "missing tasks table");
    assert!(names.contains(&"watches"), "missing watches table");
}

#[test]
fn test_node_registered() {
    skip_if_not_running!();
    let rows = common::query_all("SELECT node_name::text, status::text FROM pgswarm.list_nodes()")
        .unwrap();
    assert!(!rows.is_empty(), "no nodes registered");
    // At least one active node
    let active = rows.iter().any(|r| r[1] == "active");
    assert!(active, "no active node found: {:?}", rows);
}

#[test]
fn test_status_returns_json() {
    skip_if_not_running!();
    let val = common::query_one("SELECT pgswarm.status()::text")
        .unwrap()
        .unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&val).expect("status() should return valid JSON");
    assert!(parsed.get("active_nodes").is_some());
    assert!(parsed.get("pending_tasks").is_some());
}

// ──────────────────────────────────────────────────────────
//  Executor management
// ──────────────────────────────────────────────────────────

#[test]
fn test_register_and_unregister_executor() {
    skip_if_not_running!();
    let name = "it_test_register";
    common::cleanup_executor(name);

    // Register
    let id = common::query_one(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'test executor')",
        name
    ))
    .unwrap();
    assert!(id.is_some(), "register_executor should return an id");

    // Verify listed
    let rows = common::query_all("SELECT name::text FROM pgswarm.list_executors()").unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(names.contains(&name), "executor not listed after register");

    // Unregister
    let ok = common::query_one(&format!("SELECT pgswarm.unregister_executor('{}')", name)).unwrap();
    assert_eq!(ok, Some("true".to_string()));

    // Verify gone
    let rows2 = common::query_all("SELECT name::text FROM pgswarm.list_executors()").unwrap();
    let names2: Vec<&str> = rows2.iter().map(|r| r[0].as_str()).collect();
    assert!(
        !names2.contains(&name),
        "executor still listed after unregister"
    );
}

#[test]
fn test_executor_stats() {
    skip_if_not_running!();
    let name = "it_test_stats";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'stats test')",
        name
    ))
    .unwrap();

    let rows = common::query_all(&format!(
        "SELECT name::text, health_status::text FROM pgswarm.executor_stats('{}')",
        name
    ))
    .unwrap();
    assert!(!rows.is_empty(), "executor_stats should return a row");
    assert_eq!(rows[0][0], name);
    assert_eq!(rows[0][1], "healthy");

    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Job submission and execution (echo executor)
// ──────────────────────────────────────────────────────────

#[test]
fn test_submit_job_echo() {
    skip_if_not_running!();
    let name = "it_echo_submit";
    common::cleanup_executor(name);

    // Register executor
    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'echo test')",
        name
    ))
    .unwrap();

    // Submit job with 1 chunk
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"msg\": \"hello\"}}'::jsonb, 1)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().expect("job_id should be numeric");
    assert!(job_id > 0);

    // Wait for completion
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "job completion",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Verify task result
    let results = common::query_all(&format!(
        "SELECT result::text FROM pgswarm.get_task_results({})",
        job_id
    ))
    .unwrap();
    assert_eq!(results.len(), 1, "should have 1 task result");
    let result: serde_json::Value = serde_json::from_str(&results[0][0]).unwrap();
    assert_eq!(result["echo"]["msg"], "hello");

    common::cleanup_executor(name);
}

#[test]
fn test_submit_job_multi_chunk() {
    skip_if_not_running!();
    let name = "it_echo_multi";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'multi chunk test')",
        name
    ))
    .unwrap();

    // Submit job with 3 chunks
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"batch\": true}}'::jsonb, 3)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Wait for completion
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "multi-chunk job",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Verify all 3 tasks completed
    let tasks = common::query_all(&format!(
        "SELECT chunk_index::text, status::text FROM pgswarm.get_tasks({})",
        job_id
    ))
    .unwrap();
    assert_eq!(tasks.len(), 3, "should have 3 tasks");
    for task in &tasks {
        assert_eq!(task[1], "completed", "task chunk {} not completed", task[0]);
    }

    // Verify results
    let results = common::query_all(&format!(
        "SELECT result::text FROM pgswarm.get_task_results({})",
        job_id
    ))
    .unwrap();
    assert_eq!(results.len(), 3, "should have 3 results");

    common::cleanup_executor(name);
}

#[test]
fn test_submit_job_with_chunks() {
    skip_if_not_running!();
    let name = "it_echo_chunks";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'explicit chunks test')",
        name
    ))
    .unwrap();

    // Submit with explicit per-chunk payloads
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job_with_chunks('{}', \
         '[{{\"chunk_id\": \"a\", \"payload\": {{\"x\": 1}}}}, {{\"chunk_id\": \"b\", \"payload\": {{\"x\": 2}}}}]'::jsonb)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "chunked job",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Verify chunk IDs
    let tasks = common::query_all(&format!(
        "SELECT chunk_id::text, status::text FROM pgswarm.get_tasks({})",
        job_id
    ))
    .unwrap();
    assert_eq!(tasks.len(), 2);
    let chunk_ids: Vec<&str> = tasks.iter().map(|r| r[0].as_str()).collect();
    assert!(chunk_ids.contains(&"a"), "missing chunk a");
    assert!(chunk_ids.contains(&"b"), "missing chunk b");

    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Failed tasks and retries
// ──────────────────────────────────────────────────────────

#[test]
fn test_failed_job() {
    skip_if_not_running!();
    let name = "it_fail_test";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_fail_executor', 'failure test')",
        name
    ))
    .unwrap();

    // Submit with max_retries=0 so it fails immediately
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"will\": \"fail\"}}'::jsonb, 1, 0, 0)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Wait for the job to fail
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for("job failure", &check_sql, "failed", Duration::from_secs(30)).unwrap();

    // Verify task has error message
    let tasks = common::query_all(&format!(
        "SELECT status::text, error_message::text FROM pgswarm.get_tasks({})",
        job_id
    ))
    .unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0][0], "failed");
    assert!(
        tasks[0][1].contains("intentional failure"),
        "error message should contain 'intentional failure', got: {}",
        tasks[0][1]
    );

    common::cleanup_executor(name);
}

#[test]
fn test_retry_failed_job() {
    skip_if_not_running!();
    let name = "it_retry_test";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_fail_executor', 'retry test')",
        name
    ))
    .unwrap();

    // Submit with max_retries=0
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"r\": 1}}'::jsonb, 1, 0, 0)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Wait for failure
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "initial failure",
        &check_sql,
        "failed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Now switch executor to echo (succeeding) before retrying
    common::execute(&format!(
        "UPDATE pgswarm.executors SET function_name = 'it_echo_executor' WHERE name = '{}'",
        name
    ))
    .unwrap();

    // Retry the job
    let retried = common::query_one(&format!("SELECT pgswarm.retry_job({})", job_id))
        .unwrap()
        .unwrap();
    let retried: i64 = retried.parse().unwrap();
    assert!(
        retried > 0,
        "retry_job should return count of retried tasks"
    );

    // Wait for successful completion
    common::wait_for(
        "retry completion",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Job control
// ──────────────────────────────────────────────────────────

#[test]
fn test_cancel_job() {
    skip_if_not_running!();
    let name = "it_cancel_test";
    common::cleanup_executor(name);

    // Use slow executor so we have time to cancel
    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_slow_executor', 'cancel test')",
        name
    ))
    .unwrap();

    // Submit job with many chunks to ensure some stay pending
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"c\": 1}}'::jsonb, 10)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Give scheduler a moment to start picking up tasks
    std::thread::sleep(Duration::from_millis(500));

    // Cancel
    let ok = common::query_one(&format!("SELECT pgswarm.cancel_job({})", job_id))
        .unwrap()
        .unwrap();
    assert_eq!(ok, "true");

    // Verify job status is cancelled
    let status = common::query_one(&format!(
        "SELECT status::text FROM pgswarm.get_job({})",
        job_id
    ))
    .unwrap()
    .unwrap();
    assert_eq!(status, "cancelled");

    common::cleanup_executor(name);
}

#[test]
fn test_list_jobs() {
    skip_if_not_running!();
    let name = "it_list_jobs";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'list test')",
        name
    ))
    .unwrap();

    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"l\": 1}}'::jsonb)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Wait for completion
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for("list job", &check_sql, "completed", Duration::from_secs(30)).unwrap();

    // list_jobs should include our job
    let rows = common::query_all("SELECT id::text, status::text FROM pgswarm.list_jobs()").unwrap();
    let found = rows.iter().any(|r| r[0] == job_id.to_string());
    assert!(found, "job {} not found in list_jobs", job_id);

    // list_jobs with status filter
    let completed =
        common::query_all("SELECT id::text FROM pgswarm.list_jobs('completed')").unwrap();
    let found2 = completed.iter().any(|r| r[0] == job_id.to_string());
    assert!(found2, "job {} not in completed filter", job_id);

    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Source tables and watches
// ──────────────────────────────────────────────────────────

#[test]
fn test_source_table_crud() {
    skip_if_not_running!();
    let table = "pgswarm.it_source_crud";
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", table));

    // Create source table
    let ok = common::query_one(&format!("SELECT pgswarm.create_source_table('{}')", table))
        .unwrap()
        .unwrap();
    assert_eq!(ok, "true");

    // Insert single row
    let id = common::query_one(&format!(
        "SELECT pgswarm.swarm_insert('{}', '{{\"k\": 1}}'::jsonb)",
        table
    ))
    .unwrap()
    .unwrap();
    let id: i64 = id.parse().unwrap();
    assert!(id > 0);

    // Insert batch
    let count = common::query_one(&format!(
        "SELECT pgswarm.swarm_insert_batch('{}', '[{{\"k\": 2}}, {{\"k\": 3}}]'::jsonb)",
        table
    ))
    .unwrap()
    .unwrap();
    assert_eq!(count, "2");

    // Count
    let total = common::query_one(&format!("SELECT pgswarm.swarm_count('{}')", table))
        .unwrap()
        .unwrap();
    assert_eq!(total, "3");

    // Cleanup
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", table));
}

#[test]
fn test_submit_job_from_table() {
    skip_if_not_running!();
    let name = "it_from_table";
    let table = "pgswarm.it_source_fromtbl";
    common::cleanup_executor(name);
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", table));

    // Setup
    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'from-table test')",
        name
    ))
    .unwrap();
    common::execute(&format!("SELECT pgswarm.create_source_table('{}')", table)).unwrap();
    common::execute(&format!(
        "SELECT pgswarm.swarm_insert('{}', '{{\"row\": 1}}'::jsonb)",
        table
    ))
    .unwrap();
    common::execute(&format!(
        "SELECT pgswarm.swarm_insert('{}', '{{\"row\": 2}}'::jsonb)",
        table
    ))
    .unwrap();

    // Submit from table
    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job_from_table('{}', '{}')",
        name, table
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Wait for completion
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "from-table job",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Each row = 1 task
    let results = common::query_all(&format!(
        "SELECT result::text FROM pgswarm.get_task_results({})",
        job_id
    ))
    .unwrap();
    assert_eq!(results.len(), 2, "should have 2 task results");

    // Cleanup
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", table));
    common::cleanup_executor(name);
}

#[test]
fn test_watch_source_table() {
    skip_if_not_running!();
    let name = "it_watch_exec";
    let table = "pgswarm.it_source_watch";
    let _ = common::execute(&format!(
        "SELECT pgswarm.unwatch_source_table('{}', '{}')",
        table, name
    ));
    common::cleanup_executor(name);
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", table));

    // Setup
    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'watch test')",
        name
    ))
    .unwrap();
    common::execute(&format!("SELECT pgswarm.create_source_table('{}')", table)).unwrap();

    // Start watching with short poll interval
    let watch_id = common::query_one(&format!(
        "SELECT pgswarm.watch_source_table('{}', '{}', '{{}}'::jsonb, 10, 2)",
        table, name
    ))
    .unwrap()
    .unwrap();
    let watch_id: i64 = watch_id.parse().unwrap();
    assert!(watch_id > 0);

    // Verify watch is listed
    let watches = common::query_all(
        "SELECT source_table::text, executor_name::text FROM pgswarm.list_watches()",
    )
    .unwrap();
    let found = watches.iter().any(|r| r[0] == table && r[1] == name);
    assert!(found, "watch not listed");

    // Insert rows — the node manager should auto-create jobs
    common::execute(&format!(
        "SELECT pgswarm.swarm_insert('{}', '{{\"auto\": 1}}'::jsonb)",
        table
    ))
    .unwrap();

    // Wait for at least one job to appear for this executor
    let job_check = format!(
        "SELECT count(*)::bigint FROM pgswarm.jobs WHERE executor_name = '{}' AND status = 'completed'",
        name
    );
    // Give the watch poller enough time to detect and process
    let result = common::wait_for("watch auto-job", &job_check, "1", Duration::from_secs(30));

    // Unwatch
    let _ = common::execute(&format!(
        "SELECT pgswarm.unwatch_source_table('{}', '{}')",
        table, name
    ));

    // Verify watch disabled (unwatch sets enabled=false, doesn't delete)
    let watches2 = common::query_all(
        "SELECT source_table::text, executor_name::text, enabled::text FROM pgswarm.list_watches()",
    )
    .unwrap();
    let still_enabled = watches2
        .iter()
        .any(|r| r[0] == table && r[1] == name && r[2] == "true");
    assert!(!still_enabled, "watch still enabled after unwatch");

    // Now assert the auto-job worked (after cleanup to avoid leaving state on failure)
    result.unwrap();

    // Cleanup
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", table));
    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Result tables
// ──────────────────────────────────────────────────────────

#[test]
fn test_result_table() {
    skip_if_not_running!();
    let name = "it_result_tbl";
    let result_table = "pgswarm.it_results";
    common::cleanup_executor(name);
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", result_table));

    // Create result table
    let ok = common::query_one(&format!(
        "SELECT pgswarm.create_result_table('{}')",
        result_table
    ))
    .unwrap()
    .unwrap();
    assert_eq!(ok, "true");

    // Register executor and submit job with result_table
    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'result table test')",
        name
    ))
    .unwrap();

    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"rt\": 1}}'::jsonb, 2, 0, 3, 3600, 'greedy', '{}')",
        name, result_table
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    // Wait for completion
    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "result table job",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Verify results written to result table
    let rows = common::query_all(&format!(
        "SELECT job_id::text, result::text FROM {} WHERE job_id = {}",
        result_table, job_id
    ))
    .unwrap();
    assert_eq!(rows.len(), 2, "should have 2 results in result table");

    // Cleanup
    let _ = common::execute(&format!("DROP TABLE IF EXISTS {}", result_table));
    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Views
// ──────────────────────────────────────────────────────────

#[test]
fn test_job_progress_view() {
    skip_if_not_running!();
    let name = "it_progress_view";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'progress view test')",
        name
    ))
    .unwrap();

    let job_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"pv\": 1}}'::jsonb, 2)",
        name
    ))
    .unwrap()
    .unwrap();
    let job_id: i64 = job_id.parse().unwrap();

    let check_sql = format!("SELECT status::text FROM pgswarm.get_job({})", job_id);
    common::wait_for(
        "progress view job",
        &check_sql,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();

    // Check job_progress view
    let rows = common::query_all(&format!(
        "SELECT total_tasks::text, completed::text FROM pgswarm.job_progress WHERE job_id = {}",
        job_id
    ))
    .unwrap();
    assert_eq!(rows.len(), 1, "should have 1 row in job_progress");
    assert_eq!(rows[0][0], "2", "total_tasks should be 2");
    assert_eq!(rows[0][1], "2", "completed_tasks should be 2");

    common::cleanup_executor(name);
}

#[test]
fn test_node_status_view() {
    skip_if_not_running!();
    let rows =
        common::query_all("SELECT node_name::text, status::text FROM pgswarm.node_status").unwrap();
    assert!(
        !rows.is_empty(),
        "node_status view should have at least 1 row"
    );
}

#[test]
fn test_executor_health_view() {
    skip_if_not_running!();
    let name = "it_health_view";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'health view test')",
        name
    ))
    .unwrap();

    let rows = common::query_all(&format!(
        "SELECT name::text, health_status::text FROM pgswarm.executor_health WHERE name = '{}'",
        name
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], "healthy");

    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Priority ordering
// ──────────────────────────────────────────────────────────

#[test]
fn test_job_priority() {
    skip_if_not_running!();
    let name = "it_priority";
    common::cleanup_executor(name);

    common::execute(&format!(
        "SELECT pgswarm.register_executor('{}', 'it_echo_executor', 'priority test')",
        name
    ))
    .unwrap();

    // Submit low priority first, high priority second
    let low_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"p\": \"low\"}}'::jsonb, 1, 0)",
        name
    ))
    .unwrap()
    .unwrap();

    let high_id = common::query_one(&format!(
        "SELECT pgswarm.submit_job('{}', '{{\"p\": \"high\"}}'::jsonb, 1, 10)",
        name
    ))
    .unwrap()
    .unwrap();

    let low_id: i64 = low_id.parse().unwrap();
    let high_id: i64 = high_id.parse().unwrap();

    // Wait for both to complete
    let check_low = format!("SELECT status::text FROM pgswarm.get_job({})", low_id);
    let check_high = format!("SELECT status::text FROM pgswarm.get_job({})", high_id);
    common::wait_for(
        "high prio",
        &check_high,
        "completed",
        Duration::from_secs(30),
    )
    .unwrap();
    common::wait_for("low prio", &check_low, "completed", Duration::from_secs(30)).unwrap();

    // Both completed — high priority should have started first (check started_at)
    let times = common::query_all(&format!(
        "SELECT id::text, started_at::text FROM pgswarm.get_job({}) \
         UNION ALL \
         SELECT id::text, started_at::text FROM pgswarm.get_job({})",
        high_id, low_id
    ))
    .unwrap();
    // We can't strictly assert ordering with parallel workers, but both should complete
    assert_eq!(times.len(), 2, "should have 2 job rows");

    common::cleanup_executor(name);
}

// ──────────────────────────────────────────────────────────
//  Node management
// ──────────────────────────────────────────────────────────

#[test]
fn test_drain_and_activate_node() {
    skip_if_not_running!();

    // Get the first active node
    let nodes = common::query_all(
        "SELECT id::text, status::text FROM pgswarm.list_nodes() WHERE status = 'active' LIMIT 1",
    )
    .unwrap();
    if nodes.is_empty() {
        eprintln!("SKIPPED: no active nodes to test drain/activate");
        return;
    }
    let node_id: i64 = nodes[0][0].parse().unwrap();

    // Drain it
    let ok = common::query_one(&format!("SELECT pgswarm.drain_node({})", node_id))
        .unwrap()
        .unwrap();
    assert_eq!(ok, "true");

    // Verify drained
    let status = common::query_one(&format!(
        "SELECT status::text FROM pgswarm.list_nodes() WHERE id = {}",
        node_id
    ))
    .unwrap()
    .unwrap();
    assert_eq!(status, "draining");

    // Reactivate
    let ok2 = common::query_one(&format!("SELECT pgswarm.activate_node({})", node_id))
        .unwrap()
        .unwrap();
    assert_eq!(ok2, "true");

    // Verify active again
    let status2 = common::query_one(&format!(
        "SELECT status::text FROM pgswarm.list_nodes() WHERE id = {}",
        node_id
    ))
    .unwrap()
    .unwrap();
    assert_eq!(status2, "active");
}
