//! Integration tests for pg_git.
//!
//! These tests require a running PostgreSQL instance with pg_git loaded
//! (via shared_preload_libraries). Run with `./test.sh` which handles
//! setup and teardown.

mod common;

use common::{connect, poll_until, skip_if_not_running};
use std::time::Duration;

const PG_PORT: u16 = 28816;
const GIT_HTTP_PORT: u16 = 5433;

#[tokio::test]
async fn test_sql_git_init_and_commit() {
    skip_if_not_running!(PG_PORT);
    let client = connect(PG_PORT).await;

    // Clean up from previous runs
    let _ = client
        .execute(
            "DELETE FROM pggit.sync_state WHERE repo_id = 'inttest1'",
            &[],
        )
        .await;
    let _ = client
        .execute("DELETE FROM pggit.commits WHERE repo_id = 'inttest1'", &[])
        .await;
    let _ = client
        .execute("DELETE FROM pggit.repos WHERE id = 'inttest1'", &[])
        .await;
    let _ = std::fs::remove_dir_all("/tmp/pg_git_inttest1");

    // Init repo
    let row = client
        .query_one(
            "SELECT pggit.git_init('inttest1', '/tmp/pg_git_inttest1', 'Integration test')",
            &[],
        )
        .await
        .expect("git_init failed");
    let repo_id: String = row.get(0);
    assert_eq!(repo_id, "inttest1");

    // Add and commit
    client
        .query_one("SELECT pggit.git_add('inttest1', 'test.md', '# Test')", &[])
        .await
        .expect("git_add failed");

    let row = client
        .query_one("SELECT pggit.git_commit('inttest1', 'Initial commit')", &[])
        .await
        .expect("git_commit failed");
    let hash: String = row.get(0);
    assert_eq!(hash.len(), 40);

    // Show file
    let row = client
        .query_one("SELECT pggit.git_show('inttest1', 'test.md')", &[])
        .await
        .expect("git_show failed");
    let content: String = row.get(0);
    assert_eq!(content, "# Test");

    // Verify commit in table
    let count: i64 = client
        .query_one(
            "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'inttest1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);

    // Cleanup
    let _ = std::fs::remove_dir_all("/tmp/pg_git_inttest1");
}

#[tokio::test]
async fn test_http_clone_and_push() {
    skip_if_not_running!(PG_PORT);
    let client = connect(PG_PORT).await;

    // Clean up
    let _ = client
        .execute(
            "DELETE FROM pggit.sync_state WHERE repo_id = 'httptest'",
            &[],
        )
        .await;
    let _ = client
        .execute("DELETE FROM pggit.commits WHERE repo_id = 'httptest'", &[])
        .await;
    let _ = client
        .execute("DELETE FROM pggit.repos WHERE id = 'httptest'", &[])
        .await;
    let _ = std::fs::remove_dir_all("/tmp/pg_git_httptest");
    let _ = std::fs::remove_dir_all("/tmp/pg_git_httptest_clone");

    // Create repo via SQL
    client
        .query_one(
            "SELECT pggit.git_init('httptest', '/tmp/pg_git_httptest')",
            &[],
        )
        .await
        .expect("git_init failed");

    // Add initial content so the repo has a HEAD
    client
        .query_one("SELECT pggit.git_add('httptest', 'init.md', 'init')", &[])
        .await
        .unwrap();
    client
        .query_one("SELECT pggit.git_commit('httptest', 'init')", &[])
        .await
        .unwrap();

    // Clone via HTTP
    let clone_output = std::process::Command::new("git")
        .args([
            "clone",
            &format!("http://localhost:{}/httptest", GIT_HTTP_PORT),
            "/tmp/pg_git_httptest_clone",
        ])
        .output()
        .expect("git clone failed to execute");

    assert!(
        clone_output.status.success(),
        "git clone failed: {}",
        String::from_utf8_lossy(&clone_output.stderr)
    );

    // Verify cloned content
    let content = std::fs::read_to_string("/tmp/pg_git_httptest_clone/init.md")
        .expect("init.md not found in clone");
    assert_eq!(content, "init");

    // Add file and push
    std::fs::write("/tmp/pg_git_httptest_clone/pushed.md", "# Pushed via HTTP").unwrap();
    let _ = std::process::Command::new("git")
        .args(["add", "pushed.md"])
        .current_dir("/tmp/pg_git_httptest_clone")
        .output()
        .unwrap();
    let _ = std::process::Command::new("git")
        .args(["commit", "-m", "Push test"])
        .current_dir("/tmp/pg_git_httptest_clone")
        .output()
        .unwrap();

    let push_output = std::process::Command::new("git")
        .args(["push"])
        .current_dir("/tmp/pg_git_httptest_clone")
        .output()
        .expect("git push failed to execute");

    assert!(
        push_output.status.success(),
        "git push failed: {}",
        String::from_utf8_lossy(&push_output.stderr)
    );

    // Wait for sync worker to pick up the pushed commit
    let client2 = connect(PG_PORT).await;
    let found = poll_until(
        || async {
            let row = client2
                .query_one(
                    "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'httptest'",
                    &[],
                )
                .await;
            match row {
                Ok(r) => {
                    let count: i64 = r.get(0);
                    count >= 2 // init commit + push commit
                }
                Err(_) => false,
            }
        },
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .await;
    assert!(
        found,
        "Pushed commit not synced to pggit.commits within timeout"
    );

    // Verify pushed file is readable via SQL
    let row = client
        .query_one("SELECT pggit.git_show('httptest', 'pushed.md')", &[])
        .await
        .expect("git_show failed for pushed file");
    let content: String = row.get(0);
    assert_eq!(content, "# Pushed via HTTP");

    // Cleanup
    let _ = std::fs::remove_dir_all("/tmp/pg_git_httptest");
    let _ = std::fs::remove_dir_all("/tmp/pg_git_httptest_clone");
}

#[tokio::test]
async fn test_sync_worker_detects_external_commits() {
    skip_if_not_running!(PG_PORT);
    let client = connect(PG_PORT).await;

    // Clean up
    let _ = client
        .execute(
            "DELETE FROM pggit.sync_state WHERE repo_id = 'synctest'",
            &[],
        )
        .await;
    let _ = client
        .execute("DELETE FROM pggit.commits WHERE repo_id = 'synctest'", &[])
        .await;
    let _ = client
        .execute("DELETE FROM pggit.repos WHERE id = 'synctest'", &[])
        .await;
    let _ = std::fs::remove_dir_all("/tmp/pg_git_synctest");

    // Create repo via SQL
    client
        .query_one(
            "SELECT pggit.git_init('synctest', '/tmp/pg_git_synctest')",
            &[],
        )
        .await
        .expect("git_init failed");

    // Make commits directly via git CLI (external to pg_git)
    let _ = std::process::Command::new("git")
        .args(["config", "user.name", "external"])
        .current_dir("/tmp/pg_git_synctest")
        .output();
    let _ = std::process::Command::new("git")
        .args(["config", "user.email", "ext@test.com"])
        .current_dir("/tmp/pg_git_synctest")
        .output();

    std::fs::write("/tmp/pg_git_synctest/external.md", "# External").unwrap();
    let _ = std::process::Command::new("git")
        .args(["add", "external.md"])
        .current_dir("/tmp/pg_git_synctest")
        .output();
    let _ = std::process::Command::new("git")
        .args(["commit", "-m", "External commit"])
        .current_dir("/tmp/pg_git_synctest")
        .output();

    // Wait for sync worker to detect the commit
    let client2 = connect(PG_PORT).await;
    let found = poll_until(
        || async {
            let row = client2
                .query_one(
                    "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'synctest'",
                    &[],
                )
                .await;
            match row {
                Ok(r) => {
                    let count: i64 = r.get(0);
                    count >= 1
                }
                Err(_) => false,
            }
        },
        Duration::from_secs(15),
        Duration::from_millis(500),
    )
    .await;
    assert!(
        found,
        "External commit not detected by sync worker within timeout"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all("/tmp/pg_git_synctest");
}
