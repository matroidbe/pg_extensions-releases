//! Background workers for pg_git.
//!
//! Two workers:
//! 1. HTTP worker — runs the git smart HTTP endpoint
//! 2. Sync worker — periodically syncs git repos to Postgres tables

use crate::config;
use crate::metadata;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_HOST: &str = "0.0.0.0";

/// Register the HTTP server background worker.
pub fn register_http_worker() {
    BackgroundWorkerBuilder::new("pg_git HTTP server")
        .set_function("pg_git_http_worker_main")
        .set_library("pg_git")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(5)))
        .load();
}

/// Register the sync background worker.
pub fn register_sync_worker() {
    BackgroundWorkerBuilder::new("pg_git sync")
        .set_function("pg_git_sync_worker_main")
        .set_library("pg_git")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(10)))
        .load();
}

// ===========================================================================
// HTTP Worker
// ===========================================================================

/// Entry point for the HTTP server background worker.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_git_http_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let database = config::get_database();
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);

    pgrx::log!("pg_git HTTP worker: started, pid={}", std::process::id());

    if !config::PG_GIT_ENABLED.get() {
        pgrx::log!("pg_git HTTP worker: disabled via pg_git.enabled=false");
        return;
    }

    let port = config::PG_GIT_HTTP_PORT.get() as u16;

    if let Err(e) = crate::server::run_server(DEFAULT_HOST, port) {
        pgrx::log!("pg_git HTTP worker: server error: {}", e);
    }

    pgrx::log!("pg_git HTTP worker: shutting down");
}

// ===========================================================================
// Sync Worker
// ===========================================================================

/// Entry point for the sync background worker.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_git_sync_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let database = config::get_database();
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);

    pgrx::log!("pg_git sync worker: started, pid={}", std::process::id());

    if !config::PG_GIT_ENABLED.get() {
        pgrx::log!("pg_git sync worker: disabled via pg_git.enabled=false");
        return;
    }

    // Main sync loop
    let interval_secs = config::PG_GIT_SYNC_INTERVAL.get() as u64;

    while BackgroundWorker::wait_latch(Some(Duration::from_secs(interval_secs))) {
        if BackgroundWorker::sighup_received() {
            // Config could have changed
        }
        if BackgroundWorker::sigterm_received() {
            break;
        }

        if !config::PG_GIT_ENABLED.get() {
            continue;
        }

        sync_all_repos();
    }

    pgrx::log!("pg_git sync worker: shutting down");
}

/// Sync all registered repos to the commits table.
fn sync_all_repos() {
    // Get all repos
    let repos: Vec<(String, String)> = BackgroundWorker::transaction(|| {
        let mut repos = Vec::new();
        let result = Spi::connect(|client| {
            let table = client.select(
                "SELECT r.id::text, r.path::text FROM pggit.repos r",
                None,
                &[],
            )?;

            for row in table {
                let id: String = row.get(1)?.unwrap_or_default();
                let path: String = row.get(2)?.unwrap_or_default();
                repos.push((id, path));
            }
            Ok::<_, spi::Error>(())
        });

        if let Err(e) = result {
            pgrx::log!("pg_git sync: failed to list repos: {}", e);
        }
        repos
    });

    for (repo_id, path) in repos {
        if let Err(e) = sync_repo(&repo_id, &PathBuf::from(&path)) {
            pgrx::log!("pg_git sync: failed to sync repo '{}': {}", repo_id, e);
        }
    }
}

/// Sync a single repo: enumerate all branches and sync each one.
fn sync_repo(repo_id: &str, repo_path: &std::path::Path) -> Result<(), crate::error::PgGitError> {
    let branches = crate::git::list_branches(repo_path)?;

    if branches.is_empty() {
        return Ok(());
    }

    let mut total_synced = 0;

    for branch in &branches {
        match sync_branch(repo_id, repo_path, &branch.name, &branch.commit_hash) {
            Ok(count) => total_synced += count,
            Err(e) => {
                pgrx::log!(
                    "pg_git sync: failed to sync branch '{}' of repo '{}': {}",
                    branch.name,
                    repo_id,
                    e
                );
            }
        }
    }

    if total_synced > 0 {
        // Update legacy sync_state with main branch hash
        if let Some(main_branch) = branches.iter().find(|b| b.name == "main") {
            BackgroundWorker::transaction(|| {
                let sql = format!(
                    "UPDATE pggit.sync_state SET last_synced_hash = '{}', last_synced_at = now() WHERE repo_id = '{}'",
                    main_branch.commit_hash,
                    repo_id.replace('\'', "''"),
                );
                if let Err(e) = Spi::run(&sql) {
                    pgrx::warning!("pg_git sync: update sync_state failed: {}", e);
                }
            });
        }

        pgrx::log!(
            "pg_git sync: repo '{}' synced {} new commit(s) across {} branch(es)",
            repo_id,
            total_synced,
            branches.len()
        );
    }

    Ok(())
}

/// Sync a single branch: find new commits, file history, and file metadata.
///
/// Uses `git log <branch>` and `git show <branch>:<path>` — no checkout needed.
/// Returns the number of new commits synced.
fn sync_branch(
    repo_id: &str,
    repo_path: &std::path::Path,
    branch_name: &str,
    git_tip: &str,
) -> Result<usize, crate::error::PgGitError> {
    // Read the DB's known head for this branch
    let db_head: Option<String> = BackgroundWorker::transaction(|| {
        Spi::connect(|client| {
            let table = client.select(
                "SELECT head_commit::text FROM pggit.branches WHERE repo_id = $1 AND name = $2",
                None,
                &[repo_id.into(), branch_name.into()],
            )?;
            let mut result = None;
            for row in table {
                result = row.get(1)?;
            }
            Ok::<_, spi::Error>(result)
        })
        .unwrap_or(None)
    });

    // If DB head matches git tip, nothing to do
    if db_head.as_deref() == Some(git_tip) {
        return Ok(0);
    }

    let new_commits =
        crate::git::log_commits_on_branch(repo_path, branch_name, db_head.as_deref())?;

    if new_commits.is_empty() {
        return Ok(0);
    }

    let count = new_commits.len();

    // Pre-compute diffs and metadata outside the transaction
    #[allow(clippy::type_complexity)]
    let mut commit_diffs: Vec<(
        &crate::git::CommitInfo,
        Vec<crate::git::DiffEntry>,
        Vec<(String, Option<i32>, Option<i32>)>,
    )> = Vec::new();

    for commit in &new_commits {
        let diff = crate::git::diff_commit_parent(repo_path, &commit.hash).unwrap_or_default();
        let stats = if let Some(ref parent) = commit.parent_hash {
            crate::git::diff_stat(repo_path, parent, &commit.hash).unwrap_or_default()
        } else {
            Vec::new()
        };
        commit_diffs.push((commit, diff, stats));
    }

    BackgroundWorker::transaction(|| {
        for (commit, diff, stats) in &commit_diffs {
            // Insert commit (branch-agnostic — same commit hash deduped)
            let parent = match &commit.parent_hash {
                Some(h) => format!("'{}'", h),
                None => "NULL".to_string(),
            };
            let sql = format!(
                "INSERT INTO pggit.commits (repo_id, hash, parent_hash, message, author, author_email, committed_at)
                 VALUES ('{}', '{}', {}, '{}', '{}', '{}', to_timestamp({}))
                 ON CONFLICT DO NOTHING",
                repo_id.replace('\'', "''"),
                commit.hash,
                parent,
                commit.message.replace('\'', "''"),
                commit.author.replace('\'', "''"),
                commit.author_email.replace('\'', "''"),
                commit.committed_at,
            );
            if let Err(e) = Spi::run(&sql) {
                pgrx::warning!("pg_git sync: insert commit failed: {}", e);
            }

            // Build stat lookup
            let stat_map: std::collections::HashMap<&str, (Option<i32>, Option<i32>)> = stats
                .iter()
                .map(|(p, a, r)| (p.as_str(), (*a, *r)))
                .collect();

            // Insert file_history entries + update files table
            for entry in diff {
                let (lines_added, lines_removed) = stat_map
                    .get(entry.path.as_str())
                    .copied()
                    .unwrap_or((None, None));

                let la = lines_added
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string());
                let lr = lines_removed
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string());

                let fh_sql = format!(
                    "INSERT INTO pggit.file_history (repo_id, path, commit_hash, change_type, lines_added, lines_removed, committed_at, author)
                     VALUES ('{}', '{}', '{}', '{}', {}, {}, to_timestamp({}), '{}')
                     ON CONFLICT DO NOTHING",
                    repo_id.replace('\'', "''"),
                    entry.path.replace('\'', "''"),
                    commit.hash,
                    entry.change_type,
                    la,
                    lr,
                    commit.committed_at,
                    commit.author.replace('\'', "''"),
                );
                if let Err(e) = Spi::run(&fh_sql) {
                    pgrx::warning!("pg_git sync: insert file_history failed: {}", e);
                }

                // Update files table — use branch name, read via show_at_branch
                match entry.change_type.as_str() {
                    "delete" => {
                        let del_sql = format!(
                            "DELETE FROM pggit.files WHERE repo_id = '{}' AND path = '{}' AND branch = '{}'",
                            repo_id.replace('\'', "''"),
                            entry.path.replace('\'', "''"),
                            branch_name.replace('\'', "''"),
                        );
                        let _ = Spi::run(&del_sql);
                    }
                    "add" | "modify" => {
                        // Read blob via branch ref (no checkout needed)
                        if let Ok(content) =
                            crate::git::show_at_branch(repo_path, branch_name, &entry.path)
                        {
                            let meta = metadata::extract_metadata(&entry.path, &content);
                            let title = metadata::extract_title(&entry.path, &content);

                            let title_sql = title
                                .map(|t| format!("'{}'", t.replace('\'', "''")))
                                .unwrap_or_else(|| "NULL".to_string());

                            let lang_sql = meta
                                .language
                                .as_ref()
                                .map(|l| format!("'{}'", l))
                                .unwrap_or_else(|| "NULL".to_string());

                            let lc_sql = meta
                                .line_count
                                .map(|c| c.to_string())
                                .unwrap_or_else(|| "NULL".to_string());

                            let upsert_sql = format!(
                                "INSERT INTO pggit.files (repo_id, path, branch, current_hash, mime_type, size_bytes, encoding, language, line_count, title, updated_at, updated_by, created_at)
                                 VALUES ('{}', '{}', '{}', '{}', '{}', {}, '{}', {}, {}, {}, to_timestamp({}), '{}', to_timestamp({}))
                                 ON CONFLICT (repo_id, branch, path) DO UPDATE SET
                                   current_hash = EXCLUDED.current_hash,
                                   mime_type = EXCLUDED.mime_type,
                                   size_bytes = EXCLUDED.size_bytes,
                                   encoding = EXCLUDED.encoding,
                                   language = EXCLUDED.language,
                                   line_count = EXCLUDED.line_count,
                                   title = EXCLUDED.title,
                                   updated_at = EXCLUDED.updated_at,
                                   updated_by = EXCLUDED.updated_by",
                                repo_id.replace('\'', "''"),
                                entry.path.replace('\'', "''"),
                                branch_name.replace('\'', "''"),
                                commit.hash,
                                meta.mime_type,
                                meta.size_bytes,
                                meta.encoding,
                                lang_sql,
                                lc_sql,
                                title_sql,
                                commit.committed_at,
                                commit.author.replace('\'', "''"),
                                commit.committed_at,
                            );
                            if let Err(e) = Spi::run(&upsert_sql) {
                                pgrx::warning!("pg_git sync: upsert files failed: {}", e);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Update branches.head_commit
        if let Some(last_commit) = new_commits.last() {
            let branch_sql = format!(
                "INSERT INTO pggit.branches (repo_id, name, head_commit)
                 VALUES ('{}', '{}', '{}')
                 ON CONFLICT (repo_id, name) DO UPDATE SET head_commit = '{}'",
                repo_id.replace('\'', "''"),
                branch_name.replace('\'', "''"),
                last_commit.hash,
                last_commit.hash,
            );
            if let Err(e) = Spi::run(&branch_sql) {
                pgrx::warning!("pg_git sync: update branch head failed: {}", e);
            }
        }
    });

    Ok(count)
}
