//! SQL functions exposed via #[pg_extern].

use crate::config;
use crate::git;
use pgrx::prelude::*;
use std::path::PathBuf;

/// Initialize a new git repository managed by pg_git.
///
/// Creates the repo on disk and registers it in `pggit.repos`.
#[pg_extern]
pub fn git_init(
    repo_id: &str,
    path: Option<&str>,
    description: default!(Option<&str>, "NULL"),
) -> String {
    let repo_path = match path {
        Some(p) => PathBuf::from(p),
        None => PathBuf::from(config::get_default_repo_path()).join(repo_id),
    };

    let path_str = repo_path.to_string_lossy().to_string();

    // Check if repo_id already exists
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pggit.repos WHERE id = $1)",
        &[repo_id.into()],
    );
    if let Ok(Some(true)) = exists {
        pgrx::error!("Repository '{}' already exists", repo_id);
    }

    // Init on disk
    if let Err(e) = git::init_repo(&repo_path) {
        pgrx::error!("{}", e);
    }

    // Register in repos table
    let insert_result = Spi::run_with_args(
        "INSERT INTO pggit.repos (id, path, description) VALUES ($1, $2, $3)",
        &[repo_id.into(), path_str.as_str().into(), description.into()],
    );
    if let Err(e) = insert_result {
        pgrx::error!("Failed to register repo: {}", e);
    }

    // Initialize sync state
    let sync_result = Spi::run_with_args(
        "INSERT INTO pggit.sync_state (repo_id) VALUES ($1)",
        &[repo_id.into()],
    );
    if let Err(e) = sync_result {
        pgrx::error!("Failed to initialize sync state: {}", e);
    }

    // Register 'main' branch
    let branch_result = Spi::run_with_args(
        "INSERT INTO pggit.branches (repo_id, name) VALUES ($1, 'main') ON CONFLICT DO NOTHING",
        &[repo_id.into()],
    );
    if let Err(e) = branch_result {
        pgrx::error!("Failed to register main branch: {}", e);
    }

    // Auto-grant admin to the repo creator
    let perm_result = Spi::run_with_args(
        "INSERT INTO pggit.permissions (repo_id, role_name, scope_type, scope_value, access)
         VALUES ($1, current_user, 'repo', '*', 'admin')",
        &[repo_id.into()],
    );
    if let Err(e) = perm_result {
        pgrx::error!("Failed to grant admin permission: {}", e);
    }

    repo_id.to_string()
}

/// Register an **existing** git repository (on-disk) with pg_git.
///
/// Unlike `git_init`, this does NOT create a new repo — it expects `.git/` to
/// already exist at the given path. Useful for indexing/tracking repos that were
/// cloned or created outside of pg_git.
#[pg_extern]
pub fn git_register(
    repo_id: &str,
    path: &str,
    description: default!(Option<&str>, "NULL"),
) -> String {
    let repo_path = PathBuf::from(path);

    // Verify .git exists
    if !repo_path.join(".git").exists() {
        pgrx::error!(
            "No git repository found at '{}'. Use git_init() to create a new repo.",
            path
        );
    }

    // Check if repo_id already exists
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pggit.repos WHERE id = $1)",
        &[repo_id.into()],
    );
    if let Ok(Some(true)) = exists {
        pgrx::error!("Repository '{}' already exists", repo_id);
    }

    // Register in repos table
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO pggit.repos (id, path, description) VALUES ($1, $2, $3)",
        &[repo_id.into(), path.into(), description.into()],
    ) {
        pgrx::error!("Failed to register repo: {}", e);
    }

    // Initialize sync state
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO pggit.sync_state (repo_id) VALUES ($1)",
        &[repo_id.into()],
    ) {
        pgrx::error!("Failed to initialize sync state: {}", e);
    }

    // Detect current branch from the existing repo
    let branch = git::current_branch(&repo_path).unwrap_or_else(|_| "main".to_string());

    // Register branch
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO pggit.branches (repo_id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &[repo_id.into(), branch.as_str().into()],
    ) {
        pgrx::error!("Failed to register branch: {}", e);
    }

    // Auto-grant admin to the repo creator
    if let Err(e) = Spi::run_with_args(
        "INSERT INTO pggit.permissions (repo_id, role_name, scope_type, scope_value, access)
         VALUES ($1, current_user, 'repo', '*', 'admin')",
        &[repo_id.into()],
    ) {
        pgrx::error!("Failed to grant admin permission: {}", e);
    }

    repo_id.to_string()
}

/// Add or update a text file in a repository and stage it.
///
/// If content is provided, writes it to the working tree and stages.
/// If content is NULL, stages an existing file in the working tree.
#[pg_extern]
pub fn git_add(repo_id: &str, path: &str, content: default!(Option<&str>, "NULL")) -> String {
    let repo_path = get_repo_path(repo_id);

    match content {
        Some(text) => {
            if let Err(e) = git::add_file(&repo_path, path, text.as_bytes()) {
                pgrx::error!("{}", e);
            }
        }
        None => {
            if let Err(e) = git::stage_file(&repo_path, path) {
                pgrx::error!("{}", e);
            }
        }
    }

    path.to_string()
}

/// Add or update a binary file in the working tree and stage it.
///
/// Content is stored in the git repo on disk, not in Postgres.
#[pg_extern]
pub fn git_add_binary(repo_id: &str, path: &str, content: &[u8]) -> String {
    let repo_path = get_repo_path(repo_id);

    if let Err(e) = git::add_binary_file(&repo_path, path, content) {
        pgrx::error!("{}", e);
    }

    path.to_string()
}

/// Show a file's content as bytea (for binary files).
///
/// Returns the raw bytes at HEAD or at a specific commit.
#[pg_extern]
pub fn git_show_binary(
    repo_id: &str,
    path: &str,
    commit_hash: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    let repo_path = get_repo_path(repo_id);

    match git::show(&repo_path, path, commit_hash) {
        Ok(bytes) => bytes,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Commit staged changes. Author is the current Postgres role.
///
/// Returns the commit SHA hash.
#[pg_extern]
pub fn git_commit(repo_id: &str, message: &str) -> String {
    let repo_path = get_repo_path(repo_id);

    // Permission checks
    require_write(repo_id);

    // Check branch protection on the current branch
    let current_branch = git::current_branch(&repo_path).unwrap_or_else(|_| "main".to_string());
    check_branch_protection(repo_id, &current_branch);

    // Author = current_user
    let author = Spi::get_one::<String>("SELECT current_user::text")
        .unwrap_or(Some("unknown".to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    // Try to get email from user_profiles if it exists
    let email = get_author_email(&author).unwrap_or_else(|| format!("{}@pggit", author));

    let hash = match git::commit(&repo_path, message, &author, &email) {
        Ok(h) => h,
        Err(e) => pgrx::error!("{}", e),
    };

    // Insert commit record
    let commit_info = match git::log_commits(&repo_path, 1) {
        Ok(commits) if !commits.is_empty() => commits.into_iter().next().unwrap(),
        _ => {
            // Fallback: just insert with what we know
            let insert_result = Spi::run_with_args(
                "INSERT INTO pggit.commits (repo_id, hash, message, author, author_email, committed_at)
                 VALUES ($1, $2, $3, $4, $5, now())
                 ON CONFLICT DO NOTHING",
                &[
                    repo_id.into(),
                    hash.as_str().into(),
                    message.into(),
                    author.as_str().into(),
                    email.as_str().into(),
                ],
            );
            if let Err(e) = insert_result {
                pgrx::warning!("Failed to record commit: {}", e);
            }
            // Update sync state
            let _ = Spi::run_with_args(
                "UPDATE pggit.sync_state SET last_synced_hash = $1, last_synced_at = now() WHERE repo_id = $2",
                &[hash.as_str().into(), repo_id.into()],
            );
            return hash;
        }
    };

    // Insert commit with full info from gitoxide
    let ts = commit_info.committed_at;
    let insert_sql = format!(
        "INSERT INTO pggit.commits (repo_id, hash, parent_hash, message, author, author_email, committed_at)
         VALUES ($1, $2, {}, $3, $4, $5, to_timestamp({}))
         ON CONFLICT DO NOTHING",
        match &commit_info.parent_hash {
            Some(h) => format!("'{}'", h),
            None => "NULL".to_string(),
        },
        ts,
    );
    let insert_result = Spi::run_with_args(
        &insert_sql,
        &[
            repo_id.into(),
            commit_info.hash.as_str().into(),
            commit_info.message.as_str().into(),
            commit_info.author.as_str().into(),
            commit_info.author_email.as_str().into(),
        ],
    );
    if let Err(e) = insert_result {
        pgrx::warning!("Failed to record commit: {}", e);
    }

    // Update sync state
    let _ = Spi::run_with_args(
        "UPDATE pggit.sync_state SET last_synced_hash = $1, last_synced_at = now() WHERE repo_id = $2",
        &[hash.as_str().into(), repo_id.into()],
    );

    // Update branches.head_commit for the current branch
    let _ = Spi::run_with_args(
        "INSERT INTO pggit.branches (repo_id, name, head_commit)
         VALUES ($1, $2, $3)
         ON CONFLICT (repo_id, name) DO UPDATE SET head_commit = EXCLUDED.head_commit",
        &[
            repo_id.into(),
            current_branch.as_str().into(),
            hash.as_str().into(),
        ],
    );

    hash
}

/// Show a file's content at HEAD or at a specific commit.
#[pg_extern]
pub fn git_show(repo_id: &str, path: &str, commit_hash: default!(Option<&str>, "NULL")) -> String {
    let repo_path = get_repo_path(repo_id);

    let content = match git::show(&repo_path, path, commit_hash) {
        Ok(bytes) => bytes,
        Err(e) => pgrx::error!("{}", e),
    };

    match String::from_utf8(content) {
        Ok(s) => s,
        Err(_) => pgrx::error!("File '{}' is binary and cannot be displayed as text", path),
    }
}

/// Show the status of the pg_git extension.
#[pg_extern]
pub fn git_status() -> String {
    let enabled = config::PG_GIT_ENABLED.get();
    let port = config::PG_GIT_HTTP_PORT.get();

    let repo_count = Spi::get_one::<i64>("SELECT COUNT(*)::bigint FROM pggit.repos")
        .unwrap_or(Some(0))
        .unwrap_or(0);

    if enabled {
        format!(
            "pg_git: HTTP endpoint on port {}, {} repo(s) managed",
            port, repo_count
        )
    } else {
        format!("pg_git: disabled, {} repo(s) registered", repo_count)
    }
}

/// Query the commit log for a repository, optionally filtered by file path.
///
/// Returns commits in reverse chronological order.
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn git_log(
    repo_id: &str,
    path: default!(Option<&str>, "NULL"),
    max_rows: default!(i32, "100"),
) -> TableIterator<
    'static,
    (
        name!(hash, String),
        name!(parent_hash, Option<String>),
        name!(message, String),
        name!(author, String),
        name!(author_email, Option<String>),
        name!(committed_at, String),
    ),
> {
    let sql = if let Some(file_path) = path {
        format!(
            "SELECT c.hash, c.parent_hash, c.message, c.author, c.author_email,
                    c.committed_at::text
             FROM pggit.commits c
             JOIN pggit.file_history fh ON c.repo_id = fh.repo_id AND c.hash = fh.commit_hash
             WHERE c.repo_id = '{}' AND fh.path LIKE '{}'
             ORDER BY c.committed_at DESC, c.synced_at DESC, c.ctid DESC
             LIMIT {}",
            repo_id.replace('\'', "''"),
            file_path.replace('\'', "''").replace('*', "%"),
            max_rows
        )
    } else {
        format!(
            "SELECT hash, parent_hash, message, author, author_email,
                    committed_at::text
             FROM pggit.commits
             WHERE repo_id = '{}'
             ORDER BY committed_at DESC, synced_at DESC, ctid DESC
             LIMIT {}",
            repo_id.replace('\'', "''"),
            max_rows
        )
    };

    let rows: Vec<_> = Spi::connect(|client| {
        let table = client.select(&sql, None, &[])?;
        let mut results = Vec::new();
        for row in table {
            let hash: String = row.get(1)?.unwrap_or_default();
            let parent_hash: Option<String> = row.get(2)?;
            let message: String = row.get(3)?.unwrap_or_default();
            let author: String = row.get(4)?.unwrap_or_default();
            let author_email: Option<String> = row.get(5)?;
            let committed_at: String = row.get(6)?.unwrap_or_default();
            results.push((
                hash,
                parent_hash,
                message,
                author,
                author_email,
                committed_at,
            ));
        }
        Ok::<_, spi::Error>(results)
    })
    .unwrap_or_else(|e| pgrx::error!("git_log query failed: {}", e));

    TableIterator::new(rows)
}

/// Diff two refs (commit hashes or ref names), returning changed files.
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn git_diff(
    repo_id: &str,
    from_ref: &str,
    to_ref: &str,
) -> TableIterator<
    'static,
    (
        name!(path, String),
        name!(change_type, String),
        name!(lines_added, Option<i32>),
        name!(lines_removed, Option<i32>),
    ),
> {
    let repo_path = get_repo_path(repo_id);

    // Resolve refs to commit hashes
    let from_hash = resolve_ref(&repo_path, from_ref);
    let to_hash = resolve_ref(&repo_path, to_ref);

    let diff = match git::diff_commits(&repo_path, &from_hash, &to_hash) {
        Ok(d) => d,
        Err(e) => pgrx::error!("{}", e),
    };

    // Get line stats
    let stats = git::diff_stat(&repo_path, &from_hash, &to_hash).unwrap_or_default();
    let stat_map: std::collections::HashMap<String, (Option<i32>, Option<i32>)> = stats
        .into_iter()
        .map(|(path, added, removed)| (path, (added, removed)))
        .collect();

    let rows: Vec<_> = diff
        .into_iter()
        .map(|entry| {
            let (added, removed) = stat_map.get(&entry.path).copied().unwrap_or((None, None));
            (entry.path, entry.change_type, added, removed)
        })
        .collect();

    TableIterator::new(rows)
}

// ---------------------------------------------------------------------------
// Permission management
// ---------------------------------------------------------------------------

/// Grant a role access to a repository (or a specific scope within it).
///
/// Requires admin access on the repo (or superuser).
#[pg_extern]
pub fn git_grant(
    repo_id: &str,
    role_name: &str,
    access: &str,
    scope_type: default!(&str, "'repo'"),
    scope_value: default!(&str, "'*'"),
) -> String {
    // Validate inputs
    if !["read", "write", "admin"].contains(&access) {
        pgrx::error!(
            "Invalid access level '{}': must be read, write, or admin",
            access
        );
    }
    if !["repo", "branch", "path"].contains(&scope_type) {
        pgrx::error!(
            "Invalid scope_type '{}': must be repo, branch, or path",
            scope_type
        );
    }

    // Check caller has admin (or is superuser)
    require_admin(repo_id);

    // Verify the repo exists (will error if not found via RLS or missing)
    let _ = get_repo_path(repo_id);

    let result = Spi::run_with_args(
        "INSERT INTO pggit.permissions (repo_id, role_name, scope_type, scope_value, access)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (repo_id, role_name, scope_type, scope_value)
         DO UPDATE SET access = EXCLUDED.access, granted_by = current_user, granted_at = now()",
        &[
            repo_id.into(),
            role_name.into(),
            scope_type.into(),
            scope_value.into(),
            access.into(),
        ],
    );
    if let Err(e) = result {
        pgrx::error!("Failed to grant permission: {}", e);
    }

    format!(
        "Granted {} {} on {} ({}:{})",
        role_name, access, repo_id, scope_type, scope_value
    )
}

/// Revoke a role's access to a repository.
///
/// Removes all permission rows matching the repo, role, and access level.
#[pg_extern]
pub fn git_revoke(repo_id: &str, role_name: &str, access: &str) -> String {
    if !["read", "write", "admin"].contains(&access) {
        pgrx::error!(
            "Invalid access level '{}': must be read, write, or admin",
            access
        );
    }

    require_admin(repo_id);

    let result = Spi::run_with_args(
        "DELETE FROM pggit.permissions WHERE repo_id = $1 AND role_name = $2 AND access = $3",
        &[repo_id.into(), role_name.into(), access.into()],
    );
    if let Err(e) = result {
        pgrx::error!("Failed to revoke permission: {}", e);
    }

    format!("Revoked {} {} on {}", role_name, access, repo_id)
}

/// Protect a branch so only specified roles can push to it.
///
/// Pass an empty array to unprotect the branch.
#[pg_extern]
pub fn git_protect_branch(repo_id: &str, branch_name: &str, allowed_roles: Vec<String>) -> String {
    require_admin(repo_id);

    let protected = !allowed_roles.is_empty();
    let roles_array = if allowed_roles.is_empty() {
        "NULL".to_string()
    } else {
        let escaped: Vec<String> = allowed_roles
            .iter()
            .map(|r| format!("'{}'", r.replace('\'', "''")))
            .collect();
        format!("ARRAY[{}]::text[]", escaped.join(", "))
    };

    let sql = format!(
        "INSERT INTO pggit.branches (repo_id, name, protected, allowed_roles)
         VALUES ($1, $2, {}, {})
         ON CONFLICT (repo_id, name) DO UPDATE SET
           protected = EXCLUDED.protected, allowed_roles = EXCLUDED.allowed_roles",
        protected, roles_array
    );
    let result = Spi::run_with_args(&sql, &[repo_id.into(), branch_name.into()]);
    if let Err(e) = result {
        pgrx::error!("Failed to protect branch: {}", e);
    }

    if protected {
        format!(
            "Protected branch '{}' on repo '{}' — allowed: {:?}",
            branch_name, repo_id, allowed_roles
        )
    } else {
        format!("Unprotected branch '{}' on repo '{}'", branch_name, repo_id)
    }
}

// ---------------------------------------------------------------------------
// Branch management
// ---------------------------------------------------------------------------

/// Create a new branch in a repository.
///
/// Optionally specify a start point (commit hash, tag, or branch name).
/// Defaults to starting from the current HEAD.
#[pg_extern]
pub fn git_branch(
    repo_id: &str,
    branch_name: &str,
    start_point: default!(Option<&str>, "NULL"),
) -> String {
    let repo_path = get_repo_path(repo_id);
    require_write(repo_id);

    if let Err(e) = git::create_branch(&repo_path, branch_name, start_point) {
        pgrx::error!("{}", e);
    }

    // Get the head hash of the new branch
    let head_hash = git::branch_head_hash(&repo_path, branch_name).ok();

    // Register in branches table
    let head_val = head_hash
        .as_deref()
        .map(|h| format!("'{}'", h))
        .unwrap_or_else(|| "NULL".to_string());

    let sql = format!(
        "INSERT INTO pggit.branches (repo_id, name, head_commit)
         VALUES ($1, $2, {})
         ON CONFLICT (repo_id, name) DO UPDATE SET head_commit = {}",
        head_val, head_val
    );
    if let Err(e) = Spi::run_with_args(&sql, &[repo_id.into(), branch_name.into()]) {
        pgrx::error!("Failed to register branch: {}", e);
    }

    format!("Created branch '{}' on repo '{}'", branch_name, repo_id)
}

/// Switch the working tree to a different branch.
#[pg_extern]
pub fn git_checkout(repo_id: &str, branch_name: &str) -> String {
    let repo_path = get_repo_path(repo_id);

    if let Err(e) = git::checkout(&repo_path, branch_name) {
        pgrx::error!("{}", e);
    }

    format!("Switched to branch '{}'", branch_name)
}

/// Merge a source branch into a target branch (fast-forward only).
///
/// If target_branch is NULL, merges into the current branch.
/// Returns the resulting commit hash.
#[pg_extern]
pub fn git_merge(
    repo_id: &str,
    source_branch: &str,
    target_branch: default!(Option<&str>, "NULL"),
) -> String {
    let repo_path = get_repo_path(repo_id);
    require_write(repo_id);

    // Determine the actual target
    let target = match target_branch {
        Some(t) => t.to_string(),
        None => git::current_branch(&repo_path).unwrap_or_else(|_| "main".to_string()),
    };

    check_branch_protection(repo_id, &target);

    // Checkout target if not already on it
    let current = git::current_branch(&repo_path).unwrap_or_default();
    if current != target {
        if let Err(e) = git::checkout(&repo_path, &target) {
            pgrx::error!("Failed to checkout target branch '{}': {}", target, e);
        }
    }

    let hash = match git::merge_fast_forward(&repo_path, source_branch) {
        Ok(h) => h,
        Err(e) => pgrx::error!("{}", e),
    };

    // Update branches.head_commit for the target
    let sql = format!(
        "INSERT INTO pggit.branches (repo_id, name, head_commit)
         VALUES ($1, $2, '{}')
         ON CONFLICT (repo_id, name) DO UPDATE SET head_commit = '{}'",
        hash, hash
    );
    if let Err(e) = Spi::run_with_args(&sql, &[repo_id.into(), target.as_str().into()]) {
        pgrx::warning!("Failed to update branch head: {}", e);
    }

    hash
}

/// List all branches in a repository with protection info.
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn git_branches(
    repo_id: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(head_commit, Option<String>),
        name!(is_current, bool),
        name!(protected, bool),
    ),
> {
    let repo_path = get_repo_path(repo_id);

    let git_branches = match git::list_branches(&repo_path) {
        Ok(b) => b,
        Err(e) => pgrx::error!("{}", e),
    };

    // Load protection info from branches table
    let protection: std::collections::HashMap<String, bool> = Spi::connect(|client| {
        let table = client.select(
            "SELECT name, protected FROM pggit.branches WHERE repo_id = $1",
            None,
            &[repo_id.into()],
        )?;
        let mut map = std::collections::HashMap::new();
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let is_protected: bool = row.get(2)?.unwrap_or(false);
            map.insert(name, is_protected);
        }
        Ok::<_, spi::Error>(map)
    })
    .unwrap_or_default();

    let rows: Vec<_> = git_branches
        .into_iter()
        .map(|b| {
            let is_protected = protection.get(&b.name).copied().unwrap_or(false);
            (
                b.name,
                if b.commit_hash.is_empty() {
                    None
                } else {
                    Some(b.commit_hash)
                },
                b.is_current,
                is_protected,
            )
        })
        .collect();

    TableIterator::new(rows)
}

/// Delete a branch from a repository.
///
/// Requires admin access. Cannot delete the default or current branch.
#[pg_extern]
pub fn git_delete_branch(repo_id: &str, branch_name: &str) -> String {
    let repo_path = get_repo_path(repo_id);
    require_admin(repo_id);

    // Block deleting current branch
    let current = git::current_branch(&repo_path).unwrap_or_default();
    if current == branch_name {
        pgrx::error!(
            "Cannot delete the currently checked-out branch '{}'",
            branch_name
        );
    }

    // Block deleting default branch
    let default_branch = Spi::get_one_with_args::<String>(
        "SELECT default_branch FROM pggit.repos WHERE id = $1",
        &[repo_id.into()],
    )
    .unwrap_or(Some("main".to_string()))
    .unwrap_or_else(|| "main".to_string());

    if branch_name == default_branch {
        pgrx::error!("Cannot delete the default branch '{}'", branch_name);
    }

    // Delete from git
    if let Err(e) = git::delete_branch(&repo_path, branch_name) {
        pgrx::error!("{}", e);
    }

    // Clean up files for this branch
    let _ = Spi::run_with_args(
        "DELETE FROM pggit.files WHERE repo_id = $1 AND branch = $2",
        &[repo_id.into(), branch_name.into()],
    );

    // Remove from branches table
    let _ = Spi::run_with_args(
        "DELETE FROM pggit.branches WHERE repo_id = $1 AND name = $2",
        &[repo_id.into(), branch_name.into()],
    );

    format!("Deleted branch '{}' from repo '{}'", branch_name, repo_id)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_ref(repo_path: &PathBuf, refspec: &str) -> String {
    let output = std::process::Command::new("git")
        .args(["rev-parse", refspec])
        .current_dir(repo_path)
        .env("GIT_DIR", repo_path.join(".git"))
        .output();

    match output {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => pgrx::error!("Cannot resolve ref: {}", refspec),
    }
}

fn is_superuser() -> bool {
    Spi::get_one::<bool>("SELECT current_setting('is_superuser') = 'on'")
        .unwrap_or(Some(false))
        .unwrap_or(false)
}

fn require_admin(repo_id: &str) {
    if is_superuser() {
        return;
    }

    let has_admin = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(
            SELECT 1 FROM pggit.permissions
            WHERE repo_id = $1 AND role_name = current_user AND access = 'admin'
        )",
        &[repo_id.into()],
    );

    if !matches!(has_admin, Ok(Some(true))) {
        pgrx::error!(
            "Permission denied: admin access required on repo '{}'",
            repo_id
        );
    }
}

fn require_write(repo_id: &str) {
    if is_superuser() {
        return;
    }

    let has_write = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(
            SELECT 1 FROM pggit.permissions
            WHERE repo_id = $1 AND role_name = current_user
              AND access IN ('write', 'admin') AND scope_type = 'repo'
        )",
        &[repo_id.into()],
    );

    if !matches!(has_write, Ok(Some(true))) {
        pgrx::error!(
            "Permission denied: write access required on repo '{}'",
            repo_id
        );
    }
}

fn check_branch_protection(repo_id: &str, branch: &str) {
    if is_superuser() {
        return;
    }

    let result = Spi::connect(|client| {
        let mut table = client.select(
            "SELECT protected, allowed_roles FROM pggit.branches WHERE repo_id = $1 AND name = $2",
            None,
            &[repo_id.into(), branch.into()],
        )?;
        if let Some(row) = table.next() {
            let protected: bool = row.get(1)?.unwrap_or(false);
            if !protected {
                return Ok(true); // Not protected, allow
            }
            // Check if current user is in allowed_roles
            let roles: Option<Vec<String>> = row.get(2)?;
            if let Some(roles) = roles {
                let current_user = Spi::get_one::<String>("SELECT current_user::text")
                    .unwrap_or(Some("unknown".to_string()))
                    .unwrap_or_else(|| "unknown".to_string());
                return Ok(roles.contains(&current_user));
            }
            return Ok(false); // Protected with no allowed roles = nobody can push
        }
        Ok::<bool, spi::Error>(true) // No branch entry = not protected
    });

    if !matches!(result, Ok(true)) {
        pgrx::error!(
            "Permission denied: branch '{}' is protected on repo '{}'",
            branch,
            repo_id
        );
    }
}

fn get_repo_path(repo_id: &str) -> PathBuf {
    let path = Spi::get_one_with_args::<String>(
        "SELECT path FROM pggit.repos WHERE id = $1",
        &[repo_id.into()],
    );

    match path {
        Ok(Some(p)) => PathBuf::from(p),
        _ => pgrx::error!("Repository '{}' not found", repo_id),
    }
}

fn get_author_email(role: &str) -> Option<String> {
    // Check if user_profiles table exists and has an email for this role
    let result = Spi::get_one_with_args::<String>(
        "SELECT email FROM pggit.user_profiles WHERE role_name = $1",
        &[role.into()],
    );
    match result {
        Ok(Some(email)) => Some(email),
        _ => None, // Table doesn't exist or no row — that's fine
    }
}
