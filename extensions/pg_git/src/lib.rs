//! pg_git — Git version control backed by PostgreSQL tables.
//!
//! Wraps a real git repository on disk with queryable metadata tables in Postgres.
//! Provides an HTTP git endpoint for clone/push/fetch and SQL functions for
//! content management.

pub mod config;
pub mod error;
pub mod git;
pub mod metadata;
pub mod server;
pub mod sql;
pub mod worker;

// Re-export worker entry points so they appear as dynamic symbols
pub use worker::pg_git_http_worker_main;
pub use worker::pg_git_sync_worker_main;

use pgrx::prelude::*;

pgrx::pg_module_magic!();

// ===========================================================================
// Bootstrap SQL — creates schema tables on CREATE EXTENSION
// ===========================================================================

extension_sql!(
    r#"
    -- Repository registry
    CREATE TABLE IF NOT EXISTS repos (
        id          TEXT PRIMARY KEY,
        path        TEXT NOT NULL UNIQUE,
        description TEXT,
        default_branch TEXT NOT NULL DEFAULT 'main',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_by  TEXT NOT NULL DEFAULT current_user
    );

    -- Commit log synced from git
    CREATE TABLE IF NOT EXISTS commits (
        repo_id     TEXT NOT NULL REFERENCES repos(id),
        hash        TEXT NOT NULL,
        parent_hash TEXT,
        message     TEXT NOT NULL,
        author      TEXT NOT NULL,
        author_email TEXT,
        committed_at TIMESTAMPTZ NOT NULL,
        synced_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (repo_id, hash)
    );

    CREATE INDEX IF NOT EXISTS idx_commits_repo_date
        ON commits(repo_id, committed_at DESC);

    -- Sync state tracking
    CREATE TABLE IF NOT EXISTS sync_state (
        repo_id         TEXT PRIMARY KEY REFERENCES repos(id),
        last_synced_hash TEXT,
        last_synced_at  TIMESTAMPTZ
    );

    -- Current state of tracked files with auto-extracted metadata
    CREATE TABLE IF NOT EXISTS files (
        repo_id     TEXT NOT NULL REFERENCES repos(id),
        path        TEXT NOT NULL,
        branch      TEXT NOT NULL DEFAULT 'main',
        current_hash TEXT NOT NULL,
        mime_type   TEXT,
        size_bytes  BIGINT,
        encoding    TEXT,
        language    TEXT,
        line_count  INTEGER,
        title       TEXT,
        owner       TEXT,
        tags        JSONB DEFAULT '[]'::jsonb,
        custom_meta JSONB DEFAULT '{}'::jsonb,
        created_at  TIMESTAMPTZ,
        updated_at  TIMESTAMPTZ,
        updated_by  TEXT,
        PRIMARY KEY (repo_id, branch, path)
    );

    -- Change log per file (one row per commit that touched the file)
    CREATE TABLE IF NOT EXISTS file_history (
        repo_id     TEXT NOT NULL,
        path        TEXT NOT NULL,
        commit_hash TEXT NOT NULL,
        change_type TEXT NOT NULL,
        old_hash    TEXT,
        new_hash    TEXT,
        old_path    TEXT,
        lines_added   INTEGER,
        lines_removed INTEGER,
        committed_at TIMESTAMPTZ NOT NULL,
        author      TEXT NOT NULL,
        PRIMARY KEY (repo_id, path, commit_hash)
    );

    CREATE INDEX IF NOT EXISTS idx_file_history_repo_commit
        ON file_history(repo_id, commit_hash);

    -- Optional: user profile mapping (role -> email for git commits)
    CREATE TABLE IF NOT EXISTS user_profiles (
        role_name    TEXT PRIMARY KEY,
        display_name TEXT,
        email        TEXT
    );

    -- Role-based access control
    CREATE TABLE IF NOT EXISTS permissions (
        repo_id     TEXT NOT NULL REFERENCES repos(id),
        role_name   TEXT NOT NULL,
        scope_type  TEXT NOT NULL CHECK (scope_type IN ('repo', 'branch', 'path')),
        scope_value TEXT NOT NULL DEFAULT '*',
        access      TEXT NOT NULL CHECK (access IN ('read', 'write', 'admin')),
        granted_by  TEXT NOT NULL DEFAULT current_user,
        granted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (repo_id, role_name, scope_type, scope_value)
    );

    -- Branch metadata and protection
    CREATE TABLE IF NOT EXISTS branches (
        repo_id       TEXT NOT NULL REFERENCES repos(id),
        name          TEXT NOT NULL,
        head_commit   TEXT,
        protected     BOOLEAN NOT NULL DEFAULT false,
        allowed_roles TEXT[],
        PRIMARY KEY (repo_id, name)
    );

    -- =======================================================================
    -- Row-Level Security policies
    -- =======================================================================

    -- repos: visible if user has any permission on it
    ALTER TABLE repos ENABLE ROW LEVEL SECURITY;
    CREATE POLICY repos_read ON repos FOR SELECT USING (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = repos.id AND p.role_name = current_user)
    );
    CREATE POLICY repos_admin ON repos FOR ALL USING (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = repos.id AND p.role_name = current_user
                  AND p.access = 'admin')
    );

    -- commits: read if any permission on repo; write if write/admin
    ALTER TABLE commits ENABLE ROW LEVEL SECURITY;
    CREATE POLICY commits_read ON commits FOR SELECT USING (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = commits.repo_id AND p.role_name = current_user
                  AND p.access IN ('read', 'write', 'admin')
                  AND (p.scope_type = 'repo' OR p.scope_type = 'branch' OR p.scope_type = 'path'))
    );
    CREATE POLICY commits_write ON commits FOR INSERT WITH CHECK (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = commits.repo_id AND p.role_name = current_user
                  AND p.access IN ('write', 'admin') AND p.scope_type = 'repo')
    );

    -- files: scope-aware read and write
    ALTER TABLE files ENABLE ROW LEVEL SECURITY;
    CREATE POLICY files_read ON files FOR SELECT USING (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = files.repo_id AND p.role_name = current_user
                  AND p.access IN ('read', 'write', 'admin')
                  AND ((p.scope_type = 'repo')
                    OR (p.scope_type = 'branch' AND p.scope_value = files.branch)
                    OR (p.scope_type = 'path' AND files.path LIKE replace(p.scope_value, '*', '%'))))
    );
    CREATE POLICY files_write ON files FOR UPDATE USING (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = files.repo_id AND p.role_name = current_user
                  AND p.access IN ('write', 'admin')
                  AND ((p.scope_type = 'repo')
                    OR (p.scope_type = 'path' AND files.path LIKE replace(p.scope_value, '*', '%'))))
    );

    -- file_history: read if any permission on repo
    ALTER TABLE file_history ENABLE ROW LEVEL SECURITY;
    CREATE POLICY file_history_read ON file_history FOR SELECT USING (
        EXISTS (SELECT 1 FROM permissions p
                WHERE p.repo_id = file_history.repo_id AND p.role_name = current_user
                  AND p.access IN ('read', 'write', 'admin')
                  AND (p.scope_type = 'repo' OR p.scope_type = 'branch' OR p.scope_type = 'path'))
    );

    -- =======================================================================
    -- Public table access (RLS policies handle row-level filtering)
    -- =======================================================================
    GRANT USAGE ON SCHEMA @extschema@ TO PUBLIC;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA @extschema@ TO PUBLIC;
    "#,
    name = "bootstrap",
    bootstrap
);

// ===========================================================================
// Extension init
// ===========================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    config::register_gucs();

    // Register background workers unconditionally
    // (they check pg_git.enabled inside worker_main)
    worker::register_http_worker();
    worker::register_sync_worker();

    pgrx::log!("pg_git: initialized, registering 2 background workers");
}

// ===========================================================================
// Extension docs
// ===========================================================================

/// Returns the extension documentation.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../../../design/pg_git/README.md")
}

// ===========================================================================
// Integration tests (require Postgres)
// ===========================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    /// Helper: bootstrap schema tables for tests (pg_test runs in a transaction
    /// where the bootstrap SQL hasn't executed)
    fn bootstrap_tables() {
        Spi::run("CREATE SCHEMA IF NOT EXISTS pggit").unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.repos (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                description TEXT,
                default_branch TEXT NOT NULL DEFAULT 'main',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_by TEXT NOT NULL DEFAULT current_user
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.commits (
                repo_id TEXT NOT NULL REFERENCES pggit.repos(id),
                hash TEXT NOT NULL,
                parent_hash TEXT,
                message TEXT NOT NULL,
                author TEXT NOT NULL,
                author_email TEXT,
                committed_at TIMESTAMPTZ NOT NULL,
                synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (repo_id, hash)
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.sync_state (
                repo_id TEXT PRIMARY KEY REFERENCES pggit.repos(id),
                last_synced_hash TEXT,
                last_synced_at TIMESTAMPTZ
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.user_profiles (
                role_name TEXT PRIMARY KEY,
                display_name TEXT,
                email TEXT
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.files (
                repo_id TEXT NOT NULL REFERENCES pggit.repos(id),
                path TEXT NOT NULL,
                branch TEXT NOT NULL DEFAULT 'main',
                current_hash TEXT NOT NULL,
                mime_type TEXT,
                size_bytes BIGINT,
                encoding TEXT,
                language TEXT,
                line_count INTEGER,
                title TEXT,
                owner TEXT,
                tags JSONB DEFAULT '[]'::jsonb,
                custom_meta JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ,
                updated_by TEXT,
                PRIMARY KEY (repo_id, branch, path)
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.file_history (
                repo_id TEXT NOT NULL,
                path TEXT NOT NULL,
                commit_hash TEXT NOT NULL,
                change_type TEXT NOT NULL,
                old_hash TEXT,
                new_hash TEXT,
                old_path TEXT,
                lines_added INTEGER,
                lines_removed INTEGER,
                committed_at TIMESTAMPTZ NOT NULL,
                author TEXT NOT NULL,
                PRIMARY KEY (repo_id, path, commit_hash)
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.permissions (
                repo_id TEXT NOT NULL REFERENCES pggit.repos(id),
                role_name TEXT NOT NULL,
                scope_type TEXT NOT NULL CHECK (scope_type IN ('repo', 'branch', 'path')),
                scope_value TEXT NOT NULL DEFAULT '*',
                access TEXT NOT NULL CHECK (access IN ('read', 'write', 'admin')),
                granted_by TEXT NOT NULL DEFAULT current_user,
                granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (repo_id, role_name, scope_type, scope_value)
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pggit.branches (
                repo_id TEXT NOT NULL REFERENCES pggit.repos(id),
                name TEXT NOT NULL,
                head_commit TEXT,
                protected BOOLEAN NOT NULL DEFAULT false,
                allowed_roles TEXT[],
                PRIMARY KEY (repo_id, name)
            )",
        )
        .unwrap();
    }

    fn temp_repo_path(name: &str) -> String {
        format!("/tmp/pg_git_test_{}", name)
    }

    fn cleanup_repo(path: &str) {
        let _ = std::fs::remove_dir_all(path);
    }

    #[pg_test]
    fn test_git_init() {
        bootstrap_tables();
        let path = temp_repo_path("init");
        cleanup_repo(&path);

        let result = crate::sql::git_init("test_init", Some(&path), None);
        assert_eq!(result, "test_init");

        // Verify repos table entry
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*)::bigint FROM pggit.repos WHERE id = 'test_init'")
                .unwrap()
                .unwrap_or(0);
        assert_eq!(count, 1);

        // Verify sync_state entry
        let sync_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.sync_state WHERE repo_id = 'test_init'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(sync_count, 1);

        // Verify .git directory exists
        assert!(std::path::Path::new(&path).join(".git").exists());

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_add_and_commit() {
        bootstrap_tables();
        let path = temp_repo_path("add_commit");
        cleanup_repo(&path);

        crate::sql::git_init("test_ac", Some(&path), None);
        crate::sql::git_add("test_ac", "hello.md", Some("# Hello World"));
        let hash = crate::sql::git_commit("test_ac", "Add hello");

        // Hash should be 40 hex chars
        assert_eq!(hash.len(), 40);

        // Verify commit in table
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'test_ac'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_show() {
        bootstrap_tables();
        let path = temp_repo_path("show");
        cleanup_repo(&path);

        crate::sql::git_init("test_show", Some(&path), None);
        crate::sql::git_add(
            "test_show",
            "doc.md",
            Some("# Design Document\n\nContent here."),
        );
        crate::sql::git_commit("test_show", "Add doc");

        let content = crate::sql::git_show("test_show", "doc.md", None);
        assert_eq!(content, "# Design Document\n\nContent here.");

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_show_at_specific_commit() {
        bootstrap_tables();
        let path = temp_repo_path("show_commit");
        cleanup_repo(&path);

        crate::sql::git_init("test_sc", Some(&path), None);

        // First version
        crate::sql::git_add("test_sc", "readme.md", Some("version 1"));
        let hash1 = crate::sql::git_commit("test_sc", "v1");

        // Second version
        crate::sql::git_add("test_sc", "readme.md", Some("version 2"));
        crate::sql::git_commit("test_sc", "v2");

        // Show at first commit
        let v1 = crate::sql::git_show("test_sc", "readme.md", Some(&hash1));
        assert_eq!(v1, "version 1");

        // Show at HEAD
        let v2 = crate::sql::git_show("test_sc", "readme.md", None);
        assert_eq!(v2, "version 2");

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_status() {
        bootstrap_tables();
        let status = crate::sql::git_status();
        assert!(status.contains("pg_git"));
    }

    #[pg_test]
    fn test_multiple_commits_tracked() {
        bootstrap_tables();
        let path = temp_repo_path("multi_commits");
        cleanup_repo(&path);

        crate::sql::git_init("test_mc", Some(&path), None);

        crate::sql::git_add("test_mc", "a.md", Some("aaa"));
        crate::sql::git_commit("test_mc", "Add a");

        crate::sql::git_add("test_mc", "b.md", Some("bbb"));
        crate::sql::git_commit("test_mc", "Add b");

        crate::sql::git_add("test_mc", "c.md", Some("ccc"));
        crate::sql::git_commit("test_mc", "Add c");

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'test_mc'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 3);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_nested_file_paths() {
        bootstrap_tables();
        let path = temp_repo_path("nested");
        cleanup_repo(&path);

        crate::sql::git_init("test_nested", Some(&path), None);
        crate::sql::git_add("test_nested", "docs/design/api/spec.md", Some("# API Spec"));
        crate::sql::git_commit("test_nested", "Add nested file");

        let content = crate::sql::git_show("test_nested", "docs/design/api/spec.md", None);
        assert_eq!(content, "# API Spec");

        cleanup_repo(&path);
    }

    // =====================================================================
    // Phase 2 tests: git_log, git_diff, files, file_history
    // =====================================================================

    #[pg_test]
    fn test_git_log_basic() {
        bootstrap_tables();
        let path = temp_repo_path("log_basic");
        cleanup_repo(&path);

        crate::sql::git_init("test_log", Some(&path), None);
        crate::sql::git_add("test_log", "a.md", Some("aaa"));
        crate::sql::git_commit("test_log", "First commit");
        crate::sql::git_add("test_log", "b.md", Some("bbb"));
        crate::sql::git_commit("test_log", "Second commit");

        // Use SPI to call git_log and check results
        let count = Spi::get_one::<i64>("SELECT COUNT(*)::bigint FROM pggit.git_log('test_log')")
            .unwrap()
            .unwrap_or(0);
        assert_eq!(count, 2);

        // Check ordering (newest first)
        let msg = Spi::get_one::<String>("SELECT message FROM pggit.git_log('test_log') LIMIT 1")
            .unwrap()
            .unwrap_or_default();
        assert_eq!(msg, "Second commit");

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_log_with_limit() {
        bootstrap_tables();
        let path = temp_repo_path("log_limit");
        cleanup_repo(&path);

        crate::sql::git_init("test_ll", Some(&path), None);
        for i in 0..5 {
            crate::sql::git_add(
                "test_ll",
                &format!("{}.md", i),
                Some(&format!("content {}", i)),
            );
            crate::sql::git_commit("test_ll", &format!("Commit {}", i));
        }

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.git_log('test_ll', max_rows := 3)",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 3);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_log_with_path_filter() {
        bootstrap_tables();
        let path = temp_repo_path("log_path");
        cleanup_repo(&path);

        crate::sql::git_init("test_lp", Some(&path), None);

        // Commit 1: add docs/a.md
        crate::sql::git_add("test_lp", "docs/a.md", Some("aaa"));
        let hash1 = crate::sql::git_commit("test_lp", "Add docs/a");

        // Commit 2: add src/b.rs (different path)
        crate::sql::git_add("test_lp", "src/b.rs", Some("fn main() {}"));
        let hash2 = crate::sql::git_commit("test_lp", "Add src/b");

        // Manually populate file_history so git_log path filter works
        let fh1 = format!(
            "INSERT INTO pggit.file_history (repo_id, path, commit_hash, change_type, committed_at, author)
             VALUES ('test_lp', 'docs/a.md', '{}', 'add', now(), 'test')", hash1
        );
        Spi::run(&fh1).unwrap();
        let fh2 = format!(
            "INSERT INTO pggit.file_history (repo_id, path, commit_hash, change_type, committed_at, author)
             VALUES ('test_lp', 'src/b.rs', '{}', 'add', now(), 'test')", hash2
        );
        Spi::run(&fh2).unwrap();

        // Filter by docs/* path
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.git_log('test_lp', path := 'docs/%')",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_diff_between_commits() {
        bootstrap_tables();
        let path = temp_repo_path("diff");
        cleanup_repo(&path);

        crate::sql::git_init("test_diff", Some(&path), None);
        crate::sql::git_add("test_diff", "a.md", Some("line 1\nline 2\n"));
        let hash1 = crate::sql::git_commit("test_diff", "v1");

        crate::sql::git_add(
            "test_diff",
            "a.md",
            Some("line 1\nline 2 modified\nline 3\n"),
        );
        crate::sql::git_add("test_diff", "b.md", Some("new file"));
        let hash2 = crate::sql::git_commit("test_diff", "v2");

        // Use git_diff via SPI
        let count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*)::bigint FROM pggit.git_diff('test_diff', '{}', '{}')",
            hash1, hash2
        ))
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 2); // a.md modified + b.md added

        // Check change types
        let add_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*)::bigint FROM pggit.git_diff('test_diff', '{}', '{}') WHERE change_type = 'add'",
            hash1, hash2
        ))
        .unwrap()
        .unwrap_or(0);
        assert_eq!(add_count, 1);

        let mod_count = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*)::bigint FROM pggit.git_diff('test_diff', '{}', '{}') WHERE change_type = 'modify'",
            hash1, hash2
        ))
        .unwrap()
        .unwrap_or(0);
        assert_eq!(mod_count, 1);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_files_table_populated_on_commit() {
        bootstrap_tables();
        let path = temp_repo_path("files_pop");
        cleanup_repo(&path);

        crate::sql::git_init("test_fp", Some(&path), None);
        crate::sql::git_add(
            "test_fp",
            "design/api.md",
            Some("# API Design\n\nContent here.\n"),
        );
        crate::sql::git_commit("test_fp", "Add design doc");

        // The git_commit SQL function inserts into commits + sync_state.
        // For files table population, we need the sync worker or trigger_sync.
        // In pg_test context, manually call the metadata extraction.
        let content = b"# API Design\n\nContent here.\n";
        let meta = crate::metadata::extract_metadata("design/api.md", content);
        let title = crate::metadata::extract_title("design/api.md", content);

        assert_eq!(meta.mime_type, "text/markdown");
        assert_eq!(meta.encoding, "utf-8");
        assert_eq!(meta.language.as_deref(), Some("markdown"));
        assert_eq!(meta.line_count, Some(3));
        assert_eq!(title, Some("API Design".to_string()));

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_file_history_populated() {
        bootstrap_tables();
        let path = temp_repo_path("fh_pop");
        cleanup_repo(&path);

        crate::sql::git_init("test_fh", Some(&path), None);
        crate::sql::git_add("test_fh", "readme.md", Some("v1"));
        let hash1 = crate::sql::git_commit("test_fh", "v1");

        // Manually simulate what sync worker does: diff_commit_parent
        let diff =
            crate::git::diff_commit_parent(&std::path::PathBuf::from(&path), &hash1).unwrap();

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "readme.md");
        assert_eq!(diff[0].change_type, "add");

        // Second commit modifies
        crate::sql::git_add("test_fh", "readme.md", Some("v2"));
        let hash2 = crate::sql::git_commit("test_fh", "v2");

        let diff2 =
            crate::git::diff_commit_parent(&std::path::PathBuf::from(&path), &hash2).unwrap();

        assert_eq!(diff2.len(), 1);
        assert_eq!(diff2[0].path, "readme.md");
        assert_eq!(diff2[0].change_type, "modify");

        cleanup_repo(&path);
    }

    // =====================================================================
    // Phase 3 tests: IAM, permissions, RLS, branch protection
    // =====================================================================

    /// Helper: create a test role with access to pggit schema tables
    fn create_test_role(name: &str) {
        let _ = Spi::run(&format!(
            "DO $$ BEGIN CREATE ROLE {} LOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$",
            name
        ));
    }

    #[pg_test]
    fn test_init_auto_grants_admin() {
        bootstrap_tables();
        let path = temp_repo_path("iam_init");
        cleanup_repo(&path);

        crate::sql::git_init("test_iam_init", Some(&path), None);

        // Verify admin permission was auto-granted
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.permissions
             WHERE repo_id = 'test_iam_init' AND access = 'admin'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_grant_and_revoke() {
        bootstrap_tables();
        let path = temp_repo_path("iam_grant");
        cleanup_repo(&path);

        crate::sql::git_init("test_grant", Some(&path), None);

        // Grant read to a role
        create_test_role("test_reader");
        let result = crate::sql::git_grant("test_grant", "test_reader", "read", "repo", "*");
        assert!(result.contains("Granted"));

        // Verify permission row
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.permissions
             WHERE repo_id = 'test_grant' AND role_name = 'test_reader' AND access = 'read'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);

        // Revoke
        let revoke_result = crate::sql::git_revoke("test_grant", "test_reader", "read");
        assert!(revoke_result.contains("Revoked"));

        // Verify gone
        let count2 = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.permissions
             WHERE repo_id = 'test_grant' AND role_name = 'test_reader' AND access = 'read'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count2, 0);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_grant_with_path_scope() {
        bootstrap_tables();
        let path = temp_repo_path("iam_path_scope");
        cleanup_repo(&path);

        crate::sql::git_init("test_ps", Some(&path), None);

        create_test_role("test_path_writer");
        crate::sql::git_grant("test_ps", "test_path_writer", "write", "path", "docs/*");

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.permissions
             WHERE repo_id = 'test_ps' AND role_name = 'test_path_writer'
               AND scope_type = 'path' AND scope_value = 'docs/*'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_rls_repos_visibility() {
        bootstrap_tables();
        let path = temp_repo_path("iam_rls_repos");
        cleanup_repo(&path);

        // Enable RLS on repos for this test
        let _ = Spi::run("ALTER TABLE pggit.repos ENABLE ROW LEVEL SECURITY");
        let _ = Spi::run("DROP POLICY IF EXISTS repos_read ON pggit.repos");
        let _ = Spi::run(
            "CREATE POLICY repos_read ON pggit.repos FOR SELECT USING (
                EXISTS (SELECT 1 FROM pggit.permissions p
                        WHERE p.repo_id = pggit.repos.id AND p.role_name = current_user)
            )",
        );

        crate::sql::git_init("test_rls_vis", Some(&path), None);

        // Create two roles: one with permission, one without
        create_test_role("iam_viewer");
        create_test_role("iam_nobody");

        // Grant read to viewer
        crate::sql::git_grant("test_rls_vis", "iam_viewer", "read", "repo", "*");

        // As viewer: should see 1 repo
        Spi::run("SET ROLE iam_viewer").unwrap();
        let viewer_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.repos WHERE id = 'test_rls_vis'",
        )
        .unwrap()
        .unwrap_or(0);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(viewer_count, 1);

        // As nobody: should see 0 repos
        Spi::run("SET ROLE iam_nobody").unwrap();
        let nobody_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.repos WHERE id = 'test_rls_vis'",
        )
        .unwrap()
        .unwrap_or(0);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(nobody_count, 0);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_rls_files_path_scope() {
        bootstrap_tables();
        let path = temp_repo_path("iam_rls_files");
        cleanup_repo(&path);

        // Enable RLS on files
        let _ = Spi::run("ALTER TABLE pggit.files ENABLE ROW LEVEL SECURITY");
        let _ = Spi::run("DROP POLICY IF EXISTS files_read ON pggit.files");
        let _ = Spi::run(
            "CREATE POLICY files_read ON pggit.files FOR SELECT USING (
                EXISTS (SELECT 1 FROM pggit.permissions p
                        WHERE p.repo_id = pggit.files.repo_id AND p.role_name = current_user
                          AND p.access IN ('read', 'write', 'admin')
                          AND ((p.scope_type = 'repo')
                            OR (p.scope_type = 'path' AND pggit.files.path LIKE replace(p.scope_value, '*', '%'))))
            )",
        );

        crate::sql::git_init("test_rls_fp", Some(&path), None);

        // Insert some files manually (as superuser)
        Spi::run(
            "INSERT INTO pggit.files (repo_id, path, branch, current_hash, mime_type, size_bytes, encoding)
             VALUES ('test_rls_fp', 'docs/readme.md', 'main', 'abc123', 'text/markdown', 100, 'utf-8'),
                    ('test_rls_fp', 'src/main.rs', 'main', 'def456', 'text/x-rust', 200, 'utf-8'),
                    ('test_rls_fp', 'docs/api.md', 'main', 'ghi789', 'text/markdown', 150, 'utf-8')"
        ).unwrap();

        create_test_role("iam_docs_reader");
        // Grant path-scoped read only to docs/*
        crate::sql::git_grant("test_rls_fp", "iam_docs_reader", "read", "path", "docs/*");

        // As docs_reader: should see only 2 files (docs/readme.md, docs/api.md)
        Spi::run("SET ROLE iam_docs_reader").unwrap();
        let docs_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.files WHERE repo_id = 'test_rls_fp'",
        )
        .unwrap()
        .unwrap_or(0);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(docs_count, 2);

        // Grant repo-level read to another role — should see all 3
        create_test_role("iam_all_reader");
        crate::sql::git_grant("test_rls_fp", "iam_all_reader", "read", "repo", "*");
        Spi::run("SET ROLE iam_all_reader").unwrap();
        let all_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.files WHERE repo_id = 'test_rls_fp'",
        )
        .unwrap()
        .unwrap_or(0);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(all_count, 3);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_rls_commits_visibility() {
        bootstrap_tables();
        let path = temp_repo_path("iam_rls_commits");
        cleanup_repo(&path);

        // Enable RLS on commits
        let _ = Spi::run("ALTER TABLE pggit.commits ENABLE ROW LEVEL SECURITY");
        let _ = Spi::run("DROP POLICY IF EXISTS commits_read ON pggit.commits");
        let _ = Spi::run(
            "CREATE POLICY commits_read ON pggit.commits FOR SELECT USING (
                EXISTS (SELECT 1 FROM pggit.permissions p
                        WHERE p.repo_id = pggit.commits.repo_id AND p.role_name = current_user
                          AND p.access IN ('read', 'write', 'admin'))
            )",
        );

        crate::sql::git_init("test_rls_c", Some(&path), None);
        crate::sql::git_add("test_rls_c", "a.md", Some("content"));
        crate::sql::git_commit("test_rls_c", "First commit");

        // Role with read permission sees commits
        create_test_role("iam_commit_reader");
        crate::sql::git_grant("test_rls_c", "iam_commit_reader", "read", "repo", "*");

        Spi::run("SET ROLE iam_commit_reader").unwrap();
        let reader_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'test_rls_c'",
        )
        .unwrap()
        .unwrap_or(0);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(reader_count, 1);

        // Role without permission sees nothing
        create_test_role("iam_commit_nobody");
        Spi::run("SET ROLE iam_commit_nobody").unwrap();
        let nobody_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.commits WHERE repo_id = 'test_rls_c'",
        )
        .unwrap()
        .unwrap_or(0);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(nobody_count, 0);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_protect_branch() {
        bootstrap_tables();
        let path = temp_repo_path("iam_protect");
        cleanup_repo(&path);

        crate::sql::git_init("test_protect", Some(&path), None);

        // Protect main branch, only allow 'lead_dev'
        let result =
            crate::sql::git_protect_branch("test_protect", "main", vec!["lead_dev".to_string()]);
        assert!(result.contains("Protected"));

        // Verify branches table
        let protected = Spi::get_one::<bool>(
            "SELECT protected FROM pggit.branches WHERE repo_id = 'test_protect' AND name = 'main'",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(protected);

        // Unprotect by passing empty array
        let result2 = crate::sql::git_protect_branch("test_protect", "main", vec![]);
        assert!(result2.contains("Unprotected"));

        let protected2 = Spi::get_one::<bool>(
            "SELECT protected FROM pggit.branches WHERE repo_id = 'test_protect' AND name = 'main'",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(!protected2);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_commit_checks_write_permission() {
        bootstrap_tables();
        let path = temp_repo_path("iam_write_check");
        cleanup_repo(&path);

        crate::sql::git_init("test_wc", Some(&path), None);

        // Grant read-only to a role
        create_test_role("iam_readonly");
        crate::sql::git_grant("test_wc", "iam_readonly", "read", "repo", "*");

        // Stage a file as superuser
        crate::sql::git_add("test_wc", "test.md", Some("content"));

        // Try to commit as read-only role — should fail
        Spi::run("SET ROLE iam_readonly").unwrap();
        let result = std::panic::catch_unwind(|| {
            crate::sql::git_commit("test_wc", "Should fail");
        });
        Spi::run("RESET ROLE").unwrap();

        assert!(result.is_err());

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_commit_branch_protection_enforced() {
        bootstrap_tables();
        let path = temp_repo_path("iam_bp_enforce");
        cleanup_repo(&path);

        crate::sql::git_init("test_bp", Some(&path), None);

        // Create a role with write access
        create_test_role("iam_writer");
        crate::sql::git_grant("test_bp", "iam_writer", "write", "repo", "*");

        // Protect main, only allow 'lead_dev' (not 'iam_writer')
        crate::sql::git_protect_branch("test_bp", "main", vec!["lead_dev".to_string()]);

        // Stage as superuser
        crate::sql::git_add("test_bp", "test.md", Some("content"));

        // Try commit as iam_writer — should fail due to branch protection
        Spi::run("SET ROLE iam_writer").unwrap();
        let result = std::panic::catch_unwind(|| {
            crate::sql::git_commit("test_bp", "Should fail");
        });
        Spi::run("RESET ROLE").unwrap();

        assert!(result.is_err());

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_superuser_bypasses_checks() {
        bootstrap_tables();
        let path = temp_repo_path("iam_su");
        cleanup_repo(&path);

        // Superuser can init and commit without explicit permissions
        crate::sql::git_init("test_su", Some(&path), None);
        crate::sql::git_add("test_su", "readme.md", Some("hello"));
        let hash = crate::sql::git_commit("test_su", "Superuser commit");

        // Hash should be valid
        assert_eq!(hash.len(), 40);

        cleanup_repo(&path);
    }

    // =====================================================================
    // Binary and stage-only tests
    // =====================================================================

    #[pg_test]
    fn test_git_add_stage_existing_file() {
        bootstrap_tables();
        let path = temp_repo_path("stage_only");
        cleanup_repo(&path);

        crate::sql::git_init("test_stage", Some(&path), None);

        // Create a file on disk directly (simulating external tool / CLI)
        let file_path = std::path::Path::new(&path).join("external.md");
        std::fs::write(&file_path, "Created outside SQL").unwrap();

        // Stage without providing content (content = NULL)
        let result = crate::sql::git_add("test_stage", "external.md", None);
        assert_eq!(result, "external.md");

        // Commit and verify content via git_show
        crate::sql::git_commit("test_stage", "Stage external file");
        let content = crate::sql::git_show("test_stage", "external.md", None);
        assert_eq!(content, "Created outside SQL");

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_add_binary_and_show() {
        bootstrap_tables();
        let path = temp_repo_path("binary");
        cleanup_repo(&path);

        crate::sql::git_init("test_bin", Some(&path), None);

        // Add a binary file (fake PNG header)
        let binary_content: Vec<u8> = vec![0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];
        let result = crate::sql::git_add_binary("test_bin", "image.png", &binary_content);
        assert_eq!(result, "image.png");

        crate::sql::git_commit("test_bin", "Add binary file");

        // Retrieve via git_show_binary
        let retrieved = crate::sql::git_show_binary("test_bin", "image.png", None);
        assert_eq!(retrieved, binary_content);

        // Verify file exists on disk, not in Postgres tables
        let disk_path = std::path::Path::new(&path).join("image.png");
        assert!(disk_path.exists());
        assert_eq!(std::fs::read(&disk_path).unwrap(), binary_content);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_add_binary_large_file() {
        bootstrap_tables();
        let path = temp_repo_path("binary_large");
        cleanup_repo(&path);

        crate::sql::git_init("test_bl", Some(&path), None);

        // Create a 1MB binary file
        let large_content: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        crate::sql::git_add_binary("test_bl", "data.bin", &large_content);
        crate::sql::git_commit("test_bl", "Add large binary");

        let retrieved = crate::sql::git_show_binary("test_bl", "data.bin", None);
        assert_eq!(retrieved.len(), 1_000_000);
        assert_eq!(retrieved, large_content);

        cleanup_repo(&path);
    }

    // -----------------------------------------------------------------------
    // Phase 4: Branch operations
    // -----------------------------------------------------------------------

    #[pg_test]
    fn test_init_registers_main_branch() {
        bootstrap_tables();
        let path = temp_repo_path("init_branch");
        cleanup_repo(&path);

        crate::sql::git_init("test_ib", Some(&path), None);

        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.branches WHERE repo_id = 'test_ib' AND name = 'main'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(
            count, 1,
            "git_init should register 'main' in branches table"
        );

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_branch_and_list() {
        bootstrap_tables();
        let path = temp_repo_path("branch_list");
        cleanup_repo(&path);

        crate::sql::git_init("test_brl", Some(&path), None);
        crate::sql::git_add("test_brl", "readme.md", Some("# Hello"));
        crate::sql::git_commit("test_brl", "Initial commit");

        // Create a feature branch
        crate::sql::git_branch("test_brl", "feature-x", None);

        // List branches — should have 2
        let branches: Vec<_> = crate::sql::git_branches("test_brl").collect();
        assert_eq!(branches.len(), 2, "Should have main and feature-x");

        // Verify feature-x is registered in branches table
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.branches WHERE repo_id = 'test_brl' AND name = 'feature-x'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 1);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_checkout_and_commit() {
        bootstrap_tables();
        let path = temp_repo_path("checkout_commit");
        cleanup_repo(&path);

        crate::sql::git_init("test_cc", Some(&path), None);
        crate::sql::git_add("test_cc", "readme.md", Some("# Main"));
        crate::sql::git_commit("test_cc", "Initial commit");

        // Create and checkout feature branch
        crate::sql::git_branch("test_cc", "feature-y", None);
        crate::sql::git_checkout("test_cc", "feature-y");

        // Commit on the feature branch
        crate::sql::git_add("test_cc", "feature.md", Some("# Feature Y"));
        let hash = crate::sql::git_commit("test_cc", "Add feature");
        assert_eq!(hash.len(), 40);

        // Verify branches.head_commit was updated for feature-y
        let head = Spi::get_one::<String>(
            "SELECT head_commit FROM pggit.branches WHERE repo_id = 'test_cc' AND name = 'feature-y'",
        )
        .unwrap();
        assert_eq!(head, Some(hash));

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_merge_fast_forward() {
        bootstrap_tables();
        let path = temp_repo_path("merge_ff");
        cleanup_repo(&path);

        crate::sql::git_init("test_mff", Some(&path), None);
        crate::sql::git_add("test_mff", "readme.md", Some("# Main"));
        crate::sql::git_commit("test_mff", "Initial commit");

        // Create feature branch, checkout, add file, commit
        crate::sql::git_branch("test_mff", "feat", None);
        crate::sql::git_checkout("test_mff", "feat");
        crate::sql::git_add("test_mff", "new.md", Some("# New File"));
        crate::sql::git_commit("test_mff", "Add new file on feat");

        // Merge feat into main
        let merge_hash = crate::sql::git_merge("test_mff", "feat", Some("main"));
        assert_eq!(merge_hash.len(), 40);

        // Verify new.md is visible on main
        crate::sql::git_checkout("test_mff", "main");
        let content = crate::sql::git_show("test_mff", "new.md", None);
        assert_eq!(content, "# New File");

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_git_delete_branch() {
        bootstrap_tables();
        let path = temp_repo_path("delete_branch");
        cleanup_repo(&path);

        crate::sql::git_init("test_db", Some(&path), None);
        crate::sql::git_add("test_db", "readme.md", Some("# Main"));
        crate::sql::git_commit("test_db", "Initial commit");

        // Create and then delete a branch
        crate::sql::git_branch("test_db", "temp-branch", None);

        let branches_before: Vec<_> = crate::sql::git_branches("test_db").collect();
        assert_eq!(branches_before.len(), 2);

        crate::sql::git_delete_branch("test_db", "temp-branch");

        let branches_after: Vec<_> = crate::sql::git_branches("test_db").collect();
        assert_eq!(branches_after.len(), 1);
        assert_eq!(branches_after[0].0, "main");

        // Verify removed from branches table
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pggit.branches WHERE repo_id = 'test_db' AND name = 'temp-branch'",
        )
        .unwrap()
        .unwrap_or(0);
        assert_eq!(count, 0);

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_branch_scoped_files() {
        bootstrap_tables();
        let path = temp_repo_path("branch_files");
        cleanup_repo(&path);

        crate::sql::git_init("test_bf", Some(&path), None);
        crate::sql::git_add("test_bf", "shared.md", Some("# Shared"));
        crate::sql::git_commit("test_bf", "Initial commit");

        // Create feature branch and add a file there
        crate::sql::git_branch("test_bf", "feature-z", None);
        crate::sql::git_checkout("test_bf", "feature-z");
        crate::sql::git_add("test_bf", "only-on-feature.md", Some("# Feature Only"));
        crate::sql::git_commit("test_bf", "Feature-only file");

        // Verify that git_show works for feature-z file
        let content = crate::sql::git_show("test_bf", "only-on-feature.md", None);
        assert_eq!(content, "# Feature Only");

        // Switch back to main — file should not be visible
        crate::sql::git_checkout("test_bf", "main");
        let result = std::panic::catch_unwind(|| {
            crate::sql::git_show("test_bf", "only-on-feature.md", None);
        });
        assert!(result.is_err(), "File should not exist on main branch");

        cleanup_repo(&path);
    }

    #[pg_test]
    fn test_commit_updates_branch_head() {
        bootstrap_tables();
        let path = temp_repo_path("commit_head");
        cleanup_repo(&path);

        crate::sql::git_init("test_ch", Some(&path), None);
        crate::sql::git_add("test_ch", "readme.md", Some("# Hello"));
        let hash1 = crate::sql::git_commit("test_ch", "First commit");

        // Verify branches.head_commit matches first commit
        let head1 = Spi::get_one::<String>(
            "SELECT head_commit FROM pggit.branches WHERE repo_id = 'test_ch' AND name = 'main'",
        )
        .unwrap();
        assert_eq!(head1, Some(hash1.clone()));

        // Second commit should update head
        crate::sql::git_add("test_ch", "readme.md", Some("# Updated"));
        let hash2 = crate::sql::git_commit("test_ch", "Second commit");
        assert_ne!(hash1, hash2);

        let head2 = Spi::get_one::<String>(
            "SELECT head_commit FROM pggit.branches WHERE repo_id = 'test_ch' AND name = 'main'",
        )
        .unwrap();
        assert_eq!(head2, Some(hash2));

        cleanup_repo(&path);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // Don't use shared_preload_libraries — workers bind ports
        vec![]
    }
}
