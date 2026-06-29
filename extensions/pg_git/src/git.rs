//! Git operations module.
//!
//! Uses gitoxide (gix) for read operations (show, log, list_refs) and
//! the git binary via subprocess for write operations (init, add, commit).
//! The git binary is already required for the HTTP smart protocol endpoint.
//!
//! This module has no pgrx dependencies and is fully unit-testable.

use crate::error::PgGitError;
use gix::bstr::ByteSlice;
use std::path::Path;
use std::process::Command;

/// Metadata for a single commit.
#[derive(Debug, Clone)]
pub struct CommitInfo {
    pub hash: String,
    pub parent_hash: Option<String>,
    pub message: String,
    pub author: String,
    pub author_email: String,
    /// Unix timestamp (seconds since epoch)
    pub committed_at: i64,
}

/// A single file change between two commits.
#[derive(Debug, Clone)]
pub struct DiffEntry {
    pub path: String,
    pub change_type: String, // "add", "modify", "delete", "rename"
    pub old_hash: Option<String>,
    pub new_hash: Option<String>,
    pub old_path: Option<String>,
}

/// An entry in a git tree (directory listing).
#[derive(Debug, Clone)]
pub struct TreeEntry {
    pub path: String,
    pub kind: String, // "blob" or "tree"
    pub hash: String,
    pub size: i64,
}

/// A local branch with its tip commit.
#[derive(Debug, Clone)]
pub struct BranchInfo {
    pub name: String,
    pub commit_hash: String,
    pub is_current: bool,
}

// ===========================================================================
// Write operations (git subprocess)
// ===========================================================================

/// Initialize a new git repository at the given path.
pub fn init_repo(path: &Path) -> Result<(), PgGitError> {
    if path.join(".git").exists() {
        return Err(PgGitError::RepoAlreadyExists(path.display().to_string()));
    }

    std::fs::create_dir_all(path).map_err(|e| {
        PgGitError::InvalidPath(format!("Cannot create directory {}: {}", path.display(), e))
    })?;

    run_git(path, &["init", "-b", "main"])?;
    // Configure so commits work without global git config
    run_git(path, &["config", "user.name", "pg_git"])?;
    run_git(path, &["config", "user.email", "pg_git@localhost"])?;
    // Allow pushes to the checked-out branch
    run_git(
        path,
        &["config", "receive.denyCurrentBranch", "updateInstead"],
    )?;

    Ok(())
}

/// Write a file to the worktree and stage it in the index.
pub fn add_file(repo_path: &Path, file_path: &str, content: &[u8]) -> Result<(), PgGitError> {
    let full_path = repo_path.join(file_path);
    if let Some(parent) = full_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&full_path, content)?;

    run_git(repo_path, &["add", file_path])?;
    Ok(())
}

/// Stage an existing file in the working tree (no content write).
pub fn stage_file(repo_path: &Path, file_path: &str) -> Result<(), PgGitError> {
    let full_path = repo_path.join(file_path);
    if !full_path.exists() {
        return Err(PgGitError::Git(format!(
            "File '{}' does not exist in working tree",
            file_path
        )));
    }
    run_git(repo_path, &["add", file_path])?;
    Ok(())
}

/// Write binary content to the working tree and stage it.
pub fn add_binary_file(
    repo_path: &Path,
    file_path: &str,
    content: &[u8],
) -> Result<(), PgGitError> {
    add_file(repo_path, file_path, content)
}

/// Create a commit from the current index state.
///
/// Returns the hex SHA of the new commit.
pub fn commit(
    repo_path: &Path,
    message: &str,
    author_name: &str,
    author_email: &str,
) -> Result<String, PgGitError> {
    let author = format!("{} <{}>", author_name, author_email);
    let output = run_git(
        repo_path,
        &[
            "commit",
            "--allow-empty",
            "-m",
            message,
            "--author",
            &author,
        ],
    )?;

    // Get the commit hash
    let hash_output = run_git(repo_path, &["rev-parse", "HEAD"])?;
    let hash = hash_output.trim().to_string();
    if hash.len() < 40 {
        return Err(PgGitError::Git(format!(
            "Unexpected rev-parse output: {}",
            output
        )));
    }
    Ok(hash)
}

// ===========================================================================
// Branch operations (git subprocess)
// ===========================================================================

/// Get the name of the currently checked-out branch.
///
/// Works even on empty repos (before first commit).
/// Returns an error if HEAD is detached.
pub fn current_branch(repo_path: &Path) -> Result<String, PgGitError> {
    let output = run_git(repo_path, &["symbolic-ref", "--short", "HEAD"])?;
    Ok(output.trim().to_string())
}

/// Create a new branch at the given start point (or HEAD if None).
pub fn create_branch(
    repo_path: &Path,
    branch_name: &str,
    start_point: Option<&str>,
) -> Result<(), PgGitError> {
    let mut args = vec!["branch", branch_name];
    if let Some(sp) = start_point {
        args.push(sp);
    }
    run_git(repo_path, &args)?;
    Ok(())
}

/// Checkout an existing branch.
pub fn checkout(repo_path: &Path, branch_name: &str) -> Result<(), PgGitError> {
    run_git(repo_path, &["checkout", branch_name])?;
    Ok(())
}

/// List all local branches with their HEAD commit hashes.
pub fn list_branches(repo_path: &Path) -> Result<Vec<BranchInfo>, PgGitError> {
    let output = run_git(repo_path, &["branch", "-v", "--no-abbrev"])?;
    let mut branches = Vec::new();
    for line in output.lines() {
        if line.is_empty() {
            continue;
        }
        let is_current = line.starts_with('*');
        // Lines are "* name  hash message" or "  name  hash message"
        let trimmed = &line[2..];
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() >= 2 {
            branches.push(BranchInfo {
                name: parts[0].to_string(),
                commit_hash: parts[1].to_string(),
                is_current,
            });
        }
    }
    Ok(branches)
}

/// Delete a local branch. Fails if it's the currently checked-out branch.
pub fn delete_branch(repo_path: &Path, branch_name: &str) -> Result<(), PgGitError> {
    run_git(repo_path, &["branch", "-d", branch_name])?;
    Ok(())
}

/// Merge source_branch into the current branch (fast-forward only).
///
/// Returns the resulting commit hash after merge.
pub fn merge_fast_forward(repo_path: &Path, source_branch: &str) -> Result<String, PgGitError> {
    run_git(repo_path, &["merge", "--ff-only", source_branch])?;
    let output = run_git(repo_path, &["rev-parse", "HEAD"])?;
    Ok(output.trim().to_string())
}

/// Get the commit hash at the tip of a specific branch.
pub fn branch_head_hash(repo_path: &Path, branch_name: &str) -> Result<String, PgGitError> {
    let output = run_git(repo_path, &["rev-parse", branch_name])?;
    Ok(output.trim().to_string())
}

/// Get commits reachable from a branch tip but not from `since_hash`.
///
/// Returns in chronological order (oldest first) for insertion.
/// Does NOT require checkout — uses `git log <range>`.
pub fn log_commits_on_branch(
    repo_path: &Path,
    branch_name: &str,
    since_hash: Option<&str>,
) -> Result<Vec<CommitInfo>, PgGitError> {
    let range = match since_hash {
        Some(hash) => format!("{}..{}", hash, branch_name),
        None => branch_name.to_string(),
    };

    let output = run_git(repo_path, &["log", "--format=%H|%P|%an|%ae|%at|%s", &range])?;

    let mut commits = Vec::new();
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(6, '|').collect();
        if parts.len() >= 6 {
            commits.push(CommitInfo {
                hash: parts[0].to_string(),
                parent_hash: if parts[1].is_empty() {
                    None
                } else {
                    Some(parts[1].split(' ').next().unwrap_or("").to_string())
                },
                author: parts[2].to_string(),
                author_email: parts[3].to_string(),
                committed_at: parts[4].parse::<i64>().unwrap_or(0),
                message: parts[5].to_string(),
            });
        }
    }
    commits.reverse(); // oldest first for insertion
    Ok(commits)
}

/// Read a file at the tip of a specific branch (without checkout).
pub fn show_at_branch(
    repo_path: &Path,
    branch_name: &str,
    file_path: &str,
) -> Result<Vec<u8>, PgGitError> {
    let spec = format!("{}:{}", branch_name, file_path);
    let output = Command::new("git")
        .args(["show", &spec])
        .current_dir(repo_path)
        .env("GIT_DIR", repo_path.join(".git"))
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PgGitError::Git(format!(
            "git show {} failed: {}",
            spec,
            stderr.trim()
        )));
    }

    Ok(output.stdout)
}

// ===========================================================================
// Read operations (gitoxide)
// ===========================================================================

/// Read a file's content at HEAD or at a specific commit.
pub fn show(
    repo_path: &Path,
    file_path: &str,
    commit_hash: Option<&str>,
) -> Result<Vec<u8>, PgGitError> {
    let repo = open_repo(repo_path)?;

    let commit_obj = match commit_hash {
        Some(hash) => {
            let id = gix::ObjectId::from_hex(hash.as_bytes())
                .map_err(|e| PgGitError::Git(format!("Invalid commit hash: {}", e)))?;
            repo.find_commit(id)
                .map_err(|e| PgGitError::Git(format!("Commit not found: {}", e)))?
        }
        None => repo
            .head_commit()
            .map_err(|e| PgGitError::Git(format!("No HEAD commit: {}", e)))?,
    };

    let tree = commit_obj
        .tree()
        .map_err(|e| PgGitError::Git(format!("Failed to read tree: {}", e)))?;

    let entry = tree
        .lookup_entry_by_path(file_path)
        .map_err(|e| PgGitError::Git(format!("Failed to lookup path: {}", e)))?
        .ok_or_else(|| PgGitError::FileNotFound(file_path.to_string()))?;

    let object = entry
        .object()
        .map_err(|e| PgGitError::Git(format!("Failed to read object: {}", e)))?;

    Ok(object.data.to_vec())
}

/// Walk the commit graph and return commit metadata.
///
/// Returns commits in reverse chronological order (newest first).
pub fn log_commits(repo_path: &Path, limit: usize) -> Result<Vec<CommitInfo>, PgGitError> {
    let repo = open_repo(repo_path)?;

    let head = match repo.head_commit() {
        Ok(c) => c,
        Err(_) => return Ok(vec![]),
    };

    let mut commits = Vec::new();
    let mut current = Some(head);

    while let Some(c) = current {
        if commits.len() >= limit {
            break;
        }

        let info = commit_to_info(&c)?;
        let parent_id = c.parent_ids().next();
        commits.push(info);

        current = match parent_id {
            Some(pid) => pid.object().ok().and_then(|o| o.try_into_commit().ok()),
            None => None,
        };
    }

    Ok(commits)
}

/// Get commits reachable from HEAD but not from `since_hash`.
///
/// Returns in chronological order (oldest first) for insertion.
pub fn log_commits_since(
    repo_path: &Path,
    since_hash: Option<&str>,
) -> Result<Vec<CommitInfo>, PgGitError> {
    let repo = open_repo(repo_path)?;

    let head = match repo.head_commit() {
        Ok(c) => c,
        Err(_) => return Ok(vec![]),
    };

    let stop_at = since_hash
        .map(|h| {
            gix::ObjectId::from_hex(h.as_bytes())
                .map_err(|e| PgGitError::Git(format!("Invalid hash: {}", e)))
        })
        .transpose()?;

    let mut commits = Vec::new();
    let mut current = Some(head);

    while let Some(c) = current {
        if Some(c.id().into()) == stop_at {
            break;
        }

        let info = commit_to_info(&c)?;
        let parent_id = c.parent_ids().next();
        commits.push(info);

        current = match parent_id {
            Some(pid) => pid.object().ok().and_then(|o| o.try_into_commit().ok()),
            None => None,
        };
    }

    commits.reverse();
    Ok(commits)
}

/// List all references in the repo as (ref_name, commit_hash) pairs.
pub fn list_refs(repo_path: &Path) -> Result<Vec<(String, String)>, PgGitError> {
    let repo = open_repo(repo_path)?;

    let mut result = Vec::new();

    // HEAD
    if let Ok(head) = repo.head_commit() {
        let id: gix::ObjectId = head.id().into();
        result.push(("HEAD".to_string(), id.to_string()));
    }

    // All refs
    let refs = repo
        .references()
        .map_err(|e| PgGitError::Git(format!("Failed to list refs: {}", e)))?;

    let all = refs
        .all()
        .map_err(|e| PgGitError::Git(format!("Failed to iterate refs: {}", e)))?;

    for reference in all.flatten() {
        let name = reference.name().as_bstr().to_string();
        if let Some(id) = reference.try_id() {
            result.push((name, id.to_string()));
        }
    }

    Ok(result)
}

/// Get the HEAD commit hash, if any.
pub fn head_hash(repo_path: &Path) -> Result<Option<String>, PgGitError> {
    let repo = open_repo(repo_path)?;
    let result = match repo.head_commit() {
        Ok(c) => {
            let id: gix::ObjectId = c.id().into();
            Some(id.to_string())
        }
        Err(_) => None,
    };
    Ok(result)
}

// ===========================================================================
// Diff operations (git subprocess)
// ===========================================================================

/// Diff two commits, returning changed files.
///
/// Uses `git diff --name-status` for reliable output.
pub fn diff_commits(
    repo_path: &Path,
    from_hash: &str,
    to_hash: &str,
) -> Result<Vec<DiffEntry>, PgGitError> {
    let output = run_git(
        repo_path,
        &["diff", "--name-status", "--no-renames", from_hash, to_hash],
    )?;
    parse_name_status(&output)
}

/// Diff a commit against its parent. For root commits (no parent), all files are "add".
pub fn diff_commit_parent(
    repo_path: &Path,
    commit_hash: &str,
) -> Result<Vec<DiffEntry>, PgGitError> {
    // Check if commit has a parent
    let parent_check = run_git(repo_path, &["rev-parse", &format!("{}^", commit_hash)]);

    match parent_check {
        Ok(parent_hash) => {
            let parent = parent_hash.trim();
            diff_commits(repo_path, parent, commit_hash)
        }
        Err(_) => {
            // Root commit — diff against empty tree
            let output = run_git(
                repo_path,
                &[
                    "diff-tree",
                    "--root",
                    "--no-commit-id",
                    "-r",
                    "--name-status",
                    commit_hash,
                ],
            )?;
            parse_name_status(&output)
        }
    }
}

/// Get diff stats (lines added/removed) between two commits for a specific file.
#[allow(clippy::type_complexity)]
pub fn diff_stat(
    repo_path: &Path,
    from_hash: &str,
    to_hash: &str,
) -> Result<Vec<(String, Option<i32>, Option<i32>)>, PgGitError> {
    let output = run_git(repo_path, &["diff", "--numstat", from_hash, to_hash])?;
    let mut stats = Vec::new();
    for line in output.lines() {
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() >= 3 {
            let added = parts[0].parse::<i32>().ok();
            let removed = parts[1].parse::<i32>().ok();
            let path = parts[2].to_string();
            stats.push((path, added, removed));
        }
    }
    Ok(stats)
}

fn parse_name_status(output: &str) -> Result<Vec<DiffEntry>, PgGitError> {
    let mut entries = Vec::new();
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(2, '\t').collect();
        if parts.len() < 2 {
            continue;
        }
        let status = parts[0];
        let path = parts[1].to_string();
        let change_type = match status {
            "A" => "add",
            "M" => "modify",
            "D" => "delete",
            s if s.starts_with('R') => "rename",
            _ => "modify", // fallback
        };
        entries.push(DiffEntry {
            path,
            change_type: change_type.to_string(),
            old_hash: None,
            new_hash: None,
            old_path: None,
        });
    }
    Ok(entries)
}

// ===========================================================================
// Tree operations (gitoxide)
// ===========================================================================

/// List entries in a tree at the given path (or root if None).
///
/// Uses `git ls-tree` subprocess for reliable output.
pub fn tree_entries(
    repo_path: &Path,
    subpath: Option<&str>,
    commit_hash: Option<&str>,
) -> Result<Vec<TreeEntry>, PgGitError> {
    let rev = commit_hash.unwrap_or("HEAD");
    let tree_spec = match subpath {
        Some(p) => format!("{}:{}", rev, p),
        None => rev.to_string(),
    };

    let output = run_git(repo_path, &["ls-tree", "-l", &tree_spec])?;

    let mut entries = Vec::new();
    for line in output.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Format: <mode> <type> <hash> <size>\t<path>
        // e.g.: 100644 blob abc123    42\treadme.md
        //       040000 tree def456     -\tdocs
        let (meta, path) = line
            .split_once('\t')
            .ok_or_else(|| PgGitError::Git(format!("Invalid ls-tree line: {}", line)))?;

        let parts: Vec<&str> = meta.split_whitespace().collect();
        if parts.len() < 4 {
            continue;
        }

        let kind = parts[1].to_string();
        let hash = parts[2].to_string();
        let size = parts[3].parse::<i64>().unwrap_or(0);

        entries.push(TreeEntry {
            path: path.to_string(),
            kind,
            hash,
            size,
        });
    }

    Ok(entries)
}

// ===========================================================================
// Helpers
// ===========================================================================

fn open_repo(path: &Path) -> Result<gix::Repository, PgGitError> {
    gix::open(path)
        .map_err(|e| PgGitError::Git(format!("Failed to open repo at {}: {}", path.display(), e)))
}

fn run_git(repo_path: &Path, args: &[&str]) -> Result<String, PgGitError> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_path)
        .env("GIT_DIR", repo_path.join(".git"))
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PgGitError::Git(format!(
            "git {} failed: {}",
            args.first().unwrap_or(&""),
            stderr.trim()
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn commit_to_info(c: &gix::Commit<'_>) -> Result<CommitInfo, PgGitError> {
    let author = c.author().map_err(|e| PgGitError::Git(e.to_string()))?;
    let committer = c.committer().map_err(|e| PgGitError::Git(e.to_string()))?;

    Ok(CommitInfo {
        hash: c.id.to_string(),
        parent_hash: c.parent_ids().next().map(|p| p.to_string()),
        message: c.message_raw_sloppy().to_str_lossy().trim().to_string(),
        author: author.name.to_string(),
        author_email: author.email.to_string(),
        committed_at: committer.time.seconds,
    })
}

// ===========================================================================
// Unit Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_repo() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();
        init_repo(&path).unwrap();
        (dir, path)
    }

    #[test]
    fn test_init_repo() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test_repo");
        init_repo(&path).unwrap();
        assert!(path.join(".git").exists());
    }

    #[test]
    fn test_init_repo_already_exists() {
        let (_dir, path) = setup_repo();
        let result = init_repo(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_add_file() {
        let (_dir, path) = setup_repo();
        add_file(&path, "hello.md", b"# Hello World").unwrap();
        let content = std::fs::read(path.join("hello.md")).unwrap();
        assert_eq!(content, b"# Hello World");
    }

    #[test]
    fn test_add_file_nested_path() {
        let (_dir, path) = setup_repo();
        add_file(&path, "docs/design/api.md", b"# API Design").unwrap();
        assert!(path.join("docs/design/api.md").exists());
    }

    #[test]
    fn test_commit() {
        let (_dir, path) = setup_repo();
        add_file(&path, "hello.md", b"# Hello").unwrap();
        let hash = commit(&path, "Initial commit", "test", "test@example.com").unwrap();
        assert_eq!(hash.len(), 40);
    }

    #[test]
    fn test_show_at_head() {
        let (_dir, path) = setup_repo();
        add_file(&path, "hello.md", b"# Hello World").unwrap();
        commit(&path, "Add hello", "test", "test@example.com").unwrap();

        let content = show(&path, "hello.md", None).unwrap();
        assert_eq!(content, b"# Hello World");
    }

    #[test]
    fn test_show_at_specific_commit() {
        let (_dir, path) = setup_repo();

        add_file(&path, "hello.md", b"version 1").unwrap();
        let hash1 = commit(&path, "v1", "test", "test@example.com").unwrap();

        add_file(&path, "hello.md", b"version 2").unwrap();
        commit(&path, "v2", "test", "test@example.com").unwrap();

        let content = show(&path, "hello.md", Some(&hash1)).unwrap();
        assert_eq!(content, b"version 1");

        let content = show(&path, "hello.md", None).unwrap();
        assert_eq!(content, b"version 2");
    }

    #[test]
    fn test_show_file_not_found() {
        let (_dir, path) = setup_repo();
        add_file(&path, "hello.md", b"# Hello").unwrap();
        commit(&path, "Add hello", "test", "test@example.com").unwrap();

        let result = show(&path, "nonexistent.md", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_commits() {
        let (_dir, path) = setup_repo();

        add_file(&path, "a.md", b"a").unwrap();
        commit(&path, "First", "alice", "alice@test.com").unwrap();

        add_file(&path, "b.md", b"b").unwrap();
        commit(&path, "Second", "bob", "bob@test.com").unwrap();

        let commits = log_commits(&path, 10).unwrap();
        assert_eq!(commits.len(), 2);

        // Newest first
        assert_eq!(commits[0].message, "Second");
        assert_eq!(commits[0].author, "bob");
        assert_eq!(commits[1].message, "First");
        assert_eq!(commits[1].author, "alice");

        // Parent chain
        assert!(commits[0].parent_hash.is_some());
        assert_eq!(commits[0].parent_hash.as_ref().unwrap(), &commits[1].hash);
        assert!(commits[1].parent_hash.is_none());
    }

    #[test]
    fn test_log_commits_limit() {
        let (_dir, path) = setup_repo();

        for i in 0..5 {
            add_file(&path, &format!("{}.md", i), format!("{}", i).as_bytes()).unwrap();
            commit(&path, &format!("Commit {}", i), "test", "test@test.com").unwrap();
        }

        let commits = log_commits(&path, 3).unwrap();
        assert_eq!(commits.len(), 3);
    }

    #[test]
    fn test_log_commits_empty_repo() {
        let (_dir, path) = setup_repo();
        let commits = log_commits(&path, 10).unwrap();
        assert!(commits.is_empty());
    }

    #[test]
    fn test_log_commits_since() {
        let (_dir, path) = setup_repo();

        add_file(&path, "a.md", b"a").unwrap();
        let hash1 = commit(&path, "First", "test", "test@test.com").unwrap();

        add_file(&path, "b.md", b"b").unwrap();
        commit(&path, "Second", "test", "test@test.com").unwrap();

        add_file(&path, "c.md", b"c").unwrap();
        commit(&path, "Third", "test", "test@test.com").unwrap();

        let commits = log_commits_since(&path, Some(&hash1)).unwrap();
        assert_eq!(commits.len(), 2);
        // Chronological order (oldest first)
        assert_eq!(commits[0].message, "Second");
        assert_eq!(commits[1].message, "Third");
    }

    #[test]
    fn test_head_hash() {
        let (_dir, path) = setup_repo();
        assert!(head_hash(&path).unwrap().is_none());

        add_file(&path, "a.md", b"a").unwrap();
        let hash = commit(&path, "Init", "test", "test@test.com").unwrap();
        assert_eq!(head_hash(&path).unwrap().unwrap(), hash);
    }

    #[test]
    fn test_diff_commits_add() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"aaa").unwrap();
        let hash1 = commit(&path, "Add a", "test", "test@test.com").unwrap();

        add_file(&path, "b.md", b"bbb").unwrap();
        let hash2 = commit(&path, "Add b", "test", "test@test.com").unwrap();

        let diff = diff_commits(&path, &hash1, &hash2).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "b.md");
        assert_eq!(diff[0].change_type, "add");
    }

    #[test]
    fn test_diff_commits_modify() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"version 1").unwrap();
        let hash1 = commit(&path, "v1", "test", "test@test.com").unwrap();

        add_file(&path, "a.md", b"version 2").unwrap();
        let hash2 = commit(&path, "v2", "test", "test@test.com").unwrap();

        let diff = diff_commits(&path, &hash1, &hash2).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "a.md");
        assert_eq!(diff[0].change_type, "modify");
    }

    #[test]
    fn test_diff_commits_delete() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"aaa").unwrap();
        add_file(&path, "b.md", b"bbb").unwrap();
        let hash1 = commit(&path, "Add files", "test", "test@test.com").unwrap();

        std::fs::remove_file(path.join("b.md")).unwrap();
        run_git(&path, &["add", "-A"]).unwrap();
        let hash2 = commit(&path, "Delete b", "test", "test@test.com").unwrap();

        let diff = diff_commits(&path, &hash1, &hash2).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "b.md");
        assert_eq!(diff[0].change_type, "delete");
    }

    #[test]
    fn test_diff_commits_multiple_changes() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"aaa").unwrap();
        let hash1 = commit(&path, "Init", "test", "test@test.com").unwrap();

        add_file(&path, "a.md", b"modified").unwrap();
        add_file(&path, "b.md", b"new").unwrap();
        let hash2 = commit(&path, "Changes", "test", "test@test.com").unwrap();

        let diff = diff_commits(&path, &hash1, &hash2).unwrap();
        assert_eq!(diff.len(), 2);
        let paths: Vec<&str> = diff.iter().map(|d| d.path.as_str()).collect();
        assert!(paths.contains(&"a.md"));
        assert!(paths.contains(&"b.md"));
    }

    #[test]
    fn test_diff_commit_parent_root() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"aaa").unwrap();
        add_file(&path, "b.md", b"bbb").unwrap();
        let hash = commit(&path, "Root", "test", "test@test.com").unwrap();

        let diff = diff_commit_parent(&path, &hash).unwrap();
        assert_eq!(diff.len(), 2);
        assert!(diff.iter().all(|d| d.change_type == "add"));
    }

    #[test]
    fn test_diff_commit_parent_non_root() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"aaa").unwrap();
        commit(&path, "First", "test", "test@test.com").unwrap();

        add_file(&path, "b.md", b"bbb").unwrap();
        let hash2 = commit(&path, "Second", "test", "test@test.com").unwrap();

        let diff = diff_commit_parent(&path, &hash2).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "b.md");
        assert_eq!(diff[0].change_type, "add");
    }

    #[test]
    fn test_diff_stat() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"line 1\nline 2\n").unwrap();
        let hash1 = commit(&path, "v1", "test", "test@test.com").unwrap();

        add_file(&path, "a.md", b"line 1\nline 2 modified\nline 3\n").unwrap();
        let hash2 = commit(&path, "v2", "test", "test@test.com").unwrap();

        let stats = diff_stat(&path, &hash1, &hash2).unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].0, "a.md");
        assert!(stats[0].1.unwrap() > 0); // lines added
        assert!(stats[0].2.unwrap() > 0); // lines removed
    }

    #[test]
    fn test_tree_entries_root() {
        let (_dir, path) = setup_repo();
        add_file(&path, "a.md", b"aaa").unwrap();
        add_file(&path, "docs/b.md", b"bbb").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        let entries = tree_entries(&path, None, None).unwrap();
        assert_eq!(entries.len(), 2); // a.md + docs/
        let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(names.contains(&"a.md"));
        assert!(names.contains(&"docs"));
    }

    #[test]
    fn test_tree_entries_subdir() {
        let (_dir, path) = setup_repo();
        add_file(&path, "docs/api.md", b"api").unwrap();
        add_file(&path, "docs/guide.md", b"guide").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        let entries = tree_entries(&path, Some("docs"), None).unwrap();
        assert_eq!(entries.len(), 2);
        let names: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(names.contains(&"api.md"));
        assert!(names.contains(&"guide.md"));
    }

    #[test]
    fn test_list_refs() {
        let (_dir, path) = setup_repo();
        add_file(&path, "hello.md", b"hello").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        let refs = list_refs(&path).unwrap();
        assert!(!refs.is_empty());
        // Should have HEAD and refs/heads/main
        let names: Vec<&str> = refs.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"HEAD"));
        assert!(names.contains(&"refs/heads/main"));
    }

    // =====================================================================
    // Branch operation tests
    // =====================================================================

    #[test]
    fn test_current_branch() {
        let (_dir, path) = setup_repo();
        // After init -b main, current branch is main
        let branch = current_branch(&path).unwrap();
        assert_eq!(branch, "main");
    }

    #[test]
    fn test_create_branch() {
        let (_dir, path) = setup_repo();
        add_file(&path, "init.md", b"init").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        create_branch(&path, "feature/test", None).unwrap();

        let branches = list_branches(&path).unwrap();
        let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
        assert!(names.contains(&"main"));
        assert!(names.contains(&"feature/test"));
    }

    #[test]
    fn test_checkout() {
        let (_dir, path) = setup_repo();
        add_file(&path, "init.md", b"init").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        create_branch(&path, "dev", None).unwrap();
        checkout(&path, "dev").unwrap();
        assert_eq!(current_branch(&path).unwrap(), "dev");

        checkout(&path, "main").unwrap();
        assert_eq!(current_branch(&path).unwrap(), "main");
    }

    #[test]
    fn test_list_branches() {
        let (_dir, path) = setup_repo();
        add_file(&path, "init.md", b"init").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        create_branch(&path, "alpha", None).unwrap();
        create_branch(&path, "beta", None).unwrap();

        let branches = list_branches(&path).unwrap();
        assert_eq!(branches.len(), 3); // main, alpha, beta

        // main should be current
        let main = branches.iter().find(|b| b.name == "main").unwrap();
        assert!(main.is_current);
        assert_eq!(main.commit_hash.len(), 40);
    }

    #[test]
    fn test_merge_fast_forward() {
        let (_dir, path) = setup_repo();
        add_file(&path, "base.md", b"base").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        // Create feature branch and commit on it
        create_branch(&path, "feature", None).unwrap();
        checkout(&path, "feature").unwrap();
        add_file(&path, "new.md", b"new file").unwrap();
        commit(&path, "Add new file", "test", "test@test.com").unwrap();

        // Switch back to main and merge
        checkout(&path, "main").unwrap();
        let hash = merge_fast_forward(&path, "feature").unwrap();
        assert_eq!(hash.len(), 40);

        // new.md should be visible on main now
        let content = show(&path, "new.md", None).unwrap();
        assert_eq!(content, b"new file");
    }

    #[test]
    fn test_delete_branch() {
        let (_dir, path) = setup_repo();
        add_file(&path, "init.md", b"init").unwrap();
        commit(&path, "Init", "test", "test@test.com").unwrap();

        create_branch(&path, "temp", None).unwrap();
        assert_eq!(list_branches(&path).unwrap().len(), 2);

        delete_branch(&path, "temp").unwrap();
        assert_eq!(list_branches(&path).unwrap().len(), 1);
    }
}
