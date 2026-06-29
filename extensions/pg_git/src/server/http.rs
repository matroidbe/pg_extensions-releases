//! HTTP request handler for the git smart protocol.
//!
//! Routes incoming HTTP requests to the appropriate git service handler.

use super::git_http;
use crate::error::PgGitError;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use pg_spi::SpiBridge;
use std::path::{Path, PathBuf};
use std::sync::Arc;

type BoxBody = Full<Bytes>;

/// Handle an incoming HTTP request.
pub async fn handle_request(
    req: Request<Incoming>,
    bridge: Arc<SpiBridge>,
) -> Result<Response<BoxBody>, hyper::Error> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    // Parse repo_id from path: /{repo_id}/...
    let segments: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
    if segments.len() < 2 {
        return Ok(error_response(StatusCode::NOT_FOUND, "Not found"));
    }

    let repo_id = segments[0];
    let rest = segments[1];

    // Look up repo path from database
    let repo_path = match lookup_repo_path(&bridge, repo_id).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            return Ok(error_response(
                StatusCode::NOT_FOUND,
                &format!("Repository '{}' not found", repo_id),
            ));
        }
        Err(e) => {
            return Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("Database error: {}", e),
            ));
        }
    };

    match (method.as_str(), rest) {
        ("GET", path) if path.starts_with("info/refs") => {
            let query = req.uri().query().unwrap_or("");
            let service = extract_service_param(query);

            match service {
                Some(svc) if svc == "git-upload-pack" || svc == "git-receive-pack" => {
                    handle_ref_discovery(&svc, &repo_path).await
                }
                _ => Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    "Missing or invalid service parameter",
                )),
            }
        }

        ("POST", "git-upload-pack") => {
            let body = collect_body(req).await?;
            handle_service_rpc("git-upload-pack", &repo_path, body).await
        }

        ("POST", "git-receive-pack") => {
            let body = collect_body(req).await?;
            let response = handle_service_rpc("git-receive-pack", &repo_path, body).await;

            if let Ok(ref resp) = response {
                if resp.status().is_success() {
                    trigger_sync(&bridge, repo_id, &repo_path).await;
                }
            }

            response
        }

        _ => Ok(error_response(StatusCode::NOT_FOUND, "Not found")),
    }
}

async fn handle_ref_discovery(
    service: &str,
    repo_path: &Path,
) -> Result<Response<BoxBody>, hyper::Error> {
    match git_http::ref_discovery(service, repo_path).await {
        Ok(body) => {
            let content_type = git_http::advertisement_content_type(service);
            Ok(Response::builder()
                .status(200)
                .header("Content-Type", content_type)
                .header("Cache-Control", "no-cache")
                .body(Full::new(Bytes::from(body)))
                .unwrap())
        }
        Err(e) => Ok(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
        )),
    }
}

async fn handle_service_rpc(
    service: &str,
    repo_path: &Path,
    body: Bytes,
) -> Result<Response<BoxBody>, hyper::Error> {
    match git_http::service_rpc(service, repo_path, body).await {
        Ok(response_body) => {
            let content_type = git_http::result_content_type(service);
            Ok(Response::builder()
                .status(200)
                .header("Content-Type", content_type)
                .header("Cache-Control", "no-cache")
                .body(Full::new(Bytes::from(response_body)))
                .unwrap())
        }
        Err(e) => Ok(error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &e.to_string(),
        )),
    }
}

async fn trigger_sync(bridge: &SpiBridge, repo_id: &str, repo_path: &Path) {
    let branch = crate::git::current_branch(repo_path).unwrap_or_else(|_| "main".to_string());

    let last_hash = bridge
        .query_one_string(
            &format!(
                "SELECT last_synced_hash FROM pggit.sync_state WHERE repo_id = '{}'",
                repo_id.replace('\'', "''")
            ),
            vec![],
        )
        .await
        .ok()
        .flatten();

    let new_commits = match crate::git::log_commits_since(repo_path, last_hash.as_deref()) {
        Ok(c) => c,
        Err(_) => return,
    };

    if new_commits.is_empty() {
        return;
    }

    for commit in &new_commits {
        // Insert commit
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
        let _ = bridge.execute(&sql, vec![]).await;

        // Sync file_history and files for this commit
        let diff = crate::git::diff_commit_parent(repo_path, &commit.hash).unwrap_or_default();
        let stats = if let Some(ref p) = commit.parent_hash {
            crate::git::diff_stat(repo_path, p, &commit.hash).unwrap_or_default()
        } else {
            Vec::new()
        };
        let stat_map: std::collections::HashMap<&str, (Option<i32>, Option<i32>)> = stats
            .iter()
            .map(|(p, a, r)| (p.as_str(), (*a, *r)))
            .collect();

        for entry in &diff {
            let (la, lr) = stat_map
                .get(entry.path.as_str())
                .copied()
                .unwrap_or((None, None));
            let la_s = la
                .map(|v| v.to_string())
                .unwrap_or_else(|| "NULL".to_string());
            let lr_s = lr
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
                la_s, lr_s,
                commit.committed_at,
                commit.author.replace('\'', "''"),
            );
            let _ = bridge.execute(&fh_sql, vec![]).await;

            // Update files table for adds/modifies
            if entry.change_type == "add" || entry.change_type == "modify" {
                if let Ok(content) = crate::git::show(repo_path, &entry.path, Some(&commit.hash)) {
                    let meta = crate::metadata::extract_metadata(&entry.path, &content);
                    let title = crate::metadata::extract_title(&entry.path, &content);
                    let blob_hash = crate::git::head_hash(repo_path)
                        .ok()
                        .flatten()
                        .unwrap_or_default();

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
                           current_hash = EXCLUDED.current_hash, mime_type = EXCLUDED.mime_type,
                           size_bytes = EXCLUDED.size_bytes, encoding = EXCLUDED.encoding,
                           language = EXCLUDED.language, line_count = EXCLUDED.line_count,
                           title = EXCLUDED.title, updated_at = EXCLUDED.updated_at, updated_by = EXCLUDED.updated_by",
                        repo_id.replace('\'', "''"),
                        entry.path.replace('\'', "''"),
                        branch.replace('\'', "''"),
                        blob_hash,
                        meta.mime_type, meta.size_bytes, meta.encoding,
                        lang_sql, lc_sql, title_sql,
                        commit.committed_at,
                        commit.author.replace('\'', "''"),
                        commit.committed_at,
                    );
                    let _ = bridge.execute(&upsert_sql, vec![]).await;
                }
            } else if entry.change_type == "delete" {
                let del_sql = format!(
                    "DELETE FROM pggit.files WHERE repo_id = '{}' AND path = '{}' AND branch = '{}'",
                    repo_id.replace('\'', "''"),
                    entry.path.replace('\'', "''"),
                    branch.replace('\'', "''"),
                );
                let _ = bridge.execute(&del_sql, vec![]).await;
            }
        }
    }

    if let Some(last) = new_commits.last() {
        let sql = format!(
            "UPDATE pggit.sync_state SET last_synced_hash = '{}', last_synced_at = now() WHERE repo_id = '{}'",
            last.hash,
            repo_id.replace('\'', "''"),
        );
        let _ = bridge.execute(&sql, vec![]).await;
    }
}

async fn lookup_repo_path(
    bridge: &SpiBridge,
    repo_id: &str,
) -> Result<Option<PathBuf>, PgGitError> {
    let sql = format!(
        "SELECT path FROM pggit.repos WHERE id = '{}'",
        repo_id.replace('\'', "''")
    );

    let result = bridge
        .query_one_string(&sql, vec![])
        .await
        .map_err(|e| PgGitError::Spi(e.to_string()))?;

    Ok(result.map(PathBuf::from))
}

fn extract_service_param(query: &str) -> Option<String> {
    for param in query.split('&') {
        if let Some(value) = param.strip_prefix("service=") {
            return Some(value.to_string());
        }
    }
    None
}

async fn collect_body(req: Request<Incoming>) -> Result<Bytes, hyper::Error> {
    let body = req.into_body();
    let collected = body.collect().await?;
    Ok(collected.to_bytes())
}

fn error_response(status: StatusCode, message: &str) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header("Content-Type", "text/plain")
        .body(Full::new(Bytes::from(message.to_string())))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_service_param() {
        assert_eq!(
            extract_service_param("service=git-upload-pack"),
            Some("git-upload-pack".to_string())
        );
        assert_eq!(extract_service_param(""), None);
        assert_eq!(extract_service_param("foo=bar"), None);
    }
}
