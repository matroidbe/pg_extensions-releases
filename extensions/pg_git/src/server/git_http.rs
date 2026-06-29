//! Git smart HTTP protocol implementation.
//!
//! Delegates to `git upload-pack` and `git receive-pack` subprocesses
//! for pack protocol handling. This is the standard approach used by
//! production git servers (gitea, cgit, etc.).

use crate::error::PgGitError;
use std::path::Path;
use std::process::Stdio;
use tokio::process::Command;

/// Perform ref discovery for a git service.
///
/// Runs `git <service> --stateless-rpc --advertise-refs <path>` and
/// prepends the required pkt-line service header.
pub async fn ref_discovery(service: &str, repo_path: &Path) -> Result<Vec<u8>, PgGitError> {
    let git_service = match service {
        "git-upload-pack" => "upload-pack",
        "git-receive-pack" => "receive-pack",
        _ => return Err(PgGitError::Git(format!("Unknown service: {}", service))),
    };

    let output = Command::new("git")
        .arg(git_service)
        .arg("--stateless-rpc")
        .arg("--advertise-refs")
        .arg(repo_path)
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(PgGitError::Git(format!(
            "git {} --advertise-refs failed: {}",
            git_service, stderr
        )));
    }

    // Build response: service header + flush + refs
    let mut body = Vec::new();
    let header = format!("# service={}\n", service);
    body.extend_from_slice(&pkt_line(&header));
    body.extend_from_slice(b"0000"); // flush
    body.extend_from_slice(&output.stdout);

    Ok(body)
}

/// Handle a git service RPC request (upload-pack or receive-pack).
///
/// Pipes the request body to `git <service> --stateless-rpc <path>` stdin
/// and returns the stdout as the response body.
pub async fn service_rpc(
    service: &str,
    repo_path: &Path,
    body: bytes::Bytes,
) -> Result<Vec<u8>, PgGitError> {
    let git_service = match service {
        "git-upload-pack" => "upload-pack",
        "git-receive-pack" => "receive-pack",
        _ => return Err(PgGitError::Git(format!("Unknown service: {}", service))),
    };

    let mut child = Command::new("git")
        .arg(git_service)
        .arg("--stateless-rpc")
        .arg(repo_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Write request body to stdin
    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        let _: () = stdin.write_all(&body).await?;
        drop(stdin); // Close stdin to signal end of input
    }

    let output = child.wait_with_output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // receive-pack may exit with non-zero on errors but still produce output
        if output.stdout.is_empty() {
            return Err(PgGitError::Git(format!(
                "git {} failed: {}",
                git_service, stderr
            )));
        }
    }

    Ok(output.stdout)
}

/// Content type for ref discovery responses.
pub fn advertisement_content_type(service: &str) -> String {
    format!("application/x-{}-advertisement", service)
}

/// Content type for RPC responses.
pub fn result_content_type(service: &str) -> String {
    format!("application/x-{}-result", service)
}

/// Content type expected for RPC requests.
pub fn request_content_type(service: &str) -> String {
    format!("application/x-{}-request", service)
}

// ---------------------------------------------------------------------------
// Pkt-line encoding
// ---------------------------------------------------------------------------

/// Encode a string as a pkt-line (4 hex digits length prefix + data).
fn pkt_line(data: &str) -> Vec<u8> {
    let len = data.len() + 4; // 4 bytes for length prefix
    format!("{:04x}{}", len, data).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pkt_line() {
        let result = pkt_line("# service=git-upload-pack\n");
        let expected = b"001e# service=git-upload-pack\n";
        assert_eq!(result, expected);
    }

    #[test]
    fn test_pkt_line_short() {
        let result = pkt_line("ok\n");
        assert_eq!(result, b"0007ok\n");
    }

    #[test]
    fn test_content_types() {
        assert_eq!(
            advertisement_content_type("git-upload-pack"),
            "application/x-git-upload-pack-advertisement"
        );
        assert_eq!(
            result_content_type("git-receive-pack"),
            "application/x-git-receive-pack-result"
        );
        assert_eq!(
            request_content_type("git-upload-pack"),
            "application/x-git-upload-pack-request"
        );
    }
}
