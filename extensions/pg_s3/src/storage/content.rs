//! Content store — binary data on the local filesystem
//!
//! Files are stored using content-addressable naming (SHA-256 hash).
//! This provides natural deduplication and ETag generation.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Result of writing content to disk
pub struct ContentInfo {
    /// SHA-256 hex digest of the content
    pub hash: String,
    /// Size in bytes
    pub size: u64,
    /// Relative storage path (e.g., "my-bucket/ab/abcdef...")
    pub storage_path: String,
}

/// Manages binary content on the filesystem
pub struct ContentStore;

impl ContentStore {
    /// Ensure required directories exist
    pub async fn ensure_directories(
        data_dir: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let base = Path::new(data_dir);
        fs::create_dir_all(base.join(".tmp")).await?;
        Ok(())
    }

    /// Write content to disk, returning hash and storage path.
    /// Content is streamed to a temp file, then renamed to final location.
    pub async fn write(
        data_dir: &str,
        bucket: &str,
        content: &[u8],
    ) -> Result<ContentInfo, Box<dyn std::error::Error + Send + Sync>> {
        let base = Path::new(data_dir);

        // 1. Write to temp file while computing hash
        let tmp_name = format!("upload_{}.tmp", Uuid::new_v4());
        let tmp_path = base.join(".tmp").join(&tmp_name);

        let mut file = fs::File::create(&tmp_path).await?;
        let mut hasher = Sha256::new();

        hasher.update(content);
        file.write_all(content).await?;
        file.flush().await?;
        drop(file);

        let size = content.len() as u64;
        let hash = hex::encode(hasher.finalize());
        let prefix = &hash[..2];

        // 2. Determine final path
        let content_dir = base.join(bucket).join(prefix);
        let final_path = content_dir.join(&hash);

        // 3. Dedup check
        if final_path.exists() {
            // Content already exists, delete temp
            fs::remove_file(&tmp_path).await?;
        } else {
            // Move temp to final location
            fs::create_dir_all(&content_dir).await?;
            fs::rename(&tmp_path, &final_path).await?;
        }

        let storage_path = format!("{}/{}/{}", bucket, prefix, hash);

        Ok(ContentInfo {
            hash,
            size,
            storage_path,
        })
    }

    /// Read content from disk
    pub async fn read(
        data_dir: &str,
        storage_path: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let full_path = Path::new(data_dir).join(storage_path);
        let content = fs::read(&full_path).await?;
        Ok(content)
    }

    /// Delete a content file if no other objects reference it
    pub async fn delete_if_unreferenced(
        data_dir: &str,
        storage_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let full_path = Path::new(data_dir).join(storage_path);
        if full_path.exists() {
            fs::remove_file(&full_path).await?;
        }
        Ok(())
    }

    /// Ensure a bucket directory exists
    pub async fn ensure_bucket_dir(
        data_dir: &str,
        bucket: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bucket_path = Path::new(data_dir).join(bucket);
        fs::create_dir_all(&bucket_path).await?;
        Ok(())
    }

    /// Remove a bucket directory (must be empty)
    pub async fn remove_bucket_dir(
        data_dir: &str,
        bucket: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bucket_path = Path::new(data_dir).join(bucket);
        if bucket_path.exists() {
            fs::remove_dir_all(&bucket_path).await?;
        }
        Ok(())
    }

    /// Clean up orphaned temp files (called on startup)
    pub async fn cleanup_temp_files(
        data_dir: &str,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let tmp_dir = Path::new(data_dir).join(".tmp");
        let mut count = 0u64;

        if !tmp_dir.exists() {
            return Ok(0);
        }

        let mut entries = fs::read_dir(&tmp_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                fs::remove_file(&path).await?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Get the full filesystem path for a storage path
    pub fn full_path(data_dir: &str, storage_path: &str) -> PathBuf {
        Path::new(data_dir).join(storage_path)
    }
}
