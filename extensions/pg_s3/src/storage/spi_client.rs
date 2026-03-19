//! S3 storage client using SpiBridge for async-safe PostgreSQL access
//!
//! All methods are async and communicate with PostgreSQL through the SpiBridge
//! channel, which executes SPI queries on the background worker's main thread.

use pg_spi::{ColumnType, SpiBridge, SpiError, SpiParam};
use std::sync::Arc;

/// Bucket metadata
#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub created_at: String,
}

/// Object metadata
#[derive(Debug, Clone)]
pub struct ObjectMeta {
    pub key: String,
    pub size_bytes: i64,
    pub content_hash: String,
    pub content_type: String,
    pub storage_path: String,
    pub last_modified: String,
    pub custom_metadata: Option<String>,
}

/// Async S3 storage client wrapping SpiBridge
#[derive(Clone)]
pub struct S3StorageClient {
    bridge: Arc<SpiBridge>,
}

impl S3StorageClient {
    pub fn new(bridge: Arc<SpiBridge>) -> Self {
        Self { bridge }
    }

    // ── Bucket operations ──────────────────────────────────────────

    pub async fn list_buckets(&self) -> Result<Vec<BucketInfo>, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT name, created_at::text FROM pgs3.buckets ORDER BY name",
                vec![],
                vec![ColumnType::Text, ColumnType::Text],
            )
            .await?;

        Ok(result
            .iter()
            .map(|row| BucketInfo {
                name: row.get_string(0).unwrap_or_default(),
                created_at: row.get_string(1).unwrap_or_default(),
            })
            .collect())
    }

    pub async fn get_bucket_id(&self, name: &str) -> Result<Option<i64>, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT id FROM pgs3.buckets WHERE name = $1",
                vec![SpiParam::Text(Some(name.to_string()))],
                vec![ColumnType::Int64],
            )
            .await?;

        Ok(result.first().map(|row| row.get_i64(0).unwrap_or(0)))
    }

    pub async fn create_bucket(&self, name: &str) -> Result<i64, SpiError> {
        self.bridge
            .query_one_i64(
                "INSERT INTO pgs3.buckets (name) VALUES ($1) RETURNING id",
                vec![SpiParam::Text(Some(name.to_string()))],
            )
            .await
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<(), SpiError> {
        self.bridge
            .execute(
                "DELETE FROM pgs3.buckets WHERE name = $1",
                vec![SpiParam::Text(Some(name.to_string()))],
            )
            .await
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT 1 FROM pgs3.buckets WHERE name = $1",
                vec![SpiParam::Text(Some(name.to_string()))],
                vec![ColumnType::Int32],
            )
            .await?;

        Ok(!result.is_empty())
    }

    pub async fn bucket_is_empty(&self, name: &str) -> Result<bool, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT 1 FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = $1 LIMIT 1",
                vec![SpiParam::Text(Some(name.to_string()))],
                vec![ColumnType::Int32],
            )
            .await?;

        Ok(result.is_empty())
    }

    // ── Object operations ──────────────────────────────────────────

    /// Upsert object metadata. Returns the old content_hash if the object existed
    /// with different content (for cleanup).
    pub async fn put_object_metadata(
        &self,
        bucket_id: i64,
        key: &str,
        size_bytes: i64,
        content_hash: &str,
        content_type: &str,
        storage_path: &str,
        custom_metadata: Option<&str>,
    ) -> Result<Option<String>, SpiError> {
        let meta_param = match custom_metadata {
            Some(m) => SpiParam::Json(Some(serde_json::from_str(m).unwrap_or_default())),
            None => SpiParam::Json(None),
        };

        let result = self
            .bridge
            .query(
                "INSERT INTO pgs3.objects (bucket_id, key, size_bytes, content_hash, content_type, storage_path, custom_metadata, last_modified)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, now())
                 ON CONFLICT (bucket_id, key) DO UPDATE SET
                    size_bytes = EXCLUDED.size_bytes,
                    content_hash = EXCLUDED.content_hash,
                    content_type = EXCLUDED.content_type,
                    storage_path = EXCLUDED.storage_path,
                    custom_metadata = EXCLUDED.custom_metadata,
                    last_modified = now()
                 RETURNING (SELECT o2.content_hash FROM pgs3.objects o2 WHERE o2.bucket_id = $1 AND o2.key = $2 AND o2.content_hash != $4)",
                vec![
                    SpiParam::Int8(Some(bucket_id)),
                    SpiParam::Text(Some(key.to_string())),
                    SpiParam::Int8(Some(size_bytes)),
                    SpiParam::Text(Some(content_hash.to_string())),
                    SpiParam::Text(Some(content_type.to_string())),
                    SpiParam::Text(Some(storage_path.to_string())),
                    meta_param,
                ],
                vec![ColumnType::Text],
            )
            .await?;

        Ok(result.first().and_then(|row| row.get_string(0)))
    }

    pub async fn get_object_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectMeta>, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT o.key, o.size_bytes, o.content_hash, o.content_type, o.storage_path, o.last_modified::text, o.custom_metadata::text
                 FROM pgs3.objects o
                 JOIN pgs3.buckets b ON o.bucket_id = b.id
                 WHERE b.name = $1 AND o.key = $2",
                vec![
                    SpiParam::Text(Some(bucket.to_string())),
                    SpiParam::Text(Some(key.to_string())),
                ],
                vec![
                    ColumnType::Text,
                    ColumnType::Int64,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                ],
            )
            .await?;

        Ok(result.first().map(|row| ObjectMeta {
            key: row.get_string(0).unwrap_or_default(),
            size_bytes: row.get_i64(1).unwrap_or(0),
            content_hash: row.get_string(2).unwrap_or_default(),
            content_type: row.get_string(3).unwrap_or_default(),
            storage_path: row.get_string(4).unwrap_or_default(),
            last_modified: row.get_string(5).unwrap_or_default(),
            custom_metadata: row.get_string(6),
        }))
    }

    /// Delete object metadata and return (content_hash, storage_path) for cleanup.
    pub async fn delete_object_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<(String, String)>, SpiError> {
        let result = self
            .bridge
            .query(
                "DELETE FROM pgs3.objects
                 WHERE bucket_id = (SELECT id FROM pgs3.buckets WHERE name = $1)
                   AND key = $2
                 RETURNING content_hash, storage_path",
                vec![
                    SpiParam::Text(Some(bucket.to_string())),
                    SpiParam::Text(Some(key.to_string())),
                ],
                vec![ColumnType::Text, ColumnType::Text],
            )
            .await?;

        Ok(result.first().map(|row| {
            (
                row.get_string(0).unwrap_or_default(),
                row.get_string(1).unwrap_or_default(),
            )
        }))
    }

    /// List objects matching prefix, ordered by key.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        max_keys: i32,
        start_after: &str,
    ) -> Result<Vec<ObjectMeta>, SpiError> {
        let result = self
            .bridge
            .query(
                "SELECT o.key, o.size_bytes, o.content_hash, o.content_type, o.storage_path, o.last_modified::text, o.custom_metadata::text
                 FROM pgs3.objects o
                 JOIN pgs3.buckets b ON o.bucket_id = b.id
                 WHERE b.name = $1
                   AND o.key LIKE $2
                   AND o.key > $3
                 ORDER BY o.key
                 LIMIT $4",
                vec![
                    SpiParam::Text(Some(bucket.to_string())),
                    SpiParam::Text(Some(format!("{}%", prefix.replace('%', "\\%")))),
                    SpiParam::Text(Some(start_after.to_string())),
                    SpiParam::Int4(Some(max_keys)),
                ],
                vec![
                    ColumnType::Text,
                    ColumnType::Int64,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                ],
            )
            .await?;

        Ok(result
            .iter()
            .map(|row| ObjectMeta {
                key: row.get_string(0).unwrap_or_default(),
                size_bytes: row.get_i64(1).unwrap_or(0),
                content_hash: row.get_string(2).unwrap_or_default(),
                content_type: row.get_string(3).unwrap_or_default(),
                storage_path: row.get_string(4).unwrap_or_default(),
                last_modified: row.get_string(5).unwrap_or_default(),
                custom_metadata: row.get_string(6),
            })
            .collect())
    }

    /// Check how many objects reference a given content hash.
    pub async fn check_content_references(&self, hash: &str) -> Result<i64, SpiError> {
        self.bridge
            .query_one_i64(
                "SELECT COUNT(*)::bigint FROM pgs3.objects WHERE content_hash = $1",
                vec![SpiParam::Text(Some(hash.to_string()))],
            )
            .await
    }
}
