//! S3 operation handlers
//!
//! Each function implements one S3 REST API operation.

use crate::s3::error::S3Error;
use crate::s3::http::HttpResponse;
use crate::s3::xml;
use crate::storage::spi_client::S3StorageClient;
use crate::storage::ContentStore;

/// Convert a PostgreSQL timestamp string to RFC 7231 HTTP-date format.
/// Input: "2026-03-01 18:17:25.687517+00" (from `last_modified::text`)
/// Output: "Sat, 01 Mar 2026 18:17:25 GMT"
fn to_http_date(pg_timestamp: &str) -> String {
    // Parse: "YYYY-MM-DD HH:MM:SS.microseconds+00"
    let ts = pg_timestamp.trim();
    // Take only the date+time portion before any fractional seconds or timezone
    let dt_str = ts.split('.').next().unwrap_or(ts);
    let dt_str = dt_str.split('+').next().unwrap_or(dt_str);
    let dt_str = dt_str.split('-').collect::<Vec<_>>();

    if dt_str.len() < 3 {
        return pg_timestamp.to_string(); // fallback
    }

    // Parse components from "YYYY-MM-DD HH:MM:SS"
    let year: i32 = dt_str[0].parse().unwrap_or(2000);
    let month: u32 = dt_str[1].parse().unwrap_or(1);

    // dt_str[2] contains "DD HH:MM:SS"
    let rest_parts: Vec<&str> = dt_str[2].splitn(2, ' ').collect();
    let day: u32 = rest_parts[0].parse().unwrap_or(1);
    let time = if rest_parts.len() > 1 {
        rest_parts[1]
    } else {
        "00:00:00"
    };

    let month_name = match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "Jan",
    };

    // Compute day of week using Tomohiko Sakamoto's algorithm
    let day_name = {
        let mut y = year;
        let m = month as i32;
        let d = day as i32;
        static T: [i32; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
        if m < 3 {
            y -= 1;
        }
        let dow = (y + y / 4 - y / 100 + y / 400 + T[(m - 1) as usize] + d) % 7;
        match dow {
            0 => "Sun",
            1 => "Mon",
            2 => "Tue",
            3 => "Wed",
            4 => "Thu",
            5 => "Fri",
            6 => "Sat",
            _ => "Mon",
        }
    };

    format!(
        "{}, {:02} {} {} {} GMT",
        day_name, day, month_name, year, time
    )
}

/// Validate a bucket name following S3 naming rules
fn validate_bucket_name(name: &str) -> Result<(), String> {
    if name.len() < 3 || name.len() > 63 {
        return Err("Bucket name must be between 3 and 63 characters".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("Bucket name must contain only lowercase letters, numbers, and hyphens".into());
    }
    if !name.starts_with(|c: char| c.is_ascii_alphanumeric())
        || !name.ends_with(|c: char| c.is_ascii_alphanumeric())
    {
        return Err("Bucket name must start and end with a letter or number".into());
    }
    if name.contains("--") {
        return Err("Bucket name must not contain consecutive hyphens".into());
    }
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return Err("Bucket name must not be formatted as an IP address".into());
    }
    Ok(())
}

// ── Bucket operations ──────────────────────────────────────────────

/// GET / → ListBuckets
pub async fn handle_list_buckets(storage: &S3StorageClient) -> HttpResponse {
    match storage.list_buckets().await {
        Ok(buckets) => {
            let body = xml::list_buckets_xml(&buckets);
            HttpResponse::xml(200, &body)
        }
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

/// PUT /{bucket} → CreateBucket
pub async fn handle_create_bucket(
    bucket: &str,
    storage: &S3StorageClient,
    data_dir: &str,
) -> HttpResponse {
    if let Err(msg) = validate_bucket_name(bucket) {
        return S3Error::InvalidBucketName(msg).to_response();
    }

    // Check if bucket already exists
    match storage.bucket_exists(bucket).await {
        Ok(true) => return S3Error::BucketAlreadyOwnedByYou(bucket.to_string()).to_response(),
        Ok(false) => {}
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
    }

    match storage.create_bucket(bucket).await {
        Ok(_) => {
            // Create directory on disk
            if let Err(e) = ContentStore::ensure_bucket_dir(data_dir, bucket).await {
                eprintln!("pg_s3: failed to create bucket dir: {}", e);
            }
            HttpResponse::ok_with_headers(vec![("Location".into(), format!("/{}", bucket))])
        }
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

/// DELETE /{bucket} → DeleteBucket
pub async fn handle_delete_bucket(
    bucket: &str,
    storage: &S3StorageClient,
    data_dir: &str,
) -> HttpResponse {
    // Check bucket exists
    match storage.bucket_exists(bucket).await {
        Ok(false) => return S3Error::NoSuchBucket(bucket.to_string()).to_response(),
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
        Ok(true) => {}
    }

    // Check bucket is empty
    match storage.bucket_is_empty(bucket).await {
        Ok(false) => return S3Error::BucketNotEmpty(bucket.to_string()).to_response(),
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
        Ok(true) => {}
    }

    match storage.delete_bucket(bucket).await {
        Ok(_) => {
            if let Err(e) = ContentStore::remove_bucket_dir(data_dir, bucket).await {
                eprintln!("pg_s3: failed to remove bucket dir: {}", e);
            }
            HttpResponse::no_content()
        }
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

/// HEAD /{bucket} → HeadBucket
pub async fn handle_head_bucket(bucket: &str, storage: &S3StorageClient) -> HttpResponse {
    match storage.bucket_exists(bucket).await {
        Ok(true) => HttpResponse::ok_empty(),
        Ok(false) => S3Error::NoSuchBucket(bucket.to_string()).to_response(),
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

// ── Object operations ──────────────────────────────────────────────

/// GET /{bucket}?list-type=2 → ListObjectsV2
pub async fn handle_list_objects(
    bucket: &str,
    query_params: &std::collections::HashMap<String, String>,
    storage: &S3StorageClient,
) -> HttpResponse {
    // Check bucket exists
    match storage.bucket_exists(bucket).await {
        Ok(false) => return S3Error::NoSuchBucket(bucket.to_string()).to_response(),
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
        Ok(true) => {}
    }

    let prefix = query_params.get("prefix").map(|s| s.as_str()).unwrap_or("");
    let delimiter = query_params
        .get("delimiter")
        .map(|s| s.as_str())
        .unwrap_or("");
    let max_keys: i32 = query_params
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let start_after = query_params
        .get("start-after")
        .map(|s| s.as_str())
        .unwrap_or("");

    // Fetch more than max_keys to detect truncation
    let fetch_limit = max_keys + 1;

    match storage
        .list_objects(bucket, prefix, fetch_limit, start_after)
        .await
    {
        Ok(all_objects) => {
            let is_truncated = all_objects.len() > max_keys as usize;

            // Apply delimiter logic
            if delimiter.is_empty() {
                let objects: Vec<_> = all_objects.into_iter().take(max_keys as usize).collect();
                let key_count = objects.len();
                let body = xml::list_objects_xml(
                    bucket,
                    prefix,
                    delimiter,
                    &objects,
                    &[],
                    is_truncated,
                    key_count,
                    max_keys,
                );
                HttpResponse::xml(200, &body)
            } else {
                // Apply delimiter: extract common prefixes
                let mut result_objects = Vec::new();
                let mut common_prefixes = Vec::new();
                let mut seen_prefixes = std::collections::HashSet::new();

                for obj in all_objects.iter().take(max_keys as usize) {
                    let after_prefix = &obj.key[prefix.len()..];
                    if let Some(pos) = after_prefix.find(delimiter) {
                        let cp = format!("{}{}{}", prefix, &after_prefix[..pos], delimiter);
                        if seen_prefixes.insert(cp.clone()) {
                            common_prefixes.push(cp);
                        }
                    } else {
                        result_objects.push(obj.clone());
                    }
                }

                let key_count = result_objects.len() + common_prefixes.len();
                let body = xml::list_objects_xml(
                    bucket,
                    prefix,
                    delimiter,
                    &result_objects,
                    &common_prefixes,
                    is_truncated,
                    key_count,
                    max_keys,
                );
                HttpResponse::xml(200, &body)
            }
        }
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

/// PUT /{bucket}/{key+} → PutObject
pub async fn handle_put_object(
    bucket: &str,
    key: &str,
    body: &[u8],
    content_type: &str,
    metadata_headers: Vec<(String, String)>,
    storage: &S3StorageClient,
    data_dir: &str,
) -> HttpResponse {
    // Validate key
    if key.is_empty() || key.len() > 1024 {
        return S3Error::InvalidBucketName("Object key must be 1-1024 characters".into())
            .to_response();
    }

    // Get bucket ID
    let bucket_id = match storage.get_bucket_id(bucket).await {
        Ok(Some(id)) => id,
        Ok(None) => return S3Error::NoSuchBucket(bucket.to_string()).to_response(),
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
    };

    // Write content to disk
    let content_info = match ContentStore::write(data_dir, bucket, body).await {
        Ok(info) => info,
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
    };

    // Build custom metadata JSON from x-amz-meta-* headers
    let custom_metadata = if metadata_headers.is_empty() {
        None
    } else {
        let meta: serde_json::Value = metadata_headers
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect::<serde_json::Map<String, serde_json::Value>>()
            .into();
        Some(meta.to_string())
    };

    // Upsert metadata
    let old_hash = match storage
        .put_object_metadata(
            bucket_id,
            key,
            content_info.size as i64,
            &content_info.hash,
            content_type,
            &content_info.storage_path,
            custom_metadata.as_deref(),
        )
        .await
    {
        Ok(old) => old,
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
    };

    // Clean up old content if hash changed and unreferenced
    if let Some(old_hash) = old_hash {
        if let Ok(0) = storage.check_content_references(&old_hash).await {
            let old_prefix = &old_hash[..2];
            let old_path = format!("{}/{}/{}", bucket, old_prefix, old_hash);
            let _ = ContentStore::delete_if_unreferenced(data_dir, &old_path).await;
        }
    }

    HttpResponse::ok_with_headers(vec![("ETag".into(), format!("\"{}\"", content_info.hash))])
}

/// GET /{bucket}/{key+} → GetObject
pub async fn handle_get_object(
    bucket: &str,
    key: &str,
    storage: &S3StorageClient,
    data_dir: &str,
) -> HttpResponse {
    let meta = match storage.get_object_metadata(bucket, key).await {
        Ok(Some(m)) => m,
        Ok(None) => return S3Error::NoSuchKey(format!("{}/{}", bucket, key)).to_response(),
        Err(e) => return S3Error::InternalError(e.to_string()).to_response(),
    };

    match ContentStore::read(data_dir, &meta.storage_path).await {
        Ok(content) => HttpResponse::binary(
            200,
            &meta.content_type,
            content,
            vec![
                ("ETag".into(), format!("\"{}\"", meta.content_hash)),
                ("Last-Modified".into(), to_http_date(&meta.last_modified)),
            ],
        ),
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

/// DELETE /{bucket}/{key+} → DeleteObject
pub async fn handle_delete_object(
    bucket: &str,
    key: &str,
    storage: &S3StorageClient,
    data_dir: &str,
) -> HttpResponse {
    match storage.delete_object_metadata(bucket, key).await {
        Ok(Some((hash, storage_path))) => {
            // Check if content is still referenced
            if let Ok(0) = storage.check_content_references(&hash).await {
                let _ = ContentStore::delete_if_unreferenced(data_dir, &storage_path).await;
            }
            HttpResponse::no_content()
        }
        Ok(None) => HttpResponse::no_content(), // S3 returns 204 even for non-existent keys
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

/// HEAD /{bucket}/{key+} → HeadObject
pub async fn handle_head_object(
    bucket: &str,
    key: &str,
    storage: &S3StorageClient,
) -> HttpResponse {
    match storage.get_object_metadata(bucket, key).await {
        Ok(Some(meta)) => HttpResponse::ok_with_headers(vec![
            ("Content-Type".into(), meta.content_type),
            ("Content-Length".into(), meta.size_bytes.to_string()),
            ("ETag".into(), format!("\"{}\"", meta.content_hash)),
            ("Last-Modified".into(), to_http_date(&meta.last_modified)),
        ]),
        Ok(None) => S3Error::NoSuchKey(format!("{}/{}", bucket, key)).to_response(),
        Err(e) => S3Error::InternalError(e.to_string()).to_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_http_date() {
        assert_eq!(
            to_http_date("2026-03-01 18:17:25.687517+00"),
            "Sun, 01 Mar 2026 18:17:25 GMT"
        );
        assert_eq!(
            to_http_date("2024-01-15 09:30:00.000000+00"),
            "Mon, 15 Jan 2024 09:30:00 GMT"
        );
        assert_eq!(
            to_http_date("2025-12-25 00:00:00+00"),
            "Thu, 25 Dec 2025 00:00:00 GMT"
        );
    }

    #[test]
    fn test_validate_bucket_name() {
        assert!(validate_bucket_name("my-bucket").is_ok());
        assert!(validate_bucket_name("ab").is_err()); // too short
        assert!(validate_bucket_name("MyBucket").is_err()); // uppercase
        assert!(validate_bucket_name("my--bucket").is_err()); // consecutive hyphens
        assert!(validate_bucket_name("-bucket").is_err()); // starts with hyphen
    }
}
