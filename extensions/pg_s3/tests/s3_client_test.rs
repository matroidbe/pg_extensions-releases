//! Integration tests using the official AWS SDK S3 client against pg_s3
//!
//! These tests validate real S3 protocol compatibility by using the same client
//! that applications use to talk to AWS S3. If these tests pass, any S3 client
//! (boto3, aws-cli, aws-sdk-*) will work with pg_s3.
//!
//! Prerequisites:
//! - pg_s3 server running on localhost:9100
//! - Extension installed and configured via test.sh
//!
//! Run with: ./test.sh --no-coverage
//! Or manually: cargo test --test s3_client_test -- --nocapture --test-threads=1

mod common;

use aws_credential_types::Credentials;
use aws_sdk_s3::config::{BehaviorVersion, Builder, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::time::Duration;

const TEST_BUCKET: &str = "integration-test-bucket";

/// Build an S3 client pointing at the local pg_s3 server
fn s3_client() -> Client {
    let creds = Credentials::from_keys("test", "test", None);
    let config = Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(common::s3_base_url())
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    Client::from_conf(config)
}

/// Create a tokio runtime for async test operations
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("Failed to create tokio runtime")
}

/// Ensure the test bucket exists
fn ensure_test_bucket(client: &Client, rt: &tokio::runtime::Runtime) {
    let _ = rt.block_on(client.create_bucket().bucket(TEST_BUCKET).send());
}

/// Cleanup: delete known test objects and the bucket
fn cleanup_test_bucket(client: &Client, rt: &tokio::runtime::Runtime) {
    let keys = [
        "hello.txt",
        "binary.dat",
        "images/cat.jpg",
        "images/dog.jpg",
        "docs/readme.md",
        "root.txt",
        "overwrite.txt",
        "meta-test.txt",
        "dedup-a.txt",
        "dedup-b.txt",
    ];
    for key in keys {
        let _ = rt.block_on(client.delete_object().bucket(TEST_BUCKET).key(key).send());
    }
    let _ = rt.block_on(client.delete_bucket().bucket(TEST_BUCKET).send());
}

// ============================================================================
// Server connectivity
// ============================================================================

/// Test: S3 server is accepting connections
#[test]
fn test_server_running() {
    skip_if_no_server!(common::S3_ADDR);
    println!(
        "pg_s3 server is accepting connections at {}",
        common::S3_ADDR
    );
}

// ============================================================================
// Bucket operations
// ============================================================================

/// Test: List buckets (GET /)
#[test]
fn test_list_buckets() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();

    let resp = rt
        .block_on(client.list_buckets().send())
        .expect("ListBuckets failed");

    let buckets = resp.buckets();
    println!("ListBuckets returned {} buckets", buckets.len());
    for b in buckets {
        println!("  bucket: {:?}", b.name());
    }
}

/// Test: Create and delete a bucket using real S3 SDK
#[test]
fn test_create_and_delete_bucket() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    let bucket = "test-create-delete";

    // Create bucket
    rt.block_on(client.create_bucket().bucket(bucket).send())
        .expect("CreateBucket failed");
    println!("Created bucket: {}", bucket);

    // Verify via SQL
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM pgs3.buckets WHERE name = '{}';",
        bucket
    ))
    .expect("SQL query failed");
    assert_eq!(count.trim(), "1", "Bucket should exist in database");

    // Delete bucket
    rt.block_on(client.delete_bucket().bucket(bucket).send())
        .expect("DeleteBucket failed");
    println!("Deleted bucket: {}", bucket);

    // Verify deleted via SQL
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM pgs3.buckets WHERE name = '{}';",
        bucket
    ))
    .expect("SQL query failed");
    assert_eq!(count.trim(), "0", "Bucket should be gone from database");
}

/// Test: Head bucket (check existence)
#[test]
fn test_head_bucket() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    let bucket = "test-head-bucket";

    rt.block_on(client.create_bucket().bucket(bucket).send())
        .expect("CreateBucket failed");

    // Head existing bucket — should succeed
    rt.block_on(client.head_bucket().bucket(bucket).send())
        .expect("HeadBucket should succeed for existing bucket");

    // Head nonexistent bucket — should fail
    let err = rt
        .block_on(client.head_bucket().bucket("nonexistent-bucket-xyz").send())
        .expect_err("HeadBucket should fail for missing bucket");
    println!("HeadBucket error for missing bucket: {:?}", err);

    // Cleanup
    let _ = rt.block_on(client.delete_bucket().bucket(bucket).send());
}

/// Test: Create bucket with invalid name returns error
#[test]
fn test_create_bucket_invalid_name() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();

    // Too short
    let err = rt
        .block_on(client.create_bucket().bucket("ab").send())
        .expect_err("Should reject short bucket name");
    println!("Short name error: {:?}", err);

    // Uppercase
    let err = rt
        .block_on(client.create_bucket().bucket("MyBucket").send())
        .expect_err("Should reject uppercase bucket name");
    println!("Uppercase error: {:?}", err);
}

/// Test: Delete non-empty bucket fails
#[test]
fn test_delete_nonempty_bucket_fails() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    let bucket = "test-nonempty-delete";

    rt.block_on(client.create_bucket().bucket(bucket).send())
        .expect("CreateBucket failed");

    rt.block_on(
        client
            .put_object()
            .bucket(bucket)
            .key("file.txt")
            .content_type("text/plain")
            .body(ByteStream::from(b"content".to_vec()))
            .send(),
    )
    .expect("PutObject failed");

    // Delete non-empty bucket should fail
    let err = rt
        .block_on(client.delete_bucket().bucket(bucket).send())
        .expect_err("Should reject deleting non-empty bucket");
    println!("Non-empty bucket delete error: {:?}", err);

    // Cleanup
    let _ = rt.block_on(client.delete_object().bucket(bucket).key("file.txt").send());
    let _ = rt.block_on(client.delete_bucket().bucket(bucket).send());
}

// ============================================================================
// Object operations
// ============================================================================

/// Test: Put and Get an object via real S3 SDK
#[test]
fn test_put_and_get_object() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    let content = b"Hello, pg_s3 integration test!";

    // Put object
    let put_resp = rt
        .block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key("hello.txt")
                .content_type("text/plain")
                .body(ByteStream::from(content.to_vec()))
                .send(),
        )
        .expect("PutObject failed");

    let etag = put_resp.e_tag();
    assert!(etag.is_some(), "PutObject should return ETag");
    println!("PutObject ETag: {:?}", etag);

    // Get object
    let get_resp = rt
        .block_on(
            client
                .get_object()
                .bucket(TEST_BUCKET)
                .key("hello.txt")
                .send(),
        )
        .expect("GetObject failed");

    assert_eq!(
        get_resp.content_type(),
        Some("text/plain"),
        "Content-Type should match"
    );
    assert_eq!(
        get_resp.content_length(),
        Some(content.len() as i64),
        "Content-Length should match"
    );

    let body = rt
        .block_on(get_resp.body.collect())
        .expect("Failed to read body")
        .into_bytes();
    assert_eq!(body.as_ref(), content, "Object content should match");
    println!("GetObject returned {} bytes", body.len());

    // Verify metadata stored in database
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = '{}' AND o.key = 'hello.txt';",
        TEST_BUCKET
    )).expect("SQL query failed");
    assert_eq!(count.trim(), "1", "Object metadata should be in database");

    cleanup_test_bucket(&client, &rt);
}

/// Test: Put object overwrites existing
#[test]
fn test_put_object_overwrite() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    // Put version 1
    let resp1 = rt
        .block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key("overwrite.txt")
                .content_type("text/plain")
                .body(ByteStream::from(b"version 1".to_vec()))
                .send(),
        )
        .expect("PutObject v1 failed");
    let etag1 = resp1.e_tag().map(|s| s.to_string());

    // Put version 2 (overwrite)
    let resp2 = rt
        .block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key("overwrite.txt")
                .content_type("text/plain")
                .body(ByteStream::from(b"version 2".to_vec()))
                .send(),
        )
        .expect("PutObject v2 failed");
    let etag2 = resp2.e_tag().map(|s| s.to_string());

    assert_ne!(etag1, etag2, "ETags should differ for different content");

    // Get should return version 2
    let get_resp = rt
        .block_on(
            client
                .get_object()
                .bucket(TEST_BUCKET)
                .key("overwrite.txt")
                .send(),
        )
        .expect("GetObject failed");

    let body = rt
        .block_on(get_resp.body.collect())
        .expect("Failed to read body")
        .into_bytes();
    assert_eq!(body.as_ref(), b"version 2", "Should get latest version");

    // Should only be one object row
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = '{}' AND o.key = 'overwrite.txt';",
        TEST_BUCKET
    )).expect("SQL query failed");
    assert_eq!(
        count.trim(),
        "1",
        "Should have exactly one metadata row (upsert)"
    );

    cleanup_test_bucket(&client, &rt);
}

/// Test: Delete an object
#[test]
fn test_delete_object() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    rt.block_on(
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key("hello.txt")
            .content_type("text/plain")
            .body(ByteStream::from(b"delete me".to_vec()))
            .send(),
    )
    .expect("PutObject failed");

    // Delete
    rt.block_on(
        client
            .delete_object()
            .bucket(TEST_BUCKET)
            .key("hello.txt")
            .send(),
    )
    .expect("DeleteObject failed");

    // Get should now fail
    let err = rt
        .block_on(
            client
                .get_object()
                .bucket(TEST_BUCKET)
                .key("hello.txt")
                .send(),
        )
        .expect_err("Deleted object should return error");
    println!("Get deleted object error: {:?}", err);

    // Delete nonexistent should still succeed (S3 behavior)
    rt.block_on(
        client
            .delete_object()
            .bucket(TEST_BUCKET)
            .key("nonexistent.txt")
            .send(),
    )
    .expect("Delete of missing object should succeed (S3 idempotent delete)");

    cleanup_test_bucket(&client, &rt);
}

/// Test: Head object returns metadata
#[test]
fn test_head_object_metadata() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    let content = b"head test content";
    rt.block_on(
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key("hello.txt")
            .content_type("application/json")
            .body(ByteStream::from(content.to_vec()))
            .send(),
    )
    .expect("PutObject failed");

    let resp = rt
        .block_on(
            client
                .head_object()
                .bucket(TEST_BUCKET)
                .key("hello.txt")
                .send(),
        )
        .expect("HeadObject failed");

    assert_eq!(
        resp.content_length(),
        Some(content.len() as i64),
        "Content-Length should match"
    );
    assert_eq!(
        resp.content_type(),
        Some("application/json"),
        "Content-Type should match"
    );
    assert!(resp.e_tag().is_some(), "Should have ETag");
    println!(
        "HeadObject: content_length={:?}, etag={:?}, content_type={:?}",
        resp.content_length(),
        resp.e_tag(),
        resp.content_type()
    );

    // Head nonexistent should fail
    let err = rt
        .block_on(
            client
                .head_object()
                .bucket(TEST_BUCKET)
                .key("nope.txt")
                .send(),
        )
        .expect_err("Missing object should return error");
    println!("HeadObject missing error: {:?}", err);

    cleanup_test_bucket(&client, &rt);
}

/// Test: Get nonexistent object returns NoSuchKey error
#[test]
fn test_get_nonexistent_object() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    let err = rt
        .block_on(
            client
                .get_object()
                .bucket(TEST_BUCKET)
                .key("does-not-exist.txt")
                .send(),
        )
        .expect_err("Should fail for nonexistent object");

    let err_str = format!("{:?}", err);
    println!("NoSuchKey error: {}", err_str);

    cleanup_test_bucket(&client, &rt);
}

/// Test: Binary object round-trip preserves all bytes
#[test]
fn test_binary_object() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    // Create binary content with all byte values
    let content: Vec<u8> = (0..=255).collect();

    rt.block_on(
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key("binary.dat")
            .content_type("application/octet-stream")
            .body(ByteStream::from(content.clone()))
            .send(),
    )
    .expect("PutObject binary failed");

    let get_resp = rt
        .block_on(
            client
                .get_object()
                .bucket(TEST_BUCKET)
                .key("binary.dat")
                .send(),
        )
        .expect("GetObject binary failed");

    let body = rt
        .block_on(get_resp.body.collect())
        .expect("Failed to read body")
        .into_bytes();
    assert_eq!(
        body.as_ref(),
        content.as_slice(),
        "Binary content should round-trip exactly"
    );
    println!("Binary round-trip: {} bytes OK", body.len());

    cleanup_test_bucket(&client, &rt);
}

/// Test: Put object with custom x-amz-meta-* headers
#[test]
fn test_put_object_with_metadata() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    rt.block_on(
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key("meta-test.txt")
            .content_type("text/plain")
            .metadata("department", "engineering")
            .metadata("project", "pg_s3")
            .body(ByteStream::from(b"metadata test".to_vec()))
            .send(),
    )
    .expect("PutObject with metadata failed");

    // Verify custom metadata stored in database
    let meta = common::run_sql(&format!(
        "SELECT o.custom_metadata::text FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = '{}' AND o.key = 'meta-test.txt';",
        TEST_BUCKET
    )).expect("SQL query failed");

    println!("Custom metadata from DB: {}", meta);
    assert!(
        meta.contains("engineering") || meta.contains("department"),
        "Custom metadata should be stored"
    );

    cleanup_test_bucket(&client, &rt);
}

// ============================================================================
// ListObjects operations
// ============================================================================

/// Test: List objects in a bucket using SDK's list_objects_v2
#[test]
fn test_list_objects() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    // Upload some objects
    for (key, body, ct) in [
        ("images/cat.jpg", b"cat".as_slice(), "image/jpeg"),
        ("images/dog.jpg", b"dog".as_slice(), "image/jpeg"),
        ("docs/readme.md", b"readme".as_slice(), "text/markdown"),
        ("root.txt", b"root".as_slice(), "text/plain"),
    ] {
        rt.block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key(key)
                .content_type(ct)
                .body(ByteStream::from(body.to_vec()))
                .send(),
        )
        .expect(&format!("PutObject {} failed", key));
    }

    // List all objects
    let resp = rt
        .block_on(client.list_objects_v2().bucket(TEST_BUCKET).send())
        .expect("ListObjectsV2 failed");

    let contents = resp.contents();
    let keys: Vec<&str> = contents.iter().filter_map(|o| o.key()).collect();
    println!("ListObjects returned {} objects: {:?}", keys.len(), keys);

    assert!(keys.contains(&"images/cat.jpg"), "Should list cat.jpg");
    assert!(keys.contains(&"images/dog.jpg"), "Should list dog.jpg");
    assert!(keys.contains(&"docs/readme.md"), "Should list readme.md");
    assert!(keys.contains(&"root.txt"), "Should list root.txt");

    // Each object should have size > 0
    for obj in contents {
        assert!(
            obj.size().unwrap_or(0) > 0,
            "Object {:?} should have size > 0",
            obj.key()
        );
    }

    cleanup_test_bucket(&client, &rt);
}

/// Test: List objects with prefix filter
#[test]
fn test_list_objects_with_prefix() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    for (key, body, ct) in [
        ("images/cat.jpg", b"cat".as_slice(), "image/jpeg"),
        ("images/dog.jpg", b"dog".as_slice(), "image/jpeg"),
        ("docs/readme.md", b"readme".as_slice(), "text/markdown"),
    ] {
        rt.block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key(key)
                .content_type(ct)
                .body(ByteStream::from(body.to_vec()))
                .send(),
        )
        .expect(&format!("PutObject {} failed", key));
    }

    // List only images/
    let resp = rt
        .block_on(
            client
                .list_objects_v2()
                .bucket(TEST_BUCKET)
                .prefix("images/")
                .send(),
        )
        .expect("ListObjectsV2 with prefix failed");

    let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();
    println!("ListObjects with prefix: {:?}", keys);

    assert!(keys.contains(&"images/cat.jpg"), "Should contain cat.jpg");
    assert!(keys.contains(&"images/dog.jpg"), "Should contain dog.jpg");
    assert!(
        !keys.iter().any(|k| k.starts_with("docs/")),
        "Should NOT contain docs/ objects"
    );

    cleanup_test_bucket(&client, &rt);
}

/// Test: List objects with delimiter shows CommonPrefixes (virtual folders)
#[test]
fn test_list_objects_with_delimiter() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    for (key, body, ct) in [
        ("images/cat.jpg", b"cat".as_slice(), "image/jpeg"),
        ("images/dog.jpg", b"dog".as_slice(), "image/jpeg"),
        ("docs/readme.md", b"readme".as_slice(), "text/markdown"),
        ("root.txt", b"root".as_slice(), "text/plain"),
    ] {
        rt.block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key(key)
                .content_type(ct)
                .body(ByteStream::from(body.to_vec()))
                .send(),
        )
        .expect(&format!("PutObject {} failed", key));
    }

    // List with delimiter — should show "folders" as CommonPrefixes
    let resp = rt
        .block_on(
            client
                .list_objects_v2()
                .bucket(TEST_BUCKET)
                .delimiter("/")
                .send(),
        )
        .expect("ListObjectsV2 with delimiter failed");

    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|cp| cp.prefix())
        .collect();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|o| o.key()).collect();

    println!("CommonPrefixes: {:?}", prefixes);
    println!("Objects at root: {:?}", keys);

    assert!(
        prefixes.contains(&"images/"),
        "Should have images/ prefix: {:?}",
        prefixes
    );
    assert!(
        prefixes.contains(&"docs/"),
        "Should have docs/ prefix: {:?}",
        prefixes
    );
    assert!(
        keys.contains(&"root.txt"),
        "root.txt should be a regular object"
    );
    // Objects inside prefixes should NOT appear as direct objects
    assert!(
        !keys.contains(&"images/cat.jpg"),
        "Prefixed objects should be hidden behind CommonPrefixes"
    );

    cleanup_test_bucket(&client, &rt);
}

// ============================================================================
// SQL + HTTP roundtrip tests
// ============================================================================

/// Test: Object uploaded via HTTP is visible via SQL
#[test]
fn test_http_upload_visible_in_sql() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    let content = b"SQL visibility test content";
    rt.block_on(
        client
            .put_object()
            .bucket(TEST_BUCKET)
            .key("hello.txt")
            .content_type("text/plain")
            .body(ByteStream::from(content.to_vec()))
            .send(),
    )
    .expect("PutObject failed");

    // Small delay for write to be committed
    std::thread::sleep(Duration::from_millis(200));

    // Verify metadata via SQL
    let size = common::run_sql(&format!(
        "SELECT o.size_bytes FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = '{}' AND o.key = 'hello.txt';",
        TEST_BUCKET
    )).expect("SQL query failed");

    assert_eq!(
        size.trim(),
        content.len().to_string(),
        "Size in DB should match uploaded content size"
    );

    let ct = common::run_sql(&format!(
        "SELECT o.content_type FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = '{}' AND o.key = 'hello.txt';",
        TEST_BUCKET
    )).expect("SQL query failed");

    assert_eq!(ct.trim(), "text/plain", "Content type should match");
    println!(
        "HTTP upload verified in SQL: size={}, content_type={}",
        size.trim(),
        ct.trim()
    );

    cleanup_test_bucket(&client, &rt);
}

/// Test: Object created via SQL is retrievable via HTTP (S3 SDK)
#[test]
fn test_sql_insert_visible_via_http() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    let bucket = "test-sql-to-http";

    // Create bucket via SQL
    let _ = common::run_sql(&format!("SELECT pgs3.create_bucket('{}');", bucket));

    // Put object via SQL
    let _ = common::run_sql(&format!(
        "SELECT pgs3.put_object('{}', 'from-sql.txt', '\\x48656c6c6f'::bytea, 'text/plain', NULL);",
        bucket
    ));

    std::thread::sleep(Duration::from_millis(200));

    // Retrieve via S3 SDK
    let get_resp = rt
        .block_on(
            client
                .get_object()
                .bucket(bucket)
                .key("from-sql.txt")
                .send(),
        )
        .expect("Should be able to GET object created via SQL");

    let body = rt
        .block_on(get_resp.body.collect())
        .expect("Failed to read body")
        .into_bytes();
    assert_eq!(
        body.as_ref(),
        b"Hello",
        "Content from SQL insert should match (0x48656c6c6f = 'Hello')"
    );
    println!("SQL object retrieved via S3 SDK: {} bytes", body.len());

    // Cleanup via SQL
    let _ = common::run_sql(&format!(
        "SELECT pgs3.delete_object('{}', 'from-sql.txt');",
        bucket
    ));
    let _ = common::run_sql(&format!("SELECT pgs3.delete_bucket('{}', false);", bucket));
}

// ============================================================================
// Content deduplication
// ============================================================================

/// Test: Identical content is deduplicated on disk
#[test]
fn test_content_deduplication() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    ensure_test_bucket(&client, &rt);

    let content = b"deduplicated content shared across objects";

    // Upload same content with different keys
    let resp1 = rt
        .block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key("dedup-a.txt")
                .content_type("text/plain")
                .body(ByteStream::from(content.to_vec()))
                .send(),
        )
        .expect("PutObject dedup-a failed");

    let resp2 = rt
        .block_on(
            client
                .put_object()
                .bucket(TEST_BUCKET)
                .key("dedup-b.txt")
                .content_type("text/plain")
                .body(ByteStream::from(content.to_vec()))
                .send(),
        )
        .expect("PutObject dedup-b failed");

    let etag1 = resp1.e_tag().map(|s| s.to_string());
    let etag2 = resp2.e_tag().map(|s| s.to_string());

    assert_eq!(
        etag1, etag2,
        "Same content should produce same ETag (content hash)"
    );

    // Verify both keys have same content_hash in DB
    let hash = common::run_sql(&format!(
        "SELECT COUNT(DISTINCT content_hash) FROM pgs3.objects o JOIN pgs3.buckets b ON o.bucket_id = b.id WHERE b.name = '{}' AND o.key IN ('dedup-a.txt', 'dedup-b.txt');",
        TEST_BUCKET
    )).expect("SQL query failed");
    assert_eq!(
        hash.trim(),
        "1",
        "Both objects should share the same content hash"
    );

    println!("Content deduplication verified: ETag = {:?}", etag1);

    cleanup_test_bucket(&client, &rt);
}

// ============================================================================
// Error handling
// ============================================================================

/// Test: Operations on nonexistent bucket return proper errors
#[test]
fn test_nonexistent_bucket_errors() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();

    // Put to nonexistent bucket
    let err = rt
        .block_on(
            client
                .put_object()
                .bucket("no-such-bucket-xyz")
                .key("test.txt")
                .content_type("text/plain")
                .body(ByteStream::from(b"data".to_vec()))
                .send(),
        )
        .expect_err("Should fail for nonexistent bucket");
    println!("PutObject to missing bucket error: {:?}", err);

    // Get from nonexistent bucket
    let err = rt
        .block_on(
            client
                .get_object()
                .bucket("no-such-bucket-xyz")
                .key("test.txt")
                .send(),
        )
        .expect_err("Should fail for nonexistent bucket");
    println!("GetObject from missing bucket error: {:?}", err);
}

/// Test: Duplicate bucket creation returns proper error
#[test]
fn test_create_duplicate_bucket() {
    skip_if_no_server!(common::S3_ADDR);
    let client = s3_client();
    let rt = runtime();
    let bucket = "test-dup-create";

    rt.block_on(client.create_bucket().bucket(bucket).send())
        .expect("First CreateBucket failed");

    let err = rt
        .block_on(client.create_bucket().bucket(bucket).send())
        .expect_err("Duplicate bucket should fail");
    println!("Duplicate bucket error: {:?}", err);

    // Cleanup
    let _ = rt.block_on(client.delete_bucket().bucket(bucket).send());
}
