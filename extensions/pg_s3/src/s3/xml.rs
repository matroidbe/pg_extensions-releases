//! S3 XML response generators
//!
//! Produces standard AWS S3 XML response bodies using format! macros.

use crate::storage::spi_client::{BucketInfo, ObjectMeta};

/// Standard S3 error XML
pub fn error_xml(code: &str, message: &str, resource: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <Resource>{}</Resource>
  <RequestId>1</RequestId>
</Error>"#,
        xml_escape(code),
        xml_escape(message),
        xml_escape(resource)
    )
}

/// ListAllMyBucketsResult XML
pub fn list_buckets_xml(buckets: &[BucketInfo]) -> String {
    let mut bucket_entries = String::new();
    for b in buckets {
        bucket_entries.push_str(&format!(
            "    <Bucket>\n      <Name>{}</Name>\n      <CreationDate>{}</CreationDate>\n    </Bucket>\n",
            xml_escape(&b.name),
            xml_escape(&to_iso8601(&b.created_at))
        ));
    }

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner>
    <ID>pg_s3</ID>
    <DisplayName>pg_s3</DisplayName>
  </Owner>
  <Buckets>
{}  </Buckets>
</ListAllMyBucketsResult>"#,
        bucket_entries
    )
}

/// ListBucketResult (ListObjectsV2) XML
pub fn list_objects_xml(
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    objects: &[ObjectMeta],
    common_prefixes: &[String],
    is_truncated: bool,
    key_count: usize,
    max_keys: i32,
) -> String {
    let mut contents = String::new();
    for obj in objects {
        contents.push_str(&format!(
            "  <Contents>\n    <Key>{}</Key>\n    <LastModified>{}</LastModified>\n    <ETag>\"{}\"</ETag>\n    <Size>{}</Size>\n    <StorageClass>STANDARD</StorageClass>\n  </Contents>\n",
            xml_escape(&obj.key),
            xml_escape(&to_iso8601(&obj.last_modified)),
            xml_escape(&obj.content_hash),
            obj.size_bytes,
        ));
    }

    let mut prefixes_xml = String::new();
    for cp in common_prefixes {
        prefixes_xml.push_str(&format!(
            "  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>\n",
            xml_escape(cp)
        ));
    }

    let delimiter_xml = if delimiter.is_empty() {
        String::new()
    } else {
        format!("  <Delimiter>{}</Delimiter>\n", xml_escape(delimiter))
    };

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
{}  <KeyCount>{}</KeyCount>
  <MaxKeys>{}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
{}{}
</ListBucketResult>"#,
        xml_escape(bucket),
        xml_escape(prefix),
        delimiter_xml,
        key_count,
        max_keys,
        is_truncated,
        contents,
        prefixes_xml,
    )
}

/// Convert a PostgreSQL timestamp string to ISO 8601 format for S3 XML.
/// Input: "2026-03-01 18:17:25.687517+00"
/// Output: "2026-03-01T18:17:25.000Z"
fn to_iso8601(pg_timestamp: &str) -> String {
    let ts = pg_timestamp.trim();
    // Split off timezone suffix (+00, etc.)
    let without_tz = if let Some(pos) = ts.rfind('+') {
        &ts[..pos]
    } else if let Some(stripped) = ts.strip_suffix('Z') {
        stripped
    } else {
        ts
    };
    // Split date and time at space
    if let Some((date, time)) = without_tz.split_once(' ') {
        // Truncate fractional seconds and pad to .000
        let time_clean = time.split('.').next().unwrap_or(time);
        format!("{}T{}.000Z", date, time_clean)
    } else {
        // Already in some other format, return as-is
        pg_timestamp.to_string()
    }
}

/// XML-escape a string
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_xml() {
        let xml = error_xml("NoSuchBucket", "The bucket does not exist", "/my-bucket");
        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Message>The bucket does not exist</Message>"));
        assert!(xml.contains("<Resource>/my-bucket</Resource>"));
    }

    #[test]
    fn test_list_buckets_xml() {
        let buckets = vec![BucketInfo {
            name: "bucket-a".into(),
            created_at: "2024-01-01T00:00:00Z".into(),
        }];
        let xml = list_buckets_xml(&buckets);
        assert!(xml.contains("<Name>bucket-a</Name>"));
        assert!(xml.contains("ListAllMyBucketsResult"));
    }

    #[test]
    fn test_list_objects_xml() {
        let objects = vec![ObjectMeta {
            key: "file.txt".into(),
            size_bytes: 100,
            content_hash: "abc123".into(),
            content_type: "text/plain".into(),
            storage_path: "bucket/ab/abc123".into(),
            last_modified: "2024-01-01T00:00:00Z".into(),
            custom_metadata: None,
        }];
        let xml = list_objects_xml("my-bucket", "", "/", &objects, &[], false, 1, 1000);
        assert!(xml.contains("<Key>file.txt</Key>"));
        assert!(xml.contains("<Size>100</Size>"));
        assert!(xml.contains("<Delimiter>/</Delimiter>"));
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(xml_escape("a<b>c&d"), "a&lt;b&gt;c&amp;d");
    }

    #[test]
    fn test_to_iso8601() {
        assert_eq!(
            to_iso8601("2026-03-01 18:17:25.687517+00"),
            "2026-03-01T18:17:25.000Z"
        );
        assert_eq!(
            to_iso8601("2024-01-15 09:30:00+00"),
            "2024-01-15T09:30:00.000Z"
        );
        // Already ISO format should pass through
        assert_eq!(to_iso8601("2024-01-01T00:00:00Z"), "2024-01-01T00:00:00Z");
    }
}
