//! Kafka protocol primitive types and constants

#![allow(dead_code)]

/// Kafka API keys we support
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    ApiVersions = 18,
}

impl ApiKey {
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(ApiKey::Produce),
            1 => Some(ApiKey::Fetch),
            2 => Some(ApiKey::ListOffsets),
            3 => Some(ApiKey::Metadata),
            8 => Some(ApiKey::OffsetCommit),
            9 => Some(ApiKey::OffsetFetch),
            10 => Some(ApiKey::FindCoordinator),
            11 => Some(ApiKey::JoinGroup),
            12 => Some(ApiKey::Heartbeat),
            13 => Some(ApiKey::LeaveGroup),
            14 => Some(ApiKey::SyncGroup),
            18 => Some(ApiKey::ApiVersions),
            _ => None,
        }
    }
}

/// Kafka error codes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    UnknownTopicOrPartition = 3,
    InvalidRequest = 42,
    UnsupportedVersion = 35,
}

/// Request header (common to all requests)
#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

/// Response header (common to all responses)
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

/// API version range for a single API
#[derive(Debug, Clone)]
pub struct ApiVersionRange {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// Supported API versions (what we tell clients we support)
pub fn supported_api_versions() -> Vec<ApiVersionRange> {
    vec![
        ApiVersionRange {
            api_key: 0,
            min_version: 0,
            max_version: 8,
        }, // Produce (v9+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 1,
            min_version: 0,
            max_version: 11,
        }, // Fetch (v12+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 2,
            min_version: 0,
            max_version: 5,
        }, // ListOffsets (v6+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 3,
            min_version: 0,
            max_version: 8,
        }, // Metadata (v9+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 8,
            min_version: 0,
            max_version: 7,
        }, // OffsetCommit (v8+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 9,
            min_version: 0,
            max_version: 5,
        }, // OffsetFetch (v6+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 10,
            min_version: 0,
            max_version: 2,
        }, // FindCoordinator (v3+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 11,
            min_version: 0,
            max_version: 5,
        }, // JoinGroup (v6+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 12,
            min_version: 0,
            max_version: 3,
        }, // Heartbeat (v4+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 13,
            min_version: 0,
            max_version: 3,
        }, // LeaveGroup (v4+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 14,
            min_version: 0,
            max_version: 3,
        }, // SyncGroup (v4+ requires flexible encoding which we don't support yet)
        ApiVersionRange {
            api_key: 18,
            min_version: 0,
            max_version: 3,
        }, // ApiVersions
    ]
}
