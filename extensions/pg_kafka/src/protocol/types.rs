//! Kafka protocol primitive types and constants

#![allow(dead_code)]

/// Kafka API keys we support
///
/// **Invariant:** every variant here is (a) advertised by
/// [`supported_api_versions`] and (b) handled by `handle_request`. Advertising
/// an API that has no handler steers clients into a broken path (see #126),
/// so the dispatcher matches this enum exhaustively and a unit test checks
/// the advertised list against it.
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
    DescribeGroups = 15,
    ListGroups = 16,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
}

impl ApiKey {
    /// All supported API keys.
    pub const ALL: &'static [ApiKey] = &[
        ApiKey::Produce,
        ApiKey::Fetch,
        ApiKey::ListOffsets,
        ApiKey::Metadata,
        ApiKey::OffsetCommit,
        ApiKey::OffsetFetch,
        ApiKey::FindCoordinator,
        ApiKey::JoinGroup,
        ApiKey::Heartbeat,
        ApiKey::LeaveGroup,
        ApiKey::SyncGroup,
        ApiKey::DescribeGroups,
        ApiKey::ListGroups,
        ApiKey::ApiVersions,
        ApiKey::CreateTopics,
        ApiKey::DeleteTopics,
    ];

    pub fn from_i16(value: i16) -> Option<Self> {
        ApiKey::ALL.iter().copied().find(|k| *k as i16 == value)
    }

    /// Version range `(min, max)` this broker implements for the API.
    ///
    /// Flexible (compact) encodings are not supported yet, so each max is the
    /// last non-flexible version (except ApiVersions, whose v3 is handled).
    pub fn version_range(self) -> (i16, i16) {
        match self {
            ApiKey::Produce => (0, 8),
            ApiKey::Fetch => (0, 11),
            ApiKey::ListOffsets => (0, 5),
            ApiKey::Metadata => (0, 8),
            ApiKey::OffsetCommit => (0, 7),
            ApiKey::OffsetFetch => (0, 5),
            ApiKey::FindCoordinator => (0, 2),
            ApiKey::JoinGroup => (0, 5),
            ApiKey::Heartbeat => (0, 3),
            ApiKey::LeaveGroup => (0, 3),
            ApiKey::SyncGroup => (0, 3),
            ApiKey::DescribeGroups => (0, 4),
            ApiKey::ListGroups => (0, 2),
            ApiKey::ApiVersions => (0, 3),
            ApiKey::CreateTopics => (0, 4),
            ApiKey::DeleteTopics => (0, 3),
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
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopic = 17,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidRequest = 42,
    GroupIdNotFound = 69,
    MemberIdRequired = 79,
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
///
/// Derived from [`ApiKey::ALL`] so the advertised set can never drift from
/// the set of implemented handlers.
pub fn supported_api_versions() -> Vec<ApiVersionRange> {
    ApiKey::ALL
        .iter()
        .map(|k| {
            let (min_version, max_version) = k.version_range();
            ApiVersionRange {
                api_key: *k as i16,
                min_version,
                max_version,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_i16_roundtrip() {
        for key in ApiKey::ALL {
            assert_eq!(ApiKey::from_i16(*key as i16), Some(*key));
        }
        assert_eq!(ApiKey::from_i16(4), None); // LeaderAndIsr — not supported
        assert_eq!(ApiKey::from_i16(-1), None);
    }

    /// Regression for #126: every advertised API must map to a handled
    /// `ApiKey`, and every handled `ApiKey` must be advertised exactly once.
    #[test]
    fn test_advertised_apis_match_handled_apis() {
        let advertised = supported_api_versions();
        assert_eq!(advertised.len(), ApiKey::ALL.len());
        for range in &advertised {
            let key = ApiKey::from_i16(range.api_key)
                .unwrap_or_else(|| panic!("advertised api {} has no handler", range.api_key));
            assert_eq!(key.version_range(), (range.min_version, range.max_version));
            assert!(range.min_version <= range.max_version);
        }
    }

    #[test]
    fn test_group_and_admin_apis_are_advertised() {
        let keys: Vec<i16> = supported_api_versions().iter().map(|r| r.api_key).collect();
        for expected in [8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20] {
            assert!(keys.contains(&expected), "api {} not advertised", expected);
        }
    }
}
