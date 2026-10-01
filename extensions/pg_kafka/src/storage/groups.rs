//! Consumer-group coordinator storage (DB-backed)
//!
//! pg_kafka runs several protocol workers (SO_REUSEPORT), each in its own
//! PostgreSQL backend process, so group state cannot live in process memory:
//! two members of one group may be served by different workers. All
//! coordinator state therefore lives in `pgkafka.consumer_groups`,
//! `pgkafka.consumer_group_members` and `pgkafka.consumer_offsets`, and every
//! state transition is a single SQL statement (each bridge request runs in
//! its own transaction) so concurrent workers serialise on the row locks.
//!
//! State machine (mirrors the Kafka group coordinator, single broker):
//!
//! ```text
//! Empty ──join──▶ PreparingRebalance ──all joined──▶ CompletingRebalance ──leader SyncGroup──▶ Stable
//!   ▲                    ▲                                                                       │
//!   └── last member left ┴──────────────── member joined / left / expired ◀─────────────────────┘
//! ```
//!
//! A rebalance bumps `generation_id`; members record the generation they last
//! joined, and a member whose generation differs from the group's is told
//! `REBALANCE_IN_PROGRESS` on its next heartbeat so it re-joins.

use super::spi_bridge::{ColumnType, SpiParam};
use super::spi_client::{SpiStorageClient, StorageError};

/// Group state names as stored in `pgkafka.consumer_groups.state`.
pub const STATE_EMPTY: &str = "Empty";
pub const STATE_PREPARING: &str = "PreparingRebalance";
pub const STATE_COMPLETING: &str = "CompletingRebalance";
pub const STATE_STABLE: &str = "Stable";
/// Reported for groups that do not exist (Kafka semantics).
pub const STATE_DEAD: &str = "Dead";

/// A consumer group row.
#[derive(Debug, Clone)]
pub struct GroupInfo {
    /// Internal row id (FK target for members/offsets)
    pub id: i32,
    /// Kafka group id
    pub group_id: String,
    pub state: String,
    pub generation_id: i32,
    /// Selected partition-assignment protocol (e.g. `range`), once known
    pub protocol: Option<String>,
    /// Protocol type (e.g. `consumer`)
    pub protocol_type: Option<String>,
    pub leader_id: Option<String>,
    /// Milliseconds since the current rebalance began (0 if none)
    pub rebalance_elapsed_ms: i64,
}

/// A group member row.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    #[allow(dead_code)]
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    /// Generation this member last joined (`None` = not yet joined the current one)
    pub generation_id: Option<i32>,
    /// Supported protocols, in the member's preference order: (name, metadata)
    pub protocols: Vec<(String, Vec<u8>)>,
    pub assignment: Option<Vec<u8>>,
    /// Generation the stored assignment belongs to
    pub assignment_generation: Option<i32>,
}

impl MemberInfo {
    /// Metadata bytes for the given protocol name, if the member supports it.
    pub fn metadata_for(&self, protocol: &str) -> Option<&[u8]> {
        self.protocols
            .iter()
            .find(|(name, _)| name == protocol)
            .map(|(_, meta)| meta.as_slice())
    }
}

/// A committed offset row.
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}

/// Outcome of a `CreateTopics` request for one topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateTopicOutcome {
    Created,
    AlreadyExists,
}

/// Encode member protocols as JSON for the `protocols` JSONB column.
pub fn protocols_to_json(protocols: &[(String, Vec<u8>)]) -> serde_json::Value {
    serde_json::Value::Array(
        protocols
            .iter()
            .map(|(name, meta)| {
                serde_json::json!({
                    "name": name,
                    "metadata": hex::encode(meta),
                })
            })
            .collect(),
    )
}

/// Decode member protocols from the `protocols` JSONB column.
pub fn protocols_from_json(value: Option<serde_json::Value>) -> Vec<(String, Vec<u8>)> {
    let Some(serde_json::Value::Array(items)) = value else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let name = item.get("name")?.as_str()?.to_string();
            let meta = item
                .get("metadata")
                .and_then(|m| m.as_str())
                .and_then(|h| hex::decode(h).ok())
                .unwrap_or_default();
            Some((name, meta))
        })
        .collect()
}

/// Pick the assignment protocol: the first of the leader's protocols that
/// every member supports. Returns `None` if there is no common protocol.
pub fn select_protocol(leader: &MemberInfo, members: &[MemberInfo]) -> Option<String> {
    leader
        .protocols
        .iter()
        .map(|(name, _)| name)
        .find(|candidate| {
            members
                .iter()
                .all(|m| m.protocols.iter().any(|(n, _)| n == *candidate))
        })
        .cloned()
}

const GROUP_COLUMNS: &str =
    "id, group_id, state, generation_id, protocol, protocol_type, leader_id, \
     COALESCE((EXTRACT(EPOCH FROM (now() - rebalance_started_at)) * 1000)::bigint, 0)";

fn group_column_types() -> Vec<ColumnType> {
    vec![
        ColumnType::Int32,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Int32,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Int64,
    ]
}

fn group_from_row(row: &super::spi_bridge::SpiRow) -> GroupInfo {
    GroupInfo {
        id: row.get_i32(0).unwrap_or(0),
        group_id: row.get_string(1).unwrap_or_default(),
        state: row.get_string(2).unwrap_or_else(|| STATE_EMPTY.to_string()),
        generation_id: row.get_i32(3).unwrap_or(0),
        protocol: row.get_string(4),
        protocol_type: row.get_string(5),
        leader_id: row.get_string(6),
        rebalance_elapsed_ms: row.get_i64(7).unwrap_or(0),
    }
}

const MEMBER_COLUMNS: &str =
    "member_id, client_id, client_host, session_timeout_ms, rebalance_timeout_ms, \
     generation_id, protocols, assignment, assignment_generation";

fn member_column_types() -> Vec<ColumnType> {
    vec![
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Text,
        ColumnType::Int32,
        ColumnType::Int32,
        ColumnType::Int32,
        ColumnType::Json,
        ColumnType::Bytea,
        ColumnType::Int32,
    ]
}

fn member_from_row(row: &super::spi_bridge::SpiRow) -> MemberInfo {
    MemberInfo {
        member_id: row.get_string(0).unwrap_or_default(),
        client_id: row.get_string(1).unwrap_or_default(),
        client_host: row.get_string(2).unwrap_or_default(),
        session_timeout_ms: row.get_i32(3).unwrap_or(30_000),
        rebalance_timeout_ms: row.get_i32(4).unwrap_or(60_000),
        generation_id: row.get_i32(5).ok(),
        protocols: protocols_from_json(row.get_json(6)),
        assignment: row.get_bytes(7),
        assignment_generation: row.get_i32(8).ok(),
    }
}

fn text(s: &str) -> SpiParam {
    SpiParam::Text(Some(s.to_string()))
}

impl SpiStorageClient {
    // ── Groups ─────────────────────────────────────────────────────────

    /// Create the group row if it does not exist and return its internal id.
    pub async fn ensure_group(
        &self,
        group_id: &str,
        protocol_type: Option<&str>,
    ) -> Result<i32, StorageError> {
        let result = self
            .bridge
            .query(
                "INSERT INTO pgkafka.consumer_groups (group_id, protocol_type)
                 VALUES ($1, $2)
                 ON CONFLICT (group_id) DO UPDATE
                   SET protocol_type = COALESCE(pgkafka.consumer_groups.protocol_type, EXCLUDED.protocol_type)
                 RETURNING id",
                vec![text(group_id), SpiParam::Text(protocol_type.map(String::from))],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(result
            .first()
            .ok_or(super::spi_bridge::SpiError::NoRows)?
            .get_i32(0)?)
    }

    /// Look up a group by Kafka group id.
    pub async fn get_group(&self, group_id: &str) -> Result<Option<GroupInfo>, StorageError> {
        let result = self
            .bridge
            .query(
                &format!(
                    "SELECT {} FROM pgkafka.consumer_groups WHERE group_id = $1",
                    GROUP_COLUMNS
                ),
                vec![text(group_id)],
                group_column_types(),
            )
            .await?;
        Ok(result.first().map(group_from_row))
    }

    /// All groups, ordered by group id.
    pub async fn list_groups(&self) -> Result<Vec<GroupInfo>, StorageError> {
        let result = self
            .bridge
            .query(
                &format!(
                    "SELECT {} FROM pgkafka.consumer_groups ORDER BY group_id",
                    GROUP_COLUMNS
                ),
                vec![],
                group_column_types(),
            )
            .await?;
        Ok(result.rows.iter().map(group_from_row).collect())
    }

    // ── Members ────────────────────────────────────────────────────────

    /// All members of a group, in join order.
    pub async fn get_members(&self, gid: i32) -> Result<Vec<MemberInfo>, StorageError> {
        let result = self
            .bridge
            .query(
                &format!(
                    "SELECT {} FROM pgkafka.consumer_group_members WHERE group_id = $1 ORDER BY joined_at, id",
                    MEMBER_COLUMNS
                ),
                vec![SpiParam::Int4(Some(gid))],
                member_column_types(),
            )
            .await?;
        Ok(result.rows.iter().map(member_from_row).collect())
    }

    /// One member of a group.
    pub async fn get_member(
        &self,
        gid: i32,
        member_id: &str,
    ) -> Result<Option<MemberInfo>, StorageError> {
        let result = self
            .bridge
            .query(
                &format!(
                    "SELECT {} FROM pgkafka.consumer_group_members WHERE group_id = $1 AND member_id = $2",
                    MEMBER_COLUMNS
                ),
                vec![SpiParam::Int4(Some(gid)), text(member_id)],
                member_column_types(),
            )
            .await?;
        Ok(result.first().map(member_from_row))
    }

    /// Insert or refresh a member on JoinGroup. The member's joined
    /// generation is reset; [`mark_member_joined`](Self::mark_member_joined)
    /// records it once the group's generation is settled.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_member(
        &self,
        gid: i32,
        member_id: &str,
        client_id: &str,
        client_host: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
    ) -> Result<(), StorageError> {
        self.bridge
            .execute(
                "INSERT INTO pgkafka.consumer_group_members
                   (group_id, member_id, client_id, client_host, session_timeout_ms,
                    rebalance_timeout_ms, protocol_type, protocols, generation_id, last_heartbeat)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, now())
                 ON CONFLICT (group_id, member_id) DO UPDATE SET
                   client_id = EXCLUDED.client_id,
                   client_host = EXCLUDED.client_host,
                   session_timeout_ms = EXCLUDED.session_timeout_ms,
                   rebalance_timeout_ms = EXCLUDED.rebalance_timeout_ms,
                   protocol_type = EXCLUDED.protocol_type,
                   protocols = EXCLUDED.protocols,
                   generation_id = NULL,
                   last_heartbeat = now()",
                vec![
                    SpiParam::Int4(Some(gid)),
                    text(member_id),
                    text(client_id),
                    text(client_host),
                    SpiParam::Int4(Some(session_timeout_ms)),
                    SpiParam::Int4(Some(rebalance_timeout_ms)),
                    text(protocol_type),
                    SpiParam::Json(Some(protocols_to_json(protocols))),
                ],
            )
            .await?;
        Ok(())
    }

    /// Remove members whose session timed out. Returns how many were removed.
    pub async fn expire_members(&self, gid: i32) -> Result<usize, StorageError> {
        let result = self
            .bridge
            .query(
                "DELETE FROM pgkafka.consumer_group_members
                 WHERE group_id = $1
                   AND last_heartbeat < now() - (session_timeout_ms * interval '1 millisecond')
                 RETURNING id",
                vec![SpiParam::Int4(Some(gid))],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(result.len())
    }

    /// Remove the given members (LeaveGroup). Returns the ids actually removed.
    pub async fn remove_members(
        &self,
        gid: i32,
        member_ids: &[String],
    ) -> Result<Vec<String>, StorageError> {
        let mut removed = Vec::new();
        for member_id in member_ids {
            let result = self
                .bridge
                .query(
                    "DELETE FROM pgkafka.consumer_group_members
                     WHERE group_id = $1 AND member_id = $2
                     RETURNING member_id",
                    vec![SpiParam::Int4(Some(gid)), text(member_id)],
                    vec![ColumnType::Text],
                )
                .await?;
            if let Some(id) = result.first().and_then(|r| r.get_string(0)) {
                removed.push(id);
            }
        }
        Ok(removed)
    }

    /// Record a member's heartbeat.
    pub async fn touch_member(&self, gid: i32, member_id: &str) -> Result<(), StorageError> {
        self.bridge
            .execute(
                "UPDATE pgkafka.consumer_group_members SET last_heartbeat = now()
                 WHERE group_id = $1 AND member_id = $2",
                vec![SpiParam::Int4(Some(gid)), text(member_id)],
            )
            .await?;
        Ok(())
    }

    /// Record that a member has joined the group's *current* generation.
    pub async fn mark_member_joined(&self, gid: i32, member_id: &str) -> Result<(), StorageError> {
        self.bridge
            .execute(
                "UPDATE pgkafka.consumer_group_members m
                 SET generation_id = g.generation_id, last_heartbeat = now()
                 FROM pgkafka.consumer_groups g
                 WHERE g.id = m.group_id AND m.group_id = $1 AND m.member_id = $2",
                vec![SpiParam::Int4(Some(gid)), text(member_id)],
            )
            .await?;
        Ok(())
    }

    // ── Rebalance transitions ──────────────────────────────────────────

    /// Start a new generation unless one is already being prepared.
    /// Returns `true` if this call started it.
    pub async fn begin_rebalance(&self, gid: i32) -> Result<bool, StorageError> {
        let result = self
            .bridge
            .query(
                "UPDATE pgkafka.consumer_groups
                 SET state = 'PreparingRebalance',
                     generation_id = generation_id + 1,
                     rebalance_started_at = now(),
                     updated_at = now()
                 WHERE id = $1 AND state <> 'PreparingRebalance'
                 RETURNING generation_id",
                vec![SpiParam::Int4(Some(gid))],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(!result.is_empty())
    }

    /// Transition to `Empty` if the group has no members. Returns `true` if it did.
    pub async fn mark_empty_if_no_members(&self, gid: i32) -> Result<bool, StorageError> {
        let result = self
            .bridge
            .query(
                "UPDATE pgkafka.consumer_groups
                 SET state = 'Empty', leader_id = NULL, protocol = NULL, updated_at = now()
                 WHERE id = $1
                   AND NOT EXISTS (SELECT 1 FROM pgkafka.consumer_group_members WHERE group_id = $1)
                 RETURNING id",
                vec![SpiParam::Int4(Some(gid))],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(!result.is_empty())
    }

    /// Members that have not yet joined the group's current generation.
    pub async fn pending_join_count(&self, gid: i32) -> Result<i64, StorageError> {
        self.bridge
            .query_one_i64(
                "SELECT count(*)::bigint
                 FROM pgkafka.consumer_group_members m
                 JOIN pgkafka.consumer_groups g ON g.id = m.group_id
                 WHERE m.group_id = $1 AND m.generation_id IS DISTINCT FROM g.generation_id",
                vec![SpiParam::Int4(Some(gid))],
            )
            .await
            .map_err(Into::into)
    }

    /// Evict members that failed to re-join `generation` before the
    /// rebalance timeout. Returns how many were evicted.
    pub async fn evict_unjoined_members(
        &self,
        gid: i32,
        generation: i32,
    ) -> Result<usize, StorageError> {
        let result = self
            .bridge
            .query(
                "DELETE FROM pgkafka.consumer_group_members
                 WHERE group_id = $1 AND generation_id IS DISTINCT FROM $2
                 RETURNING id",
                vec![SpiParam::Int4(Some(gid)), SpiParam::Int4(Some(generation))],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(result.len())
    }

    /// Finish the join phase: elect the leader and protocol and move to
    /// `CompletingRebalance`. Compare-and-set on `(state, generation)` so
    /// exactly one worker wins when several members poll concurrently.
    pub async fn complete_rebalance(
        &self,
        gid: i32,
        generation: i32,
        leader_id: &str,
        protocol: Option<&str>,
    ) -> Result<bool, StorageError> {
        let result = self
            .bridge
            .query(
                "UPDATE pgkafka.consumer_groups
                 SET state = 'CompletingRebalance', leader_id = $3, protocol = $4, updated_at = now()
                 WHERE id = $1 AND generation_id = $2 AND state = 'PreparingRebalance'
                 RETURNING id",
                vec![
                    SpiParam::Int4(Some(gid)),
                    SpiParam::Int4(Some(generation)),
                    text(leader_id),
                    SpiParam::Text(protocol.map(String::from)),
                ],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(!result.is_empty())
    }

    /// Store the leader's assignments for `generation`.
    pub async fn store_assignments(
        &self,
        gid: i32,
        generation: i32,
        assignments: &[(String, Vec<u8>)],
    ) -> Result<(), StorageError> {
        for (member_id, assignment) in assignments {
            self.bridge
                .execute(
                    "UPDATE pgkafka.consumer_group_members
                     SET assignment = $3, assignment_generation = $4
                     WHERE group_id = $1 AND member_id = $2",
                    vec![
                        SpiParam::Int4(Some(gid)),
                        text(member_id),
                        SpiParam::Bytea(Some(assignment.clone())),
                        SpiParam::Int4(Some(generation)),
                    ],
                )
                .await?;
        }
        Ok(())
    }

    /// Move `CompletingRebalance` → `Stable` for `generation`.
    pub async fn mark_group_stable(&self, gid: i32, generation: i32) -> Result<bool, StorageError> {
        let result = self
            .bridge
            .query(
                "UPDATE pgkafka.consumer_groups
                 SET state = 'Stable', updated_at = now()
                 WHERE id = $1 AND generation_id = $2 AND state = 'CompletingRebalance'
                 RETURNING id",
                vec![SpiParam::Int4(Some(gid)), SpiParam::Int4(Some(generation))],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(!result.is_empty())
    }

    // ── Offsets ────────────────────────────────────────────────────────

    /// Upsert a committed offset. Returns `false` if the topic is unknown.
    pub async fn commit_offset(
        &self,
        gid: i32,
        topic_name: &str,
        partition: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> Result<bool, StorageError> {
        let result = self
            .bridge
            .query(
                "INSERT INTO pgkafka.consumer_offsets (group_id, topic_id, partition, committed_offset, metadata, committed_at)
                 SELECT $1, t.id, $3, $4, $5, now() FROM pgkafka.topics t WHERE t.name = $2
                 ON CONFLICT (group_id, topic_id, partition) DO UPDATE SET
                   committed_offset = EXCLUDED.committed_offset,
                   metadata = EXCLUDED.metadata,
                   committed_at = now()
                 RETURNING topic_id",
                vec![
                    SpiParam::Int4(Some(gid)),
                    text(topic_name),
                    SpiParam::Int4(Some(partition)),
                    SpiParam::Int8(Some(offset)),
                    SpiParam::Text(metadata.map(String::from)),
                ],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(!result.is_empty())
    }

    /// Committed offsets for a group. With `topic = None` returns all of them.
    pub async fn fetch_offsets(
        &self,
        gid: i32,
        topic: Option<&str>,
    ) -> Result<Vec<CommittedOffset>, StorageError> {
        let (sql, params) = match topic {
            Some(name) => (
                "SELECT t.name, o.partition, o.committed_offset, o.metadata
                 FROM pgkafka.consumer_offsets o
                 JOIN pgkafka.topics t ON t.id = o.topic_id
                 WHERE o.group_id = $1 AND t.name = $2
                 ORDER BY o.partition",
                vec![SpiParam::Int4(Some(gid)), text(name)],
            ),
            None => (
                "SELECT t.name, o.partition, o.committed_offset, o.metadata
                 FROM pgkafka.consumer_offsets o
                 JOIN pgkafka.topics t ON t.id = o.topic_id
                 WHERE o.group_id = $1
                 ORDER BY t.name, o.partition",
                vec![SpiParam::Int4(Some(gid))],
            ),
        };
        let result = self
            .bridge
            .query(
                sql,
                params,
                vec![
                    ColumnType::Text,
                    ColumnType::Int32,
                    ColumnType::Int64,
                    ColumnType::Text,
                ],
            )
            .await?;
        Ok(result
            .rows
            .iter()
            .map(|r| CommittedOffset {
                topic: r.get_string(0).unwrap_or_default(),
                partition: r.get_i32(1).unwrap_or(0),
                offset: r.get_i64(2).unwrap_or(-1),
                metadata: r.get_string(3),
            })
            .collect())
    }

    // ── Topic administration ───────────────────────────────────────────

    /// Create a native (message-table backed) topic.
    pub async fn create_native_topic(
        &self,
        name: &str,
    ) -> Result<CreateTopicOutcome, StorageError> {
        let result = self
            .bridge
            .query(
                "INSERT INTO pgkafka.topics (name) VALUES ($1)
                 ON CONFLICT (name) DO NOTHING
                 RETURNING id",
                vec![text(name)],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(if result.is_empty() {
            CreateTopicOutcome::AlreadyExists
        } else {
            CreateTopicOutcome::Created
        })
    }

    /// Delete a topic (and, via cascade, its messages and offsets).
    /// Returns `false` if it did not exist.
    pub async fn delete_topic(&self, name: &str) -> Result<bool, StorageError> {
        let result = self
            .bridge
            .query(
                "DELETE FROM pgkafka.topics WHERE name = $1 RETURNING id",
                vec![text(name)],
                vec![ColumnType::Int32],
            )
            .await?;
        Ok(!result.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str, protocols: &[&str]) -> MemberInfo {
        MemberInfo {
            member_id: id.to_string(),
            client_id: String::new(),
            client_host: String::new(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            generation_id: None,
            protocols: protocols
                .iter()
                .map(|p| (p.to_string(), format!("meta-{}", p).into_bytes()))
                .collect(),
            assignment: None,
            assignment_generation: None,
        }
    }

    #[test]
    fn test_protocols_json_roundtrip() {
        let protocols = vec![
            ("range".to_string(), vec![0u8, 1, 2]),
            ("roundrobin".to_string(), vec![]),
        ];
        let json = protocols_to_json(&protocols);
        assert_eq!(json[0]["name"], "range");
        assert_eq!(json[0]["metadata"], "000102");
        assert_eq!(protocols_from_json(Some(json)), protocols);
        assert!(protocols_from_json(None).is_empty());
        assert!(protocols_from_json(Some(serde_json::json!("garbage"))).is_empty());
    }

    #[test]
    fn test_select_protocol_prefers_leader_order() {
        let leader = member("a", &["roundrobin", "range"]);
        let members = vec![leader.clone(), member("b", &["range", "roundrobin"])];
        assert_eq!(
            select_protocol(&leader, &members).as_deref(),
            Some("roundrobin")
        );
    }

    #[test]
    fn test_select_protocol_requires_common_support() {
        let leader = member("a", &["roundrobin", "range"]);
        let members = vec![leader.clone(), member("b", &["range"])];
        assert_eq!(select_protocol(&leader, &members).as_deref(), Some("range"));
        let members = vec![leader.clone(), member("c", &["sticky"])];
        assert_eq!(select_protocol(&leader, &members), None);
    }

    #[test]
    fn test_metadata_for() {
        let m = member("a", &["range"]);
        assert_eq!(m.metadata_for("range"), Some(&b"meta-range"[..]));
        assert_eq!(m.metadata_for("sticky"), None);
    }
}
