//! Consumer-group APIs: JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
//! OffsetCommit, OffsetFetch, ListGroups, DescribeGroups.
//!
//! Wire parsing/encoding is kept in pure functions (`parse_*` / `encode_*`)
//! so every supported version is unit-testable without a database; the
//! `handle_*` functions glue them to the DB-backed coordinator in
//! `storage::groups`.
//!
//! Only non-flexible versions are implemented (see
//! [`ApiKey::version_range`](super::types::ApiKey::version_range)).

use bytes::BytesMut;
use pg_observability::{info, warn};
use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::codec::*;
use super::types::*;
use crate::storage::groups::{
    select_protocol, GroupInfo, MemberInfo, STATE_COMPLETING, STATE_DEAD, STATE_EMPTY,
    STATE_PREPARING, STATE_STABLE,
};
use crate::storage::{SpiStorageClient, StorageError};

/// Poll interval while a member waits for a rebalance to settle.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Upper bound on how long a JoinGroup/SyncGroup may block, whatever the
/// client asked for.
const MAX_WAIT_MS: i32 = 300_000;
/// Default rebalance timeout for JoinGroup v0 (which has no such field).
const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 60_000;

/// Map a storage failure to the error code clients retry on.
fn storage_error_code(context: &str, e: StorageError) -> ErrorCode {
    warn!(error = %e, "{} failed", context);
    ErrorCode::CoordinatorNotAvailable
}

fn wait_budget(requested_ms: i32) -> Duration {
    let ms = if requested_ms <= 0 {
        DEFAULT_REBALANCE_TIMEOUT_MS
    } else {
        requested_ms.min(MAX_WAIT_MS)
    };
    Duration::from_millis(ms as u64)
}

/// Generate a member id: `<client_id>-<unique suffix>` like Kafka does.
fn generate_member_id(client_id: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{}-{:x}-{:x}-{:x}",
        if client_id.is_empty() {
            "consumer"
        } else {
            client_id
        },
        std::process::id(),
        nanos,
        n
    )
}

// ═══════════════════════════════════════════════════════════════════════
// JoinGroup (11)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    /// (name, metadata) in preference order
    pub protocols: Vec<(String, Vec<u8>)>,
}

pub fn parse_join_group_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<JoinGroupRequest, ProtocolError> {
    let group_id = read_string(body)?;
    let session_timeout_ms = read_i32(body)?;
    let rebalance_timeout_ms = if version >= 1 {
        read_i32(body)?
    } else {
        session_timeout_ms
    };
    let member_id = read_string(body)?;
    let group_instance_id = if version >= 5 {
        read_nullable_string(body)?
    } else {
        None
    };
    let protocol_type = read_string(body)?;
    let n = read_array_len(body)?;
    let mut protocols = Vec::with_capacity(n);
    for _ in 0..n {
        let name = read_string(body)?;
        let metadata = read_bytes(body)?;
        protocols.push((name, metadata.to_vec()));
    }
    Ok(JoinGroupRequest {
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id,
        group_instance_id,
        protocol_type,
        protocols,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupResponse {
    pub error_code: ErrorCode,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    /// (member_id, metadata) — populated for the leader only
    pub members: Vec<(String, Vec<u8>)>,
}

impl JoinGroupResponse {
    fn error(code: ErrorCode, member_id: &str) -> Self {
        Self {
            error_code: code,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: member_id.to_string(),
            members: Vec::new(),
        }
    }
}

pub fn encode_join_group_response(version: i16, resp: &JoinGroupResponse) -> BytesMut {
    let mut buf = BytesMut::with_capacity(128);
    if version >= 2 {
        write_i32(&mut buf, 0); // throttle_time_ms
    }
    write_i16(&mut buf, resp.error_code as i16);
    write_i32(&mut buf, resp.generation_id);
    write_string(&mut buf, &resp.protocol_name);
    write_string(&mut buf, &resp.leader);
    write_string(&mut buf, &resp.member_id);
    write_i32(&mut buf, resp.members.len() as i32);
    for (member_id, metadata) in &resp.members {
        write_string(&mut buf, member_id);
        if version >= 5 {
            write_nullable_string(&mut buf, None); // group_instance_id
        }
        write_bytes(&mut buf, metadata);
    }
    buf
}

/// Run the JoinGroup coordinator flow for one member.
async fn join_group(
    storage: &SpiStorageClient,
    req: &JoinGroupRequest,
    client_id: &str,
    client_host: &str,
) -> JoinGroupResponse {
    if req.group_id.is_empty() {
        return JoinGroupResponse::error(ErrorCode::InvalidGroupId, &req.member_id);
    }
    if req.session_timeout_ms <= 0 {
        return JoinGroupResponse::error(ErrorCode::InvalidSessionTimeout, &req.member_id);
    }
    let member_id = if req.member_id.is_empty() {
        generate_member_id(client_id)
    } else {
        req.member_id.clone()
    };

    let gid = match storage
        .ensure_group(&req.group_id, Some(&req.protocol_type))
        .await
    {
        Ok(id) => id,
        Err(e) => {
            return JoinGroupResponse::error(storage_error_code("ensure_group", e), &member_id)
        }
    };

    if let Err(e) = reap_expired_members(storage, gid).await {
        return JoinGroupResponse::error(storage_error_code("expire_members", e), &member_id);
    }

    // Fast path: a known, non-leader member re-joining a Stable group with
    // unchanged protocols keeps the current generation (no rebalance).
    let group = match storage.get_group(&req.group_id).await {
        Ok(Some(g)) => g,
        Ok(None) => return JoinGroupResponse::error(ErrorCode::UnknownMemberId, &member_id),
        Err(e) => return JoinGroupResponse::error(storage_error_code("get_group", e), &member_id),
    };
    let existing = match storage.get_member(gid, &member_id).await {
        Ok(m) => m,
        Err(e) => return JoinGroupResponse::error(storage_error_code("get_member", e), &member_id),
    };
    if group.state == STATE_STABLE {
        if let Some(m) = &existing {
            let unchanged = m.generation_id == Some(group.generation_id)
                && m.protocols == req.protocols
                && group.leader_id.as_deref() != Some(member_id.as_str());
            if unchanged {
                let _ = storage.touch_member(gid, &member_id).await;
                return join_success(storage, gid, &group, &member_id).await;
            }
        }
    }

    // A new member must share at least one protocol with the current members.
    if existing.is_none() {
        match storage.get_members(gid).await {
            Ok(members) if !members.is_empty() => {
                let candidate = MemberInfo {
                    member_id: member_id.clone(),
                    client_id: client_id.to_string(),
                    client_host: client_host.to_string(),
                    session_timeout_ms: req.session_timeout_ms,
                    rebalance_timeout_ms: req.rebalance_timeout_ms,
                    generation_id: None,
                    protocols: req.protocols.clone(),
                    assignment: None,
                    assignment_generation: None,
                };
                if select_protocol(&candidate, &members).is_none() {
                    return JoinGroupResponse::error(
                        ErrorCode::InconsistentGroupProtocol,
                        &member_id,
                    );
                }
            }
            Ok(_) => {}
            Err(e) => {
                return JoinGroupResponse::error(storage_error_code("get_members", e), &member_id)
            }
        }
    }

    if let Err(e) = storage
        .upsert_member(
            gid,
            &member_id,
            client_id,
            client_host,
            req.session_timeout_ms,
            req.rebalance_timeout_ms,
            &req.protocol_type,
            &req.protocols,
        )
        .await
    {
        return JoinGroupResponse::error(storage_error_code("upsert_member", e), &member_id);
    }
    if let Err(e) = storage.begin_rebalance(gid).await {
        return JoinGroupResponse::error(storage_error_code("begin_rebalance", e), &member_id);
    }
    if let Err(e) = storage.mark_member_joined(gid, &member_id).await {
        return JoinGroupResponse::error(storage_error_code("mark_member_joined", e), &member_id);
    }
    info!(group = %req.group_id, member = %member_id, "JoinGroup: rebalance started");

    let deadline = Instant::now() + wait_budget(req.rebalance_timeout_ms);
    loop {
        let group = match storage.get_group(&req.group_id).await {
            Ok(Some(g)) => g,
            Ok(None) => return JoinGroupResponse::error(ErrorCode::UnknownMemberId, &member_id),
            Err(e) => {
                return JoinGroupResponse::error(storage_error_code("get_group", e), &member_id)
            }
        };
        let me = match storage.get_member(gid, &member_id).await {
            Ok(Some(m)) => m,
            // Evicted (rebalance timeout) — the client must re-join.
            Ok(None) => return JoinGroupResponse::error(ErrorCode::UnknownMemberId, &member_id),
            Err(e) => {
                return JoinGroupResponse::error(storage_error_code("get_member", e), &member_id)
            }
        };
        let joined_current = me.generation_id == Some(group.generation_id);

        match group.state.as_str() {
            STATE_PREPARING => {
                if !joined_current {
                    // A newer generation started while we waited: join it too.
                    let _ = storage.mark_member_joined(gid, &member_id).await;
                } else if let Err(e) = try_complete_join_phase(storage, gid, &group).await {
                    return JoinGroupResponse::error(
                        storage_error_code("complete_rebalance", e),
                        &member_id,
                    );
                }
            }
            STATE_COMPLETING | STATE_STABLE if joined_current => {
                return join_success(storage, gid, &group, &member_id).await;
            }
            _ => {
                // Empty, or the group settled a generation we are not part
                // of: start another rebalance that includes us.
                let _ = storage.begin_rebalance(gid).await;
                let _ = storage.mark_member_joined(gid, &member_id).await;
            }
        }

        if Instant::now() >= deadline {
            return JoinGroupResponse::error(ErrorCode::RebalanceInProgress, &member_id);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Drop timed-out members and, if that changed membership, kick off a
/// rebalance (or mark the group Empty).
async fn reap_expired_members(storage: &SpiStorageClient, gid: i32) -> Result<(), StorageError> {
    if storage.expire_members(gid).await? > 0 && !storage.mark_empty_if_no_members(gid).await? {
        storage.begin_rebalance(gid).await?;
    }
    Ok(())
}

/// If every live member has joined the current generation (or the rebalance
/// timeout has passed), elect leader + protocol and move to
/// CompletingRebalance. Safe to call from several workers concurrently.
async fn try_complete_join_phase(
    storage: &SpiStorageClient,
    gid: i32,
    group: &GroupInfo,
) -> Result<(), StorageError> {
    let pending = storage.pending_join_count(gid).await?;
    if pending > 0 {
        let members = storage.get_members(gid).await?;
        let timeout = members
            .iter()
            .map(|m| m.rebalance_timeout_ms.max(0) as i64)
            .max()
            .unwrap_or(DEFAULT_REBALANCE_TIMEOUT_MS as i64);
        if group.rebalance_elapsed_ms < timeout {
            return Ok(());
        }
        let evicted = storage
            .evict_unjoined_members(gid, group.generation_id)
            .await?;
        if evicted > 0 {
            warn!(group = %group.group_id, evicted, "evicted members that missed the rebalance");
        }
    }

    let members = storage.get_members(gid).await?;
    let Some(leader) = members
        .iter()
        .find(|m| Some(m.member_id.as_str()) == group.leader_id.as_deref())
        .or_else(|| members.first())
    else {
        storage.mark_empty_if_no_members(gid).await?;
        return Ok(());
    };
    let protocol = select_protocol(leader, &members);
    storage
        .complete_rebalance(
            gid,
            group.generation_id,
            &leader.member_id,
            protocol.as_deref(),
        )
        .await?;
    Ok(())
}

/// Build the successful JoinGroup response for `member_id`.
async fn join_success(
    storage: &SpiStorageClient,
    gid: i32,
    group: &GroupInfo,
    member_id: &str,
) -> JoinGroupResponse {
    let leader = group.leader_id.clone().unwrap_or_default();
    let protocol = group.protocol.clone().unwrap_or_default();
    let members = if leader == member_id {
        match storage.get_members(gid).await {
            Ok(ms) => ms
                .into_iter()
                .filter(|m| m.generation_id == Some(group.generation_id))
                .map(|m| {
                    let meta = m.metadata_for(&protocol).map(<[u8]>::to_vec);
                    (m.member_id, meta.unwrap_or_default())
                })
                .collect(),
            Err(e) => {
                return JoinGroupResponse::error(storage_error_code("get_members", e), member_id)
            }
        }
    } else {
        Vec::new()
    };
    JoinGroupResponse {
        error_code: ErrorCode::None,
        generation_id: group.generation_id,
        protocol_name: protocol,
        leader,
        member_id: member_id.to_string(),
        members,
    }
}

pub async fn handle_join_group(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    peer_host: &str,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_join_group_request(header.api_version, body)?;
    let client_id = header.client_id.as_deref().unwrap_or_default();
    let resp = join_group(storage, &req, client_id, peer_host).await;
    Ok(encode_join_group_response(header.api_version, &resp))
}

// ═══════════════════════════════════════════════════════════════════════
// SyncGroup (14)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub assignments: Vec<(String, Vec<u8>)>,
}

pub fn parse_sync_group_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<SyncGroupRequest, ProtocolError> {
    let group_id = read_string(body)?;
    let generation_id = read_i32(body)?;
    let member_id = read_string(body)?;
    let group_instance_id = if version >= 3 {
        read_nullable_string(body)?
    } else {
        None
    };
    let n = read_array_len(body)?;
    let mut assignments = Vec::with_capacity(n);
    for _ in 0..n {
        let member = read_string(body)?;
        let assignment = read_bytes(body)?;
        assignments.push((member, assignment.to_vec()));
    }
    Ok(SyncGroupRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        assignments,
    })
}

pub fn encode_sync_group_response(version: i16, error: ErrorCode, assignment: &[u8]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32 + assignment.len());
    if version >= 1 {
        write_i32(&mut buf, 0);
    }
    write_i16(&mut buf, error as i16);
    write_bytes(&mut buf, assignment);
    buf
}

async fn sync_group(storage: &SpiStorageClient, req: &SyncGroupRequest) -> (ErrorCode, Vec<u8>) {
    let group = match storage.get_group(&req.group_id).await {
        Ok(Some(g)) => g,
        Ok(None) => return (ErrorCode::UnknownMemberId, Vec::new()),
        Err(e) => return (storage_error_code("get_group", e), Vec::new()),
    };
    let gid = group.id;
    let me = match storage.get_member(gid, &req.member_id).await {
        Ok(Some(m)) => m,
        Ok(None) => return (ErrorCode::UnknownMemberId, Vec::new()),
        Err(e) => return (storage_error_code("get_member", e), Vec::new()),
    };
    if group.generation_id != req.generation_id {
        return (ErrorCode::IllegalGeneration, Vec::new());
    }
    match group.state.as_str() {
        STATE_PREPARING => return (ErrorCode::RebalanceInProgress, Vec::new()),
        STATE_EMPTY | STATE_DEAD => return (ErrorCode::UnknownMemberId, Vec::new()),
        _ => {}
    }

    let is_leader = group.leader_id.as_deref() == Some(req.member_id.as_str());
    if is_leader && group.state == STATE_COMPLETING {
        // Members the leader left out get an empty assignment, as Kafka does.
        let mut assignments = req.assignments.clone();
        if let Ok(members) = storage.get_members(gid).await {
            for m in members {
                if !assignments.iter().any(|(id, _)| *id == m.member_id) {
                    assignments.push((m.member_id, Vec::new()));
                }
            }
        }
        if let Err(e) = storage
            .store_assignments(gid, req.generation_id, &assignments)
            .await
        {
            return (storage_error_code("store_assignments", e), Vec::new());
        }
        if let Err(e) = storage.mark_group_stable(gid, req.generation_id).await {
            return (storage_error_code("mark_group_stable", e), Vec::new());
        }
        info!(group = %req.group_id, generation = req.generation_id, "SyncGroup: group is Stable");
    }

    let deadline = Instant::now() + wait_budget(me.rebalance_timeout_ms);
    loop {
        let me = match storage.get_member(gid, &req.member_id).await {
            Ok(Some(m)) => m,
            Ok(None) => return (ErrorCode::UnknownMemberId, Vec::new()),
            Err(e) => return (storage_error_code("get_member", e), Vec::new()),
        };
        if me.assignment_generation == Some(req.generation_id) {
            let _ = storage.touch_member(gid, &req.member_id).await;
            return (ErrorCode::None, me.assignment.unwrap_or_default());
        }
        let group = match storage.get_group(&req.group_id).await {
            Ok(Some(g)) => g,
            Ok(None) => return (ErrorCode::UnknownMemberId, Vec::new()),
            Err(e) => return (storage_error_code("get_group", e), Vec::new()),
        };
        if group.generation_id != req.generation_id || group.state == STATE_PREPARING {
            return (ErrorCode::RebalanceInProgress, Vec::new());
        }
        if Instant::now() >= deadline {
            return (ErrorCode::RebalanceInProgress, Vec::new());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn handle_sync_group(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_sync_group_request(header.api_version, body)?;
    let (error, assignment) = sync_group(storage, &req).await;
    Ok(encode_sync_group_response(
        header.api_version,
        error,
        &assignment,
    ))
}

// ═══════════════════════════════════════════════════════════════════════
// Heartbeat (12)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

pub fn parse_heartbeat_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<HeartbeatRequest, ProtocolError> {
    let group_id = read_string(body)?;
    let generation_id = read_i32(body)?;
    let member_id = read_string(body)?;
    let group_instance_id = if version >= 3 {
        read_nullable_string(body)?
    } else {
        None
    };
    Ok(HeartbeatRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
    })
}

pub fn encode_heartbeat_response(version: i16, error: ErrorCode) -> BytesMut {
    let mut buf = BytesMut::with_capacity(8);
    if version >= 1 {
        write_i32(&mut buf, 0);
    }
    write_i16(&mut buf, error as i16);
    buf
}

async fn heartbeat(storage: &SpiStorageClient, req: &HeartbeatRequest) -> ErrorCode {
    let group = match storage.get_group(&req.group_id).await {
        Ok(Some(g)) => g,
        Ok(None) => return ErrorCode::UnknownMemberId,
        Err(e) => return storage_error_code("get_group", e),
    };
    let gid = group.id;
    if let Err(e) = reap_expired_members(storage, gid).await {
        return storage_error_code("expire_members", e);
    }
    match storage.get_member(gid, &req.member_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return ErrorCode::UnknownMemberId,
        Err(e) => return storage_error_code("get_member", e),
    }
    if let Err(e) = storage.touch_member(gid, &req.member_id).await {
        return storage_error_code("touch_member", e);
    }
    // Re-read: reaping may have started a rebalance.
    let group = match storage.get_group(&req.group_id).await {
        Ok(Some(g)) => g,
        Ok(None) => return ErrorCode::UnknownMemberId,
        Err(e) => return storage_error_code("get_group", e),
    };
    match group.state.as_str() {
        STATE_PREPARING => ErrorCode::RebalanceInProgress,
        STATE_EMPTY | STATE_DEAD => ErrorCode::UnknownMemberId,
        _ if group.generation_id != req.generation_id => ErrorCode::IllegalGeneration,
        _ => ErrorCode::None,
    }
}

pub async fn handle_heartbeat(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_heartbeat_request(header.api_version, body)?;
    let error = heartbeat(storage, &req).await;
    Ok(encode_heartbeat_response(header.api_version, error))
}

// ═══════════════════════════════════════════════════════════════════════
// LeaveGroup (13)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    /// (member_id, group_instance_id)
    pub members: Vec<(String, Option<String>)>,
}

pub fn parse_leave_group_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<LeaveGroupRequest, ProtocolError> {
    let group_id = read_string(body)?;
    let members = if version >= 3 {
        let n = read_array_len(body)?;
        let mut members = Vec::with_capacity(n);
        for _ in 0..n {
            let member_id = read_string(body)?;
            let instance = read_nullable_string(body)?;
            members.push((member_id, instance));
        }
        members
    } else {
        vec![(read_string(body)?, None)]
    };
    Ok(LeaveGroupRequest { group_id, members })
}

pub fn encode_leave_group_response(
    version: i16,
    error: ErrorCode,
    members: &[(String, Option<String>, ErrorCode)],
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32);
    if version >= 1 {
        write_i32(&mut buf, 0);
    }
    write_i16(&mut buf, error as i16);
    if version >= 3 {
        write_i32(&mut buf, members.len() as i32);
        for (member_id, instance, code) in members {
            write_string(&mut buf, member_id);
            write_nullable_string(&mut buf, instance.as_deref());
            write_i16(&mut buf, *code as i16);
        }
    }
    buf
}

async fn leave_group(
    storage: &SpiStorageClient,
    req: &LeaveGroupRequest,
) -> (ErrorCode, Vec<(String, Option<String>, ErrorCode)>) {
    let per_member = |code: ErrorCode| -> Vec<(String, Option<String>, ErrorCode)> {
        req.members
            .iter()
            .map(|(id, inst)| (id.clone(), inst.clone(), code))
            .collect()
    };
    let group = match storage.get_group(&req.group_id).await {
        Ok(Some(g)) => g,
        Ok(None) => {
            return (
                ErrorCode::UnknownMemberId,
                per_member(ErrorCode::UnknownMemberId),
            )
        }
        Err(e) => {
            let code = storage_error_code("get_group", e);
            return (code, per_member(code));
        }
    };
    let ids: Vec<String> = req.members.iter().map(|(id, _)| id.clone()).collect();
    let removed = match storage.remove_members(group.id, &ids).await {
        Ok(r) => r,
        Err(e) => {
            let code = storage_error_code("remove_members", e);
            return (code, per_member(code));
        }
    };
    if !removed.is_empty() {
        match storage.mark_empty_if_no_members(group.id).await {
            Ok(false) => {
                let _ = storage.begin_rebalance(group.id).await;
            }
            Ok(true) => {}
            Err(e) => {
                let code = storage_error_code("mark_empty", e);
                return (code, per_member(code));
            }
        }
        info!(group = %req.group_id, removed = removed.len(), "LeaveGroup");
    }
    let members: Vec<_> = req
        .members
        .iter()
        .map(|(id, inst)| {
            let code = if removed.contains(id) {
                ErrorCode::None
            } else {
                ErrorCode::UnknownMemberId
            };
            (id.clone(), inst.clone(), code)
        })
        .collect();
    // v0-2 carry a single member: surface its result at the top level.
    let top = members
        .iter()
        .map(|(_, _, c)| *c)
        .find(|c| *c != ErrorCode::None)
        .filter(|_| members.len() == 1)
        .unwrap_or(ErrorCode::None);
    (top, members)
}

pub async fn handle_leave_group(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_leave_group_request(header.api_version, body)?;
    let (error, members) = leave_group(storage, &req).await;
    Ok(encode_leave_group_response(
        header.api_version,
        error,
        &members,
    ))
}

// ═══════════════════════════════════════════════════════════════════════
// OffsetCommit (8)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub committed_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub topics: Vec<(String, Vec<OffsetCommitPartition>)>,
}

pub fn parse_offset_commit_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<OffsetCommitRequest, ProtocolError> {
    let group_id = read_string(body)?;
    let (generation_id, member_id) = if version >= 1 {
        (read_i32(body)?, read_string(body)?)
    } else {
        (-1, String::new())
    };
    let group_instance_id = if version >= 7 {
        read_nullable_string(body)?
    } else {
        None
    };
    if (2..=4).contains(&version) {
        let _retention_time_ms = read_i64(body)?;
    }
    let n = read_array_len(body)?;
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = read_string(body)?;
        let np = read_array_len(body)?;
        let mut partitions = Vec::with_capacity(np);
        for _ in 0..np {
            let partition_index = read_i32(body)?;
            let committed_offset = read_i64(body)?;
            if version == 1 {
                let _commit_timestamp = read_i64(body)?;
            }
            if version >= 6 {
                let _committed_leader_epoch = read_i32(body)?;
            }
            let committed_metadata = read_nullable_string(body)?;
            partitions.push(OffsetCommitPartition {
                partition_index,
                committed_offset,
                committed_metadata,
            });
        }
        topics.push((name, partitions));
    }
    Ok(OffsetCommitRequest {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
        topics,
    })
}

pub fn encode_offset_commit_response(
    version: i16,
    topics: &[(String, Vec<(i32, ErrorCode)>)],
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    if version >= 3 {
        write_i32(&mut buf, 0);
    }
    write_i32(&mut buf, topics.len() as i32);
    for (name, partitions) in topics {
        write_string(&mut buf, name);
        write_i32(&mut buf, partitions.len() as i32);
        for (index, code) in partitions {
            write_i32(&mut buf, *index);
            write_i16(&mut buf, *code as i16);
        }
    }
    buf
}

async fn offset_commit(
    storage: &SpiStorageClient,
    req: &OffsetCommitRequest,
) -> Vec<(String, Vec<(i32, ErrorCode)>)> {
    let all = |code: ErrorCode| -> Vec<(String, Vec<(i32, ErrorCode)>)> {
        req.topics
            .iter()
            .map(|(name, parts)| {
                (
                    name.clone(),
                    parts.iter().map(|p| (p.partition_index, code)).collect(),
                )
            })
            .collect()
    };
    if req.group_id.is_empty() {
        return all(ErrorCode::InvalidGroupId);
    }
    let gid = match storage.ensure_group(&req.group_id, None).await {
        Ok(id) => id,
        Err(e) => return all(storage_error_code("ensure_group", e)),
    };

    // Commits from group members are validated against the group state;
    // commits with no member id (manual assignment) are accepted as-is.
    if !req.member_id.is_empty() {
        let group = match storage.get_group(&req.group_id).await {
            Ok(Some(g)) => g,
            Ok(None) => return all(ErrorCode::UnknownMemberId),
            Err(e) => return all(storage_error_code("get_group", e)),
        };
        match storage.get_member(gid, &req.member_id).await {
            Ok(Some(_)) => {}
            Ok(None) => return all(ErrorCode::UnknownMemberId),
            Err(e) => return all(storage_error_code("get_member", e)),
        }
        if req.generation_id >= 0 && group.generation_id != req.generation_id {
            return all(ErrorCode::IllegalGeneration);
        }
        if group.state == STATE_PREPARING {
            return all(ErrorCode::RebalanceInProgress);
        }
        let _ = storage.touch_member(gid, &req.member_id).await;
    }

    let mut out = Vec::with_capacity(req.topics.len());
    for (name, partitions) in &req.topics {
        let mut results = Vec::with_capacity(partitions.len());
        for p in partitions {
            let code = if p.partition_index != 0 {
                ErrorCode::UnknownTopicOrPartition
            } else {
                match storage
                    .commit_offset(
                        gid,
                        name,
                        p.partition_index,
                        p.committed_offset,
                        p.committed_metadata.as_deref(),
                    )
                    .await
                {
                    Ok(true) => ErrorCode::None,
                    Ok(false) => ErrorCode::UnknownTopicOrPartition,
                    Err(e) => storage_error_code("commit_offset", e),
                }
            };
            results.push((p.partition_index, code));
        }
        out.push((name.clone(), results));
    }
    out
}

pub async fn handle_offset_commit(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_offset_commit_request(header.api_version, body)?;
    let topics = offset_commit(storage, &req).await;
    Ok(encode_offset_commit_response(header.api_version, &topics))
}

// ═══════════════════════════════════════════════════════════════════════
// OffsetFetch (9)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchRequest {
    pub group_id: String,
    /// `None` (v2+ null array) means "all topics with committed offsets".
    pub topics: Option<Vec<(String, Vec<i32>)>>,
}

pub fn parse_offset_fetch_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<OffsetFetchRequest, ProtocolError> {
    let group_id = read_string(body)?;
    let topics = match read_nullable_array_len(body)? {
        None if version >= 2 => None,
        None => Some(Vec::new()),
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = read_string(body)?;
                let np = read_array_len(body)?;
                let mut partitions = Vec::with_capacity(np);
                for _ in 0..np {
                    partitions.push(read_i32(body)?);
                }
                topics.push((name, partitions));
            }
            Some(topics)
        }
    };
    Ok(OffsetFetchRequest { group_id, topics })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
    pub error_code: ErrorCode,
}

pub fn encode_offset_fetch_response(
    version: i16,
    topics: &[(String, Vec<OffsetFetchPartition>)],
    error: ErrorCode,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    if version >= 3 {
        write_i32(&mut buf, 0);
    }
    write_i32(&mut buf, topics.len() as i32);
    for (name, partitions) in topics {
        write_string(&mut buf, name);
        write_i32(&mut buf, partitions.len() as i32);
        for p in partitions {
            write_i32(&mut buf, p.partition_index);
            write_i64(&mut buf, p.committed_offset);
            if version >= 5 {
                write_i32(&mut buf, -1); // committed_leader_epoch
            }
            write_nullable_string(&mut buf, p.metadata.as_deref());
            write_i16(&mut buf, p.error_code as i16);
        }
    }
    if version >= 2 {
        write_i16(&mut buf, error as i16);
    }
    buf
}

async fn offset_fetch(
    storage: &SpiStorageClient,
    req: &OffsetFetchRequest,
) -> (Vec<(String, Vec<OffsetFetchPartition>)>, ErrorCode) {
    let none = |index: i32| OffsetFetchPartition {
        partition_index: index,
        committed_offset: -1,
        metadata: Some(String::new()),
        error_code: ErrorCode::None,
    };
    let group = match storage.get_group(&req.group_id).await {
        Ok(g) => g,
        Err(e) => return (Vec::new(), storage_error_code("get_group", e)),
    };
    match (&req.topics, group) {
        // Unknown group: every requested partition has no committed offset.
        (Some(topics), None) => (
            topics
                .iter()
                .map(|(name, parts)| (name.clone(), parts.iter().map(|p| none(*p)).collect()))
                .collect(),
            ErrorCode::None,
        ),
        (None, None) => (Vec::new(), ErrorCode::None),
        (None, Some(group)) => match storage.fetch_offsets(group.id, None).await {
            Ok(offsets) => {
                let mut topics: Vec<(String, Vec<OffsetFetchPartition>)> = Vec::new();
                for o in offsets {
                    let entry = OffsetFetchPartition {
                        partition_index: o.partition,
                        committed_offset: o.offset,
                        metadata: Some(o.metadata.unwrap_or_default()),
                        error_code: ErrorCode::None,
                    };
                    match topics.iter_mut().find(|(n, _)| *n == o.topic) {
                        Some((_, parts)) => parts.push(entry),
                        None => topics.push((o.topic, vec![entry])),
                    }
                }
                (topics, ErrorCode::None)
            }
            Err(e) => (Vec::new(), storage_error_code("fetch_offsets", e)),
        },
        (Some(topics), Some(group)) => {
            let mut out = Vec::with_capacity(topics.len());
            for (name, partitions) in topics {
                let committed = match storage.fetch_offsets(group.id, Some(name)).await {
                    Ok(c) => c,
                    Err(e) => return (Vec::new(), storage_error_code("fetch_offsets", e)),
                };
                let parts = partitions
                    .iter()
                    .map(|p| match committed.iter().find(|c| c.partition == *p) {
                        Some(c) => OffsetFetchPartition {
                            partition_index: *p,
                            committed_offset: c.offset,
                            metadata: Some(c.metadata.clone().unwrap_or_default()),
                            error_code: ErrorCode::None,
                        },
                        None => none(*p),
                    })
                    .collect();
                out.push((name.clone(), parts));
            }
            (out, ErrorCode::None)
        }
    }
}

pub async fn handle_offset_fetch(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_offset_fetch_request(header.api_version, body)?;
    let (topics, error) = offset_fetch(storage, &req).await;
    Ok(encode_offset_fetch_response(
        header.api_version,
        &topics,
        error,
    ))
}

// ═══════════════════════════════════════════════════════════════════════
// ListGroups (16)
// ═══════════════════════════════════════════════════════════════════════

pub fn encode_list_groups_response(
    version: i16,
    error: ErrorCode,
    groups: &[(String, String)],
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    if version >= 1 {
        write_i32(&mut buf, 0);
    }
    write_i16(&mut buf, error as i16);
    write_i32(&mut buf, groups.len() as i32);
    for (group_id, protocol_type) in groups {
        write_string(&mut buf, group_id);
        write_string(&mut buf, protocol_type);
    }
    buf
}

pub async fn handle_list_groups(
    header: &RequestHeader,
    _body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let (error, groups) = match storage.list_groups().await {
        Ok(groups) => (
            ErrorCode::None,
            groups
                .into_iter()
                .map(|g| (g.group_id, g.protocol_type.unwrap_or_default()))
                .collect(),
        ),
        Err(e) => (storage_error_code("list_groups", e), Vec::new()),
    };
    Ok(encode_list_groups_response(
        header.api_version,
        error,
        &groups,
    ))
}

// ═══════════════════════════════════════════════════════════════════════
// DescribeGroups (15)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
    pub assignment: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    pub error_code: ErrorCode,
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub members: Vec<DescribedMember>,
}

pub fn parse_describe_groups_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<Vec<String>, ProtocolError> {
    let groups = read_string_array(body)?;
    if version >= 3 {
        let _include_authorized_operations = read_bool(body)?;
    }
    Ok(groups)
}

pub fn encode_describe_groups_response(version: i16, groups: &[DescribedGroup]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(128);
    if version >= 1 {
        write_i32(&mut buf, 0);
    }
    write_i32(&mut buf, groups.len() as i32);
    for g in groups {
        write_i16(&mut buf, g.error_code as i16);
        write_string(&mut buf, &g.group_id);
        write_string(&mut buf, &g.state);
        write_string(&mut buf, &g.protocol_type);
        write_string(&mut buf, &g.protocol);
        write_i32(&mut buf, g.members.len() as i32);
        for m in &g.members {
            write_string(&mut buf, &m.member_id);
            if version >= 4 {
                write_nullable_string(&mut buf, None); // group_instance_id
            }
            write_string(&mut buf, &m.client_id);
            write_string(&mut buf, &m.client_host);
            write_bytes(&mut buf, &m.metadata);
            write_bytes(&mut buf, &m.assignment);
        }
        if version >= 3 {
            write_i32(&mut buf, -2147483648_i32); // authorized_operations: N/A
        }
    }
    buf
}

async fn describe_group(storage: &SpiStorageClient, group_id: &str) -> DescribedGroup {
    let dead = |code: ErrorCode| DescribedGroup {
        error_code: code,
        group_id: group_id.to_string(),
        state: STATE_DEAD.to_string(),
        protocol_type: String::new(),
        protocol: String::new(),
        members: Vec::new(),
    };
    let group = match storage.get_group(group_id).await {
        Ok(Some(g)) => g,
        Ok(None) => return dead(ErrorCode::None),
        Err(e) => return dead(storage_error_code("get_group", e)),
    };
    let members = match storage.get_members(group.id).await {
        Ok(m) => m,
        Err(e) => return dead(storage_error_code("get_members", e)),
    };
    let protocol = group.protocol.clone().unwrap_or_default();
    DescribedGroup {
        error_code: ErrorCode::None,
        group_id: group.group_id,
        state: group.state,
        protocol_type: group.protocol_type.unwrap_or_default(),
        protocol: protocol.clone(),
        members: members
            .into_iter()
            .map(|m| DescribedMember {
                metadata: m
                    .metadata_for(&protocol)
                    .map(<[u8]>::to_vec)
                    .unwrap_or_default(),
                assignment: m.assignment.clone().unwrap_or_default(),
                member_id: m.member_id,
                client_id: m.client_id,
                client_host: m.client_host,
            })
            .collect(),
    }
}

pub async fn handle_describe_groups(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let ids = parse_describe_groups_request(header.api_version, body)?;
    let mut groups = Vec::with_capacity(ids.len());
    for id in &ids {
        groups.push(describe_group(storage, id).await);
    }
    Ok(encode_describe_groups_response(header.api_version, &groups))
}

// ═══════════════════════════════════════════════════════════════════════
// Tests (pure wire format, no database)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BufMut};

    fn cursor(buf: &BytesMut) -> Cursor<&[u8]> {
        Cursor::new(buf.as_ref())
    }

    fn join_body(version: i16) -> BytesMut {
        let mut b = BytesMut::new();
        write_string(&mut b, "g1");
        write_i32(&mut b, 30_000);
        if version >= 1 {
            write_i32(&mut b, 60_000);
        }
        write_string(&mut b, "");
        if version >= 5 {
            write_nullable_string(&mut b, None);
        }
        write_string(&mut b, "consumer");
        write_i32(&mut b, 2);
        write_string(&mut b, "range");
        write_bytes(&mut b, b"m1");
        write_string(&mut b, "roundrobin");
        write_bytes(&mut b, b"");
        b
    }

    #[test]
    fn test_parse_join_group_all_versions() {
        for version in 0..=5 {
            let body = join_body(version);
            let mut cur = cursor(&body);
            let req = parse_join_group_request(version, &mut cur).unwrap();
            assert_eq!(req.group_id, "g1");
            assert_eq!(req.session_timeout_ms, 30_000);
            assert_eq!(
                req.rebalance_timeout_ms,
                if version >= 1 { 60_000 } else { 30_000 }
            );
            assert_eq!(req.member_id, "");
            assert_eq!(req.protocol_type, "consumer");
            assert_eq!(req.protocols[0], ("range".to_string(), b"m1".to_vec()));
            assert_eq!(req.protocols[1].0, "roundrobin");
            assert_eq!(cur.remaining(), 0, "v{} left bytes unread", version);
        }
    }

    #[test]
    fn test_encode_join_group_response_versions() {
        let resp = JoinGroupResponse {
            error_code: ErrorCode::None,
            generation_id: 3,
            protocol_name: "range".into(),
            leader: "a".into(),
            member_id: "a".into(),
            members: vec![("a".into(), b"x".to_vec())],
        };
        // v0: error(2) gen(4) proto(2+5) leader(2+1) member(2+1) n(4) [member(2+1) meta(4+1)]
        let v0 = encode_join_group_response(0, &resp);
        assert_eq!(v0.len(), 2 + 4 + 7 + 3 + 3 + 4 + 3 + 5);
        let mut cur = cursor(&v0);
        assert_eq!(read_i16(&mut cur).unwrap(), 0);
        assert_eq!(read_i32(&mut cur).unwrap(), 3);
        assert_eq!(read_string(&mut cur).unwrap(), "range");
        // v2 adds throttle, v5 adds nullable instance id per member (2 bytes)
        assert_eq!(encode_join_group_response(2, &resp).len(), v0.len() + 4);
        assert_eq!(encode_join_group_response(5, &resp).len(), v0.len() + 4 + 2);
    }

    #[test]
    fn test_sync_group_roundtrip() {
        for version in 0..=3 {
            let mut b = BytesMut::new();
            write_string(&mut b, "g1");
            write_i32(&mut b, 7);
            write_string(&mut b, "member-1");
            if version >= 3 {
                write_nullable_string(&mut b, Some("inst"));
            }
            write_i32(&mut b, 1);
            write_string(&mut b, "member-1");
            write_bytes(&mut b, b"assign");
            let mut cur = cursor(&b);
            let req = parse_sync_group_request(version, &mut cur).unwrap();
            assert_eq!(req.generation_id, 7);
            assert_eq!(
                req.assignments,
                vec![("member-1".into(), b"assign".to_vec())]
            );
            assert_eq!(cur.remaining(), 0);

            let resp = encode_sync_group_response(version, ErrorCode::None, b"assign");
            let mut cur = cursor(&resp);
            if version >= 1 {
                assert_eq!(read_i32(&mut cur).unwrap(), 0);
            }
            assert_eq!(read_i16(&mut cur).unwrap(), 0);
            assert_eq!(read_bytes(&mut cur).unwrap().as_ref(), b"assign");
            assert_eq!(cur.remaining(), 0);
        }
    }

    #[test]
    fn test_heartbeat_roundtrip() {
        for version in 0..=3 {
            let mut b = BytesMut::new();
            write_string(&mut b, "g1");
            write_i32(&mut b, 2);
            write_string(&mut b, "m");
            if version >= 3 {
                write_nullable_string(&mut b, None);
            }
            let mut cur = cursor(&b);
            let req = parse_heartbeat_request(version, &mut cur).unwrap();
            assert_eq!(req.member_id, "m");
            assert_eq!(cur.remaining(), 0);
            let resp = encode_heartbeat_response(version, ErrorCode::RebalanceInProgress);
            assert_eq!(resp.len(), if version >= 1 { 6 } else { 2 });
            assert_eq!(&resp[resp.len() - 2..], &[0, 27]);
        }
    }

    #[test]
    fn test_leave_group_v0_and_v3() {
        let mut b = BytesMut::new();
        write_string(&mut b, "g1");
        write_string(&mut b, "m1");
        let mut cur = cursor(&b);
        let req = parse_leave_group_request(0, &mut cur).unwrap();
        assert_eq!(req.members, vec![("m1".to_string(), None)]);

        let mut b = BytesMut::new();
        write_string(&mut b, "g1");
        write_i32(&mut b, 2);
        write_string(&mut b, "m1");
        write_nullable_string(&mut b, None);
        write_string(&mut b, "m2");
        write_nullable_string(&mut b, Some("i2"));
        let mut cur = cursor(&b);
        let req = parse_leave_group_request(3, &mut cur).unwrap();
        assert_eq!(req.members.len(), 2);
        assert_eq!(req.members[1], ("m2".to_string(), Some("i2".to_string())));

        let members = vec![("m1".to_string(), None, ErrorCode::None)];
        assert_eq!(
            encode_leave_group_response(0, ErrorCode::None, &members).len(),
            2
        );
        let v3 = encode_leave_group_response(3, ErrorCode::None, &members);
        // throttle(4) error(2) n(4) member(2+2) instance(2) error(2)
        assert_eq!(v3.len(), 4 + 2 + 4 + 4 + 2 + 2);
    }

    fn offset_commit_body(version: i16) -> BytesMut {
        let mut b = BytesMut::new();
        write_string(&mut b, "g1");
        if version >= 1 {
            write_i32(&mut b, 5);
            write_string(&mut b, "m1");
        }
        if version >= 7 {
            write_nullable_string(&mut b, None);
        }
        if (2..=4).contains(&version) {
            write_i64(&mut b, -1);
        }
        write_i32(&mut b, 1);
        write_string(&mut b, "t");
        write_i32(&mut b, 1);
        write_i32(&mut b, 0);
        write_i64(&mut b, 42);
        if version == 1 {
            write_i64(&mut b, 1234);
        }
        if version >= 6 {
            write_i32(&mut b, -1);
        }
        write_nullable_string(&mut b, Some("meta"));
        b
    }

    #[test]
    fn test_parse_offset_commit_all_versions() {
        for version in 0..=7 {
            let body = offset_commit_body(version);
            let mut cur = cursor(&body);
            let req = parse_offset_commit_request(version, &mut cur).unwrap();
            assert_eq!(req.group_id, "g1");
            assert_eq!(req.generation_id, if version >= 1 { 5 } else { -1 });
            assert_eq!(req.topics[0].0, "t");
            let p = &req.topics[0].1[0];
            assert_eq!((p.partition_index, p.committed_offset), (0, 42));
            assert_eq!(p.committed_metadata.as_deref(), Some("meta"));
            assert_eq!(cur.remaining(), 0, "v{} left bytes unread", version);
        }
    }

    /// Regression for the librdkafka "buffer underflow for OffsetCommit v7"
    /// report in #126: v3+ responses start with throttle_time_ms.
    #[test]
    fn test_encode_offset_commit_response() {
        let topics = vec![("t".to_string(), vec![(0, ErrorCode::None)])];
        let v2 = encode_offset_commit_response(2, &topics);
        assert_eq!(v2.len(), 4 + 3 + 4 + 4 + 2);
        let v7 = encode_offset_commit_response(7, &topics);
        assert_eq!(v7.len(), v2.len() + 4);
        let mut cur = cursor(&v7);
        assert_eq!(read_i32(&mut cur).unwrap(), 0, "throttle_time_ms");
        assert_eq!(read_i32(&mut cur).unwrap(), 1, "topic count");
    }

    #[test]
    fn test_parse_offset_fetch_null_topics() {
        let mut b = BytesMut::new();
        write_string(&mut b, "g1");
        write_i32(&mut b, -1);
        let mut cur = cursor(&b);
        assert_eq!(
            parse_offset_fetch_request(2, &mut cur).unwrap().topics,
            None
        );
        let mut cur = cursor(&b);
        assert_eq!(
            parse_offset_fetch_request(1, &mut cur).unwrap().topics,
            Some(vec![])
        );

        let mut b = BytesMut::new();
        write_string(&mut b, "g1");
        write_i32(&mut b, 1);
        write_string(&mut b, "t");
        write_i32(&mut b, 2);
        write_i32(&mut b, 0);
        write_i32(&mut b, 1);
        let mut cur = cursor(&b);
        let req = parse_offset_fetch_request(5, &mut cur).unwrap();
        assert_eq!(req.topics, Some(vec![("t".to_string(), vec![0, 1])]));
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn test_encode_offset_fetch_response_versions() {
        let topics = vec![(
            "t".to_string(),
            vec![OffsetFetchPartition {
                partition_index: 0,
                committed_offset: 9,
                metadata: Some(String::new()),
                error_code: ErrorCode::None,
            }],
        )];
        // v0: n(4) name(3) np(4) idx(4) off(8) meta(2) err(2)
        let v0 = encode_offset_fetch_response(0, &topics, ErrorCode::None);
        assert_eq!(v0.len(), 4 + 3 + 4 + 4 + 8 + 2 + 2);
        let v2 = encode_offset_fetch_response(2, &topics, ErrorCode::None);
        assert_eq!(v2.len(), v0.len() + 2);
        let v3 = encode_offset_fetch_response(3, &topics, ErrorCode::None);
        assert_eq!(v3.len(), v0.len() + 2 + 4);
        let v5 = encode_offset_fetch_response(5, &topics, ErrorCode::None);
        assert_eq!(v5.len(), v0.len() + 2 + 4 + 4);
        let mut cur = cursor(&v5);
        assert_eq!(read_i32(&mut cur).unwrap(), 0);
        assert_eq!(read_i32(&mut cur).unwrap(), 1);
        assert_eq!(read_string(&mut cur).unwrap(), "t");
        assert_eq!(read_i32(&mut cur).unwrap(), 1);
        assert_eq!(read_i32(&mut cur).unwrap(), 0);
        assert_eq!(read_i64(&mut cur).unwrap(), 9);
        assert_eq!(read_i32(&mut cur).unwrap(), -1);
        assert_eq!(read_nullable_string(&mut cur).unwrap(), Some(String::new()));
        assert_eq!(read_i16(&mut cur).unwrap(), 0);
        assert_eq!(read_i16(&mut cur).unwrap(), 0);
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn test_encode_list_groups_response() {
        let groups = vec![("g1".to_string(), "consumer".to_string())];
        let v0 = encode_list_groups_response(0, ErrorCode::None, &groups);
        assert_eq!(v0.len(), 2 + 4 + 4 + 10);
        let v1 = encode_list_groups_response(1, ErrorCode::None, &groups);
        assert_eq!(v1.len(), v0.len() + 4);
        let mut cur = cursor(&v1);
        assert_eq!(read_i32(&mut cur).unwrap(), 0);
        assert_eq!(read_i16(&mut cur).unwrap(), 0);
        assert_eq!(read_i32(&mut cur).unwrap(), 1);
        assert_eq!(read_string(&mut cur).unwrap(), "g1");
        assert_eq!(read_string(&mut cur).unwrap(), "consumer");
    }

    #[test]
    fn test_describe_groups_roundtrip() {
        let mut b = BytesMut::new();
        write_i32(&mut b, 1);
        write_string(&mut b, "g1");
        b.put_u8(1);
        let mut cur = cursor(&b);
        assert_eq!(
            parse_describe_groups_request(3, &mut cur).unwrap(),
            vec!["g1"]
        );
        assert_eq!(cur.remaining(), 0);

        let groups = vec![DescribedGroup {
            error_code: ErrorCode::None,
            group_id: "g1".into(),
            state: "Stable".into(),
            protocol_type: "consumer".into(),
            protocol: "range".into(),
            members: vec![DescribedMember {
                member_id: "m".into(),
                client_id: "c".into(),
                client_host: "127.0.0.1".into(),
                metadata: vec![1],
                assignment: vec![],
            }],
        }];
        let v0 = encode_describe_groups_response(0, &groups);
        let base = 4 + 2 + 4 + 8 + 10 + 7 + 4 + (3 + 3 + 11 + 5 + 4);
        assert_eq!(v0.len(), base);
        assert_eq!(encode_describe_groups_response(1, &groups).len(), base + 4);
        assert_eq!(
            encode_describe_groups_response(3, &groups).len(),
            base + 4 + 4
        );
        assert_eq!(
            encode_describe_groups_response(4, &groups).len(),
            base + 4 + 4 + 2
        );
    }

    #[test]
    fn test_generate_member_id_is_unique_and_prefixed() {
        let a = generate_member_id("rdkafka");
        let b = generate_member_id("rdkafka");
        assert_ne!(a, b);
        assert!(a.starts_with("rdkafka-"));
        assert!(generate_member_id("").starts_with("consumer-"));
    }

    #[test]
    fn test_wait_budget_bounds() {
        assert_eq!(wait_budget(0), Duration::from_millis(60_000));
        assert_eq!(wait_budget(-5), Duration::from_millis(60_000));
        assert_eq!(wait_budget(1_000), Duration::from_millis(1_000));
        assert_eq!(wait_budget(i32::MAX), Duration::from_millis(300_000));
    }
}
