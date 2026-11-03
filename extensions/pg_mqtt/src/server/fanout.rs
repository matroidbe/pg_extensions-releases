//! Fan-out cursor over `pgmqtt.outbox` (design/pg_mqtt/fanout.md)
//!
//! Pure bookkeeping, no pgrx: tokio tasks drive it.

use std::time::{Duration, Instant};

/// How often a worker polls the outbox
pub const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often a worker deletes delivered rows, and how old the horizon it
/// deletes below must be
pub const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Read the rows of transactions that finished since the last poll.
/// $1: cursor (xid8 as text, NULL on the first poll), $2: this worker's id.
pub const POLL_SQL: &str = "WITH s AS (SELECT pg_snapshot_xmin(pg_current_snapshot()) AS horizon)
     SELECT s.horizon::text, o.topic, o.payload, o.qos::int, o.retain, o.properties,
            o.origin_client, (extract(epoch FROM o.created_at) * 1000000)::bigint
     FROM s LEFT JOIN pgmqtt.outbox o
       ON o.xid >= $1::text::xid8 AND o.xid < s.horizon
      AND o.origin_worker IS DISTINCT FROM $2
     ORDER BY o.xid, o.id";

/// Delete rows below a horizon every worker has passed. $1: xid8 as text.
pub const CLEANUP_SQL: &str = "DELETE FROM pgmqtt.outbox WHERE xid < $1::text::xid8";

/// Enqueue a message for the other workers. $1 topic, $2 payload, $3 origin
/// worker, $4 QoS it was published with, $5 its RETAIN flag, $6 its PUBLISH
/// properties, $7 the publishing client (for No Local).
pub const ENQUEUE_SQL: &str = "INSERT INTO pgmqtt.outbox
         (topic, payload, origin_worker, qos, retain, properties, origin_client)
     VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// One message to deliver to local subscribers
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    pub topic: String,
    pub payload: Vec<u8>,
    /// QoS the message was published with (0 for SQL publishes)
    pub qos: u8,
    /// RETAIN flag it was published with
    pub retain: bool,
    /// PUBLISH property block (codec::encode_publish_properties)
    pub properties: Vec<u8>,
    /// Client that published it; None for SQL publishes
    pub origin_client: Option<String>,
    /// When it was enqueued, in microseconds since the epoch
    pub created_micros: i64,
}

/// Position of one worker in the outbox: the transaction horizon of its last
/// poll. Every transaction below it has finished, so rows below it were read.
#[derive(Debug, Default)]
pub struct FanoutCursor {
    cursor: Option<u64>,
    cleanup_sample: Option<(Instant, u64)>,
}

impl FanoutCursor {
    pub fn new() -> Self {
        Self::default()
    }

    /// The horizon of the last poll; None before the first
    pub fn cursor(&self) -> Option<u64> {
        self.cursor
    }

    /// Record a poll at `horizon` and return the rows to deliver. The first
    /// poll only positions the cursor: older messages are not replayed.
    pub fn advance(&mut self, horizon: u64, rows: Vec<OutboxRow>) -> Vec<OutboxRow> {
        match self.cursor {
            None => {
                self.cursor = Some(horizon);
                Vec::new()
            }
            Some(c) => {
                self.cursor = Some(c.max(horizon));
                rows
            }
        }
    }

    /// Every CLEANUP_INTERVAL, the horizon sampled one interval earlier:
    /// rows below it can be deleted.
    pub fn cleanup_bound(&mut self, now: Instant) -> Option<u64> {
        let cursor = self.cursor?;
        match self.cleanup_sample {
            None => {
                self.cleanup_sample = Some((now, cursor));
                None
            }
            Some((at, bound)) if now.duration_since(at) >= CLEANUP_INTERVAL => {
                self.cleanup_sample = Some((now, cursor));
                Some(bound)
            }
            Some(_) => None,
        }
    }
}

/// One row of POLL_SQL. With no new messages there is one row, with only the
/// horizon set.
#[derive(Debug, Default)]
pub struct PollRow {
    pub horizon: Option<String>,
    pub topic: Option<String>,
    pub payload: Option<Vec<u8>>,
    pub qos: Option<i32>,
    pub retain: Option<bool>,
    pub properties: Option<Vec<u8>>,
    pub origin_client: Option<String>,
    pub created_micros: Option<i64>,
}

/// Parse the rows of POLL_SQL
pub fn parse_poll(rows: Vec<PollRow>) -> Result<(u64, Vec<OutboxRow>), String> {
    let horizon = rows
        .first()
        .and_then(|r| r.horizon.as_deref())
        .ok_or("outbox poll returned no horizon")?;
    let horizon: u64 = horizon
        .parse()
        .map_err(|e| format!("outbox poll horizon '{horizon}': {e}"))?;
    let messages = rows
        .into_iter()
        .filter_map(|row| {
            Some(OutboxRow {
                topic: row.topic?,
                payload: row.payload.unwrap_or_default(),
                qos: row.qos.unwrap_or(0).clamp(0, 2) as u8,
                retain: row.retain.unwrap_or(false),
                properties: row.properties.unwrap_or_default(),
                origin_client: row.origin_client,
                created_micros: row.created_micros.unwrap_or(i64::MAX),
            })
        })
        .collect();
    Ok((horizon, messages))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(topic: &str) -> OutboxRow {
        OutboxRow {
            topic: topic.to_string(),
            payload: topic.as_bytes().to_vec(),
            qos: 0,
            retain: false,
            properties: Vec::new(),
            origin_client: None,
            created_micros: 0,
        }
    }

    #[test]
    fn first_poll_only_sets_the_cursor() {
        let mut c = FanoutCursor::new();
        assert_eq!(c.cursor(), None);
        assert!(c.advance(100, vec![row("old")]).is_empty());
        assert_eq!(c.cursor(), Some(100));
    }

    #[test]
    fn later_polls_deliver_rows_and_move_the_cursor() {
        let mut c = FanoutCursor::new();
        c.advance(100, vec![]);
        let out = c.advance(105, vec![row("a"), row("b")]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].topic, "a");
        assert_eq!(c.cursor(), Some(105));
    }

    #[test]
    fn cursor_never_moves_backwards() {
        let mut c = FanoutCursor::new();
        c.advance(100, vec![]);
        c.advance(90, vec![]);
        assert_eq!(c.cursor(), Some(100));
    }

    #[test]
    fn cleanup_bound_is_a_horizon_one_interval_old() {
        let t0 = Instant::now();
        let mut c = FanoutCursor::new();
        assert_eq!(c.cleanup_bound(t0), None, "no cursor yet");

        c.advance(100, vec![]);
        assert_eq!(c.cleanup_bound(t0), None, "first sample");

        c.advance(200, vec![]);
        assert_eq!(c.cleanup_bound(t0 + CLEANUP_INTERVAL / 2), None);
        assert_eq!(c.cleanup_bound(t0 + CLEANUP_INTERVAL), Some(100));

        c.advance(300, vec![]);
        assert_eq!(
            c.cleanup_bound(t0 + CLEANUP_INTERVAL * 2),
            Some(200),
            "next bound is the horizon sampled at the previous cleanup"
        );
    }

    #[test]
    fn parse_poll_reads_horizon_and_rows() {
        let (horizon, rows) = parse_poll(vec![
            PollRow {
                horizon: Some("42".into()),
                topic: Some("t/1".into()),
                payload: Some(b"x".to_vec()),
                qos: Some(1),
                retain: Some(true),
                properties: Some(vec![0]),
                origin_client: Some("c".into()),
                created_micros: Some(5),
            },
            PollRow {
                horizon: Some("42".into()),
                topic: Some("t/2".into()),
                ..Default::default()
            },
        ])
        .unwrap();
        assert_eq!(horizon, 42);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].payload, b"x");
        assert_eq!((rows[0].qos, rows[0].retain), (1, true));
        assert_eq!(rows[0].origin_client.as_deref(), Some("c"));
        assert!(rows[1].payload.is_empty());
        assert_eq!((rows[1].qos, rows[1].retain), (0, false));
    }

    #[test]
    fn parse_poll_with_no_rows_is_just_the_horizon() {
        // LEFT JOIN with no match: one row, topic NULL
        let (horizon, rows) = parse_poll(vec![PollRow {
            horizon: Some("7".into()),
            ..Default::default()
        }])
        .unwrap();
        assert_eq!(horizon, 7);
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_poll_rejects_a_missing_horizon() {
        assert!(parse_poll(vec![]).is_err());
        assert!(parse_poll(vec![PollRow {
            horizon: Some("x".into()),
            ..Default::default()
        }])
        .is_err());
    }
}
