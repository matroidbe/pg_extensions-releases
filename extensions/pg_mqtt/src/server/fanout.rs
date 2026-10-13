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
     SELECT s.horizon::text, o.topic, o.payload
     FROM s LEFT JOIN pgmqtt.outbox o
       ON o.xid >= $1::text::xid8 AND o.xid < s.horizon
      AND o.origin_worker IS DISTINCT FROM $2
     ORDER BY o.xid, o.id";

/// Delete rows below a horizon every worker has passed. $1: xid8 as text.
pub const CLEANUP_SQL: &str = "DELETE FROM pgmqtt.outbox WHERE xid < $1::text::xid8";

/// Enqueue a message for the other workers. $1 topic, $2 payload, $3 origin.
pub const ENQUEUE_SQL: &str =
    "INSERT INTO pgmqtt.outbox (topic, payload, origin_worker) VALUES ($1, $2, $3)";

/// One message to deliver to local subscribers
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    pub topic: String,
    pub payload: Vec<u8>,
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

/// One (horizon, topic, payload) row of POLL_SQL
pub type PollRow = (Option<String>, Option<String>, Option<Vec<u8>>);

/// Parse the rows of POLL_SQL
pub fn parse_poll(rows: Vec<PollRow>) -> Result<(u64, Vec<OutboxRow>), String> {
    let horizon = rows
        .first()
        .and_then(|r| r.0.as_deref())
        .ok_or("outbox poll returned no horizon")?;
    let horizon: u64 = horizon
        .parse()
        .map_err(|e| format!("outbox poll horizon '{horizon}': {e}"))?;
    let messages = rows
        .into_iter()
        .filter_map(|(_, topic, payload)| {
            Some(OutboxRow {
                topic: topic?,
                payload: payload.unwrap_or_default(),
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
            (Some("42".into()), Some("t/1".into()), Some(b"x".to_vec())),
            (Some("42".into()), Some("t/2".into()), None),
        ])
        .unwrap();
        assert_eq!(horizon, 42);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].payload, b"x");
        assert!(rows[1].payload.is_empty());
    }

    #[test]
    fn parse_poll_with_no_rows_is_just_the_horizon() {
        // LEFT JOIN with no match: one row, topic NULL
        let (horizon, rows) = parse_poll(vec![(Some("7".into()), None, None)]).unwrap();
        assert_eq!(horizon, 7);
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_poll_rejects_a_missing_horizon() {
        assert!(parse_poll(vec![]).is_err());
        assert!(parse_poll(vec![(Some("x".into()), None, None)]).is_err());
    }
}
