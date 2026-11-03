//! The queue of messages on their way to one connected client
//!
//! The broker adds to it (local delivery, fan-out from other workers, session
//! takeover); the client's connection task takes from it as fast as the client
//! accepts messages: QoS 0 as fast as the socket takes them, QoS 1 and 2 as
//! the client's Receive Maximum allows.
//!
//! It is bounded (`pg_mqtt.max_queued_messages`). When a client does not keep
//! up and its queue is full:
//! - a QoS 0 message makes room by dropping the oldest QoS 0 message waiting
//!   (itself, when none is), and the drop is counted: at most once allows it;
//! - a QoS 1 or 2 message is never dropped while the connection lives. It
//!   makes room by dropping a QoS 0 message if there is one; otherwise the
//!   client is disconnected with 0x97 Quota exceeded. Its session ends with the
//!   connection, so the client sees that it lost messages and reconnects,
//!   instead of silently missing them.
//!
//! No pgrx here: this runs on tokio threads.

use std::collections::VecDeque;
use std::sync::Mutex;
use tokio::sync::Notify;

use super::handlers::Delivery;
use super::types::{QoS, ReasonCode};

/// Default of `pg_mqtt.max_queued_messages`
pub const DEFAULT_MAX_QUEUED: usize = 10_000;

/// The queue of one client
pub struct ClientQueue {
    state: Mutex<QueueState>,
    ready: Notify,
    capacity: usize,
}

#[derive(Default)]
struct QueueState {
    messages: VecDeque<Delivery>,
    /// Set when the connection must close; wins over waiting messages
    close: Option<ReasonCode>,
    /// QoS 0 messages dropped because the client did not keep up
    dropped_qos0: u64,
}

/// What `push` did with a message
#[derive(Debug, PartialEq, Eq)]
pub enum Pushed {
    Queued,
    /// The queue was full: a QoS 0 message was dropped to make room (the
    /// oldest waiting, or the new one). Carries the count of drops so far.
    DroppedQos0(u64),
    /// The queue was full of QoS 1 and 2 messages: the client is told to
    /// disconnect (Quota exceeded)
    Overflow,
    /// The connection is already closing; the message is discarded with it
    Closing,
}

/// What the connection task does next
#[derive(Debug)]
pub enum Next {
    Deliver(Delivery),
    Close(ReasonCode),
}

impl ClientQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            ready: Notify::new(),
            capacity: capacity.max(1),
        }
    }

    /// Add a message for the client (see the module docs for a full queue)
    pub fn push(&self, delivery: Delivery) -> Pushed {
        let pushed = {
            let mut state = self.state.lock().unwrap();
            if state.close.is_some() {
                return Pushed::Closing;
            }
            if state.messages.len() < self.capacity {
                state.messages.push_back(delivery);
                Pushed::Queued
            } else {
                let oldest_qos0 = state.messages.iter().position(|m| m.qos == QoS::AtMostOnce);
                match (oldest_qos0, delivery.qos) {
                    (Some(i), _) => {
                        state.messages.remove(i);
                        state.messages.push_back(delivery);
                        state.dropped_qos0 += 1;
                        Pushed::DroppedQos0(state.dropped_qos0)
                    }
                    (None, QoS::AtMostOnce) => {
                        state.dropped_qos0 += 1;
                        Pushed::DroppedQos0(state.dropped_qos0)
                    }
                    (None, _) => {
                        state.close = Some(ReasonCode::QuotaExceeded);
                        state.messages.clear();
                        Pushed::Overflow
                    }
                }
            }
        };
        self.ready.notify_one();
        pushed
    }

    /// Tell the connection to send DISCONNECT with `reason_code` and close,
    /// before any message still waiting
    pub fn close(&self, reason_code: ReasonCode) {
        {
            let mut state = self.state.lock().unwrap();
            state.close.get_or_insert(reason_code);
            state.messages.clear();
        }
        self.ready.notify_one();
    }

    /// The next thing to do for the client, waiting until there is one.
    /// `acked_slot_free` says whether a QoS 1 or 2 message may be sent now;
    /// while not, a QoS 1 or 2 message at the front waits, and so does
    /// everything behind it (messages leave in the order they came).
    ///
    /// Cancel safe: a message leaves the queue only in the poll that returns it.
    pub async fn next(&self, acked_slot_free: bool) -> Next {
        loop {
            if let Some(next) = self.try_next(acked_slot_free) {
                return next;
            }
            self.ready.notified().await;
        }
    }

    fn try_next(&self, acked_slot_free: bool) -> Option<Next> {
        let mut state = self.state.lock().unwrap();
        if let Some(reason_code) = state.close {
            return Some(Next::Close(reason_code));
        }
        let front = state.messages.front()?;
        if front.qos != QoS::AtMostOnce && !acked_slot_free {
            return None;
        }
        state.messages.pop_front().map(Next::Deliver)
    }

    /// QoS 0 messages dropped so far because the client did not keep up
    pub fn dropped_qos0(&self) -> u64 {
        self.state.lock().unwrap().dropped_qos0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::PublishProperties;
    use std::time::Duration;

    fn msg(n: u8, qos: QoS) -> Delivery {
        Delivery {
            topic: "t".into(),
            payload: vec![n],
            qos,
            retain: false,
            properties: PublishProperties::default(),
        }
    }

    fn payload(next: Next) -> u8 {
        match next {
            Next::Deliver(d) => d.payload[0],
            Next::Close(rc) => panic!("closed with {rc:?}"),
        }
    }

    async fn next_now(q: &ClientQueue, slot: bool) -> Option<Next> {
        tokio::time::timeout(Duration::from_millis(20), q.next(slot))
            .await
            .ok()
    }

    #[tokio::test]
    async fn full_of_qos0_drops_the_oldest() {
        let q = ClientQueue::new(3);
        for n in 1..=3 {
            assert_eq!(q.push(msg(n, QoS::AtMostOnce)), Pushed::Queued);
        }
        assert_eq!(q.push(msg(4, QoS::AtMostOnce)), Pushed::DroppedQos0(1));
        assert_eq!(q.push(msg(5, QoS::AtMostOnce)), Pushed::DroppedQos0(2));
        let got: Vec<u8> = [q.next(true).await, q.next(true).await, q.next(true).await]
            .into_iter()
            .map(payload)
            .collect();
        assert_eq!(got, [3, 4, 5]);
        assert_eq!(q.dropped_qos0(), 2);
    }

    #[tokio::test]
    async fn qos1_takes_the_place_of_a_qos0_message() {
        let q = ClientQueue::new(3);
        q.push(msg(1, QoS::AtLeastOnce));
        q.push(msg(2, QoS::AtMostOnce));
        q.push(msg(3, QoS::AtLeastOnce));
        assert_eq!(q.push(msg(4, QoS::ExactlyOnce)), Pushed::DroppedQos0(1));
        let got: Vec<u8> = [q.next(true).await, q.next(true).await, q.next(true).await]
            .into_iter()
            .map(payload)
            .collect();
        assert_eq!(got, [1, 3, 4]);
    }

    #[tokio::test]
    async fn qos0_is_dropped_when_only_qos1_waits() {
        let q = ClientQueue::new(2);
        q.push(msg(1, QoS::AtLeastOnce));
        q.push(msg(2, QoS::AtLeastOnce));
        assert_eq!(q.push(msg(3, QoS::AtMostOnce)), Pushed::DroppedQos0(1));
        assert_eq!(payload(q.next(true).await), 1);
        assert_eq!(payload(q.next(true).await), 2);
        assert!(next_now(&q, true).await.is_none());
    }

    #[tokio::test]
    async fn full_of_qos1_disconnects_instead_of_dropping() {
        let q = ClientQueue::new(2);
        q.push(msg(1, QoS::AtLeastOnce));
        q.push(msg(2, QoS::ExactlyOnce));
        assert_eq!(q.push(msg(3, QoS::AtLeastOnce)), Pushed::Overflow);
        assert!(matches!(
            q.next(true).await,
            Next::Close(ReasonCode::QuotaExceeded)
        ));
        assert_eq!(q.push(msg(4, QoS::AtMostOnce)), Pushed::Closing);
    }

    #[tokio::test]
    async fn acked_messages_wait_for_a_free_slot_in_order() {
        let q = ClientQueue::new(10);
        q.push(msg(1, QoS::AtMostOnce));
        q.push(msg(2, QoS::AtLeastOnce));
        q.push(msg(3, QoS::AtMostOnce));
        assert_eq!(payload(q.next(false).await), 1);
        // The QoS 1 message at the front holds back the QoS 0 one behind it
        assert!(next_now(&q, false).await.is_none());
        assert_eq!(payload(q.next(true).await), 2);
        assert_eq!(payload(q.next(false).await), 3);
    }

    #[tokio::test]
    async fn close_comes_before_waiting_messages() {
        let q = ClientQueue::new(10);
        q.push(msg(1, QoS::AtMostOnce));
        q.close(ReasonCode::SessionTakenOver);
        q.close(ReasonCode::QuotaExceeded);
        assert!(matches!(
            q.next(true).await,
            Next::Close(ReasonCode::SessionTakenOver)
        ));
    }

    #[tokio::test]
    async fn a_waiting_connection_wakes_on_push() {
        let q = std::sync::Arc::new(ClientQueue::new(10));
        let waiter = tokio::spawn({
            let q = q.clone();
            async move { payload(q.next(true).await) }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        q.push(msg(7, QoS::AtLeastOnce));
        assert_eq!(waiter.await.unwrap(), 7);
    }
}
