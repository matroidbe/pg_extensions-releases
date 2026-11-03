//! The ingest queue: how connections hand PUBLISHes to the worker's main
//! thread, which stores them in batches (`batch.rs`).
//!
//! Connection tasks submit an item and keep reading; the main thread drains
//! whatever has queued, writes it in one transaction and answers each item
//! after the commit. While one batch commits the next one fills, so batches
//! grow with load and a lone message waits for nothing but its own commit
//! (group commit). The queue is bounded: when the main thread falls behind,
//! `submit` waits, the connection stops reading its socket, and the publisher
//! is slowed down by TCP (QoS 0) or by its full in-flight window (QoS 1/2).
//!
//! No pgrx here: this side runs on tokio threads.

use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::protocol::{PublishPacket, ReasonCode};
use crate::storage::spi_client::Subscription;

/// Messages waiting for the main thread before connections are made to wait
pub const INGEST_QUEUE: usize = 4096;

/// One PUBLISH (from a client, a Will) on its way to storage
pub struct IngestItem {
    pub packet: PublishPacket,
    /// The publishing client, for No Local; None when the broker publishes
    pub publisher: Option<String>,
    /// Answered once the batch holding the item committed (or failed)
    pub done: oneshot::Sender<IngestOutcome>,
}

/// What happened to an item
#[derive(Debug, Clone)]
pub struct IngestOutcome {
    /// Success, or why the message was not accepted (sent in PUBACK/PUBREC)
    pub reason_code: ReasonCode,
    /// Every subscription at the time of the commit; empty if not accepted
    pub subscriptions: Arc<Vec<Subscription>>,
}

impl IngestOutcome {
    /// An outcome for a message that was not stored
    pub fn rejected(reason_code: ReasonCode) -> Self {
        Self {
            reason_code,
            subscriptions: Arc::new(Vec::new()),
        }
    }

    pub fn accepted(&self) -> bool {
        (self.reason_code as u8) < 0x80
    }
}

/// The worker's latch, set after each submit so its main loop wakes at once
#[derive(Clone, Copy)]
struct WorkerLatch(*mut pgrx::pg_sys::Latch);

// SAFETY: the pointer is only handed to SetLatch, which PostgreSQL makes safe
// to call from outside the latch owner's flow of control: it only stores
// flags and wakes the owner (as in pg_spi's bridge).
unsafe impl Send for WorkerLatch {}
unsafe impl Sync for WorkerLatch {}

// Declared directly: pgrx's pg_sys wrapper refuses calls from any thread but
// the main one. SetLatch never raises an ERROR.
extern "C" {
    fn SetLatch(latch: *mut pgrx::pg_sys::Latch);
}

/// The submitting side of the ingest queue; clone it into every connection
#[derive(Clone)]
pub struct IngestQueue {
    tx: mpsc::Sender<IngestItem>,
    wake: Option<WorkerLatch>,
}

impl IngestQueue {
    /// A queue whose submits wake the background worker that calls this (call
    /// it on the worker's main thread)
    pub fn waking_worker(capacity: usize) -> (Self, mpsc::Receiver<IngestItem>) {
        let (tx, rx) = mpsc::channel(capacity);
        // SAFETY: read on the worker's main thread, where MyLatch is its own
        // process latch for the life of the process.
        let wake = Some(WorkerLatch(unsafe { pgrx::pg_sys::MyLatch }));
        (Self { tx, wake }, rx)
    }

    /// A queue nobody wakes (unit tests)
    pub fn unwoken(capacity: usize) -> (Self, mpsc::Receiver<IngestItem>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx, wake: None }, rx)
    }

    /// Queue a message for storage; waits while the queue is full. Returns
    /// the receiver of its outcome.
    pub async fn submit(
        &self,
        packet: PublishPacket,
        publisher: Option<String>,
    ) -> oneshot::Receiver<IngestOutcome> {
        let (done, outcome) = oneshot::channel();
        let item = IngestItem {
            packet,
            publisher,
            done,
        };
        if let Err(mpsc::error::SendError(item)) = self.tx.send(item).await {
            // The worker is shutting down
            let _ = item
                .done
                .send(IngestOutcome::rejected(ReasonCode::ServerUnavailable));
        }
        if let Some(latch) = self.wake {
            // SAFETY: see WorkerLatch
            unsafe { SetLatch(latch.0) };
        }
        outcome
    }
}
