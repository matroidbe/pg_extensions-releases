//! Async-to-sync bridge between [`AsyncSource`] / [`AsyncSink`] and the
//! engine's sync [`InputConnector`] / [`OutputConnector`] traits.
//!
//! A bridge owns a dedicated OS thread that runs a `current_thread` tokio
//! runtime. The async source/sink task pushes items into a bounded
//! `tokio::sync::mpsc` channel; the PG worker thread drains via
//! non-blocking `try_recv`.
//!
//! Cursors are tracked per-emitted-batch: `poll()` returns a fresh
//! batch_id as the offset value, the bridge remembers `batch_id ->
//! Cursor`, and `commit(batch_id)` persists the cursor and prunes the map.

use crate::connector::sdk::{AsyncSink, AsyncSource, Cursor, SourceItem};
use crate::connector::{InputConnector, OutputConnector};
use crate::record::{Record, RecordBatch};
use futures::StreamExt;
use pgrx::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;

/// Default capacity of the source-to-engine channel. One batch worth
/// of records — bounded so async producers backpressure naturally.
const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Wraps an [`AsyncSource`] and exposes it as a sync [`InputConnector`].
pub struct AsyncSourceBridge {
    connector_role: String,
    receiver: Option<mpsc::Receiver<Result<SourceItem, String>>>,
    /// Map of emitted batch_id -> cursor at end of that batch.
    pending_cursors: HashMap<i64, Cursor>,
    /// Most recent cursor seen (even if not yet emitted as batch_id).
    in_flight_cursor: Option<Cursor>,
    next_batch_id: i64,
    /// Joined when the bridge is dropped (channel closes, task exits).
    worker_thread: Option<std::thread::JoinHandle<()>>,
    /// Factory invoked at `initialize()` time. `Option` so we can take ownership.
    source_factory: Option<Box<dyn FnOnce() -> Box<dyn AsyncSource> + Send>>,
    /// Whether the bridge has been initialized.
    initialized: bool,
}

impl AsyncSourceBridge {
    /// Construct a bridge that will spawn `source_factory()` on its
    /// dedicated runtime when `initialize()` is called.
    pub fn new(
        connector_role: &str,
        source_factory: Box<dyn FnOnce() -> Box<dyn AsyncSource> + Send>,
    ) -> Self {
        Self {
            connector_role: connector_role.to_string(),
            receiver: None,
            pending_cursors: HashMap::new(),
            in_flight_cursor: None,
            next_batch_id: 1,
            worker_thread: None,
            source_factory: Some(source_factory),
            initialized: false,
        }
    }

    /// Load the persisted cursor for this pipeline + connector_role from SPI.
    fn load_cursor(&self, pipeline_name: &str) -> Cursor {
        let result = Spi::get_one_with_args::<pgrx::JsonB>(
            "SELECT cursor FROM pgstreams.connector_state \
             WHERE pipeline = $1 AND connector_role = $2",
            &[pipeline_name.into(), self.connector_role.as_str().into()],
        );
        match result {
            Ok(Some(jb)) => Cursor::from_json(&jb.0),
            _ => Cursor::None,
        }
    }

    /// Persist the cursor for this pipeline + connector_role to SPI.
    fn persist_cursor(&self, pipeline_name: &str, cursor: &Cursor) -> Result<(), String> {
        let cursor_json = pgrx::JsonB(cursor.to_json());
        Spi::run_with_args(
            "INSERT INTO pgstreams.connector_state (pipeline, connector_role, cursor) \
             VALUES ($1, $2, $3::jsonb) \
             ON CONFLICT (pipeline, connector_role) \
             DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()",
            &[
                pipeline_name.into(),
                self.connector_role.as_str().into(),
                cursor_json.into(),
            ],
        )
        .map_err(|e| format!("Failed to persist cursor: {}", e))?;
        Ok(())
    }
}

impl InputConnector for AsyncSourceBridge {
    fn initialize(&mut self, pipeline_name: &str) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }

        let last_cursor = self.load_cursor(pipeline_name);
        let factory = self
            .source_factory
            .take()
            .ok_or_else(|| "AsyncSourceBridge: source_factory missing".to_string())?;

        let (tx, rx) = mpsc::channel::<Result<SourceItem, String>>(DEFAULT_CHANNEL_CAPACITY);
        self.receiver = Some(rx);

        let pipeline_name_owned = pipeline_name.to_string();
        let role = self.connector_role.clone();
        let handle = std::thread::Builder::new()
            .name(format!("pg_streaming_src_{}_{}", role, pipeline_name_owned))
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(format!("Failed to build runtime: {}", e)));
                        return;
                    }
                };
                rt.block_on(async move {
                    let mut source = factory();
                    let mut current_cursor = last_cursor;
                    loop {
                        let stream_result = source.open(current_cursor.clone()).await;
                        let mut stream = match stream_result {
                            Ok(s) => s,
                            Err(e) => {
                                let _ = tx.send(Err(e)).await;
                                return;
                            }
                        };
                        while let Some(item_res) = stream.next().await {
                            if let Ok(item) = &item_res {
                                if !item.cursor_advance.is_none() {
                                    current_cursor = item.cursor_advance.clone();
                                }
                            }
                            if tx.send(item_res).await.is_err() {
                                // Receiver dropped — bridge gone.
                                return;
                            }
                        }
                        if !source.is_continuous() {
                            return;
                        }
                        // Sleep until the next reopen, but wake up immediately
                        // if the receiver is dropped (bridge teardown), so we
                        // don't pin executor cleanup for the full poll interval.
                        tokio::select! {
                            _ = tokio::time::sleep(source.poll_interval()) => {}
                            _ = tx.closed() => return,
                        }
                    }
                });
            })
            .map_err(|e| format!("Failed to spawn bridge worker thread: {}", e))?;

        self.worker_thread = Some(handle);
        self.initialized = true;
        Ok(())
    }

    fn poll(&mut self, batch_size: i32) -> Result<(RecordBatch, Option<i64>), String> {
        let rx = match self.receiver.as_mut() {
            Some(rx) => rx,
            None => return Err("AsyncSourceBridge: not initialized".to_string()),
        };

        let mut batch: Vec<Record> = Vec::new();
        let mut last_batch_cursor: Option<Cursor> = None;

        for _ in 0..batch_size {
            match rx.try_recv() {
                Ok(Ok(item)) => {
                    let SourceItem {
                        record,
                        cursor_advance,
                    } = item;
                    batch.push(record);
                    if !cursor_advance.is_none() {
                        last_batch_cursor = Some(cursor_advance);
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(mpsc::error::TryRecvError::Empty)
                | Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        // If we got records but no cursor advance, fall back to the
        // in-flight cursor so we still commit progress.
        let cursor_to_track = last_batch_cursor.or_else(|| self.in_flight_cursor.clone());
        self.in_flight_cursor = cursor_to_track.clone();

        if batch.is_empty() {
            return Ok((Vec::new(), None));
        }

        let batch_id = self.next_batch_id;
        self.next_batch_id += 1;
        if let Some(cursor) = cursor_to_track {
            self.pending_cursors.insert(batch_id, cursor);
        }
        Ok((batch, Some(batch_id)))
    }

    fn commit(&mut self, pipeline_name: &str, batch_id: i64) -> Result<(), String> {
        // Find the cursor for this batch_id (or the closest committed one).
        let cursor = self.pending_cursors.remove(&batch_id);
        // Prune any older batch_ids; they're implicitly committed too.
        self.pending_cursors.retain(|&id, _| id > batch_id);

        if let Some(c) = cursor {
            if !c.is_none() {
                self.persist_cursor(pipeline_name, &c)?;
            }
        }
        Ok(())
    }
}

impl Drop for AsyncSourceBridge {
    fn drop(&mut self) {
        // Close the receiver to signal the producer task to exit.
        self.receiver.take();
        // Detach the worker thread — it will exit when send() fails.
        if let Some(handle) = self.worker_thread.take() {
            let _ = handle.join();
        }
    }
}

// =============================================================================
// AsyncSinkBridge — wraps AsyncSink as a sync OutputConnector
// =============================================================================

/// Wraps an [`AsyncSink`] and exposes it as a sync [`OutputConnector`].
///
/// Each `write(&RecordBatch)` call sends the batch through a bounded
/// channel to the sink task; the call blocks until the send succeeds
/// (which provides natural backpressure when the sink is slow).
pub struct AsyncSinkBridge {
    sender: Option<mpsc::Sender<SinkCommand>>,
    worker_thread: Option<std::thread::JoinHandle<()>>,
    response_rx: Option<std::sync::mpsc::Receiver<Result<(), String>>>,
}

#[allow(dead_code)]
enum SinkCommand {
    Write(Vec<Value>),
    Flush, // reserved for future periodic-flush plumbing
}

impl AsyncSinkBridge {
    pub fn new(sink_factory: Box<dyn FnOnce() -> Box<dyn AsyncSink> + Send>) -> Self {
        let (tx, mut rx) = mpsc::channel::<SinkCommand>(DEFAULT_CHANNEL_CAPACITY);
        let (resp_tx, resp_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        let handle = std::thread::Builder::new()
            .name("pg_streaming_sink".to_string())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = resp_tx.send(Err(format!("Runtime build failed: {}", e)));
                        return;
                    }
                };
                rt.block_on(async move {
                    let mut sink = sink_factory();
                    while let Some(cmd) = rx.recv().await {
                        let result = match cmd {
                            SinkCommand::Write(records) => sink.write_batch(&records).await,
                            SinkCommand::Flush => sink.flush().await,
                        };
                        if resp_tx.send(result).is_err() {
                            return;
                        }
                    }
                    // Best-effort flush at shutdown.
                    let _ = sink.flush().await;
                });
            })
            .expect("Failed to spawn sink worker thread");

        Self {
            sender: Some(tx),
            worker_thread: Some(handle),
            response_rx: Some(resp_rx),
        }
    }
}

impl OutputConnector for AsyncSinkBridge {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| "AsyncSinkBridge: sender dropped".to_string())?;
        let response_rx = self
            .response_rx
            .as_ref()
            .ok_or_else(|| "AsyncSinkBridge: response receiver dropped".to_string())?;

        sender
            .blocking_send(SinkCommand::Write(records.clone()))
            .map_err(|e| format!("Failed to send batch to sink: {}", e))?;

        match response_rx.recv_timeout(Duration::from_secs(300)) {
            Ok(result) => result,
            Err(e) => Err(format!("Sink response timed out: {}", e)),
        }
    }
}

impl Drop for AsyncSinkBridge {
    fn drop(&mut self) {
        // Drop the sender — the recv loop will exit, then runtime drops.
        self.sender.take();
        if let Some(handle) = self.worker_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::sdk::SourceItem;
    use futures::stream::{self, BoxStream};
    use serde_json::json;

    /// Synthetic source: emits N records with Numeric cursors 1..=N, then ends.
    struct CountingSource {
        count: i64,
    }

    #[async_trait::async_trait]
    impl AsyncSource for CountingSource {
        async fn open(
            &mut self,
            _last_cursor: Cursor,
        ) -> Result<BoxStream<'static, Result<SourceItem, String>>, String> {
            let n = self.count;
            let items: Vec<Result<SourceItem, String>> = (1..=n)
                .map(|i| Ok(SourceItem::new(json!({"id": i}), Cursor::Numeric(i))))
                .collect();
            Ok(Box::pin(stream::iter(items)))
        }
    }

    /// Wait for the bridge to have at least one record buffered, polling
    /// non-blocking. Times out after 2 seconds.
    fn wait_for_records(bridge: &mut AsyncSourceBridge) -> bool {
        let start = std::time::Instant::now();
        loop {
            // Peek without consuming by checking the receiver state.
            if let Some(rx) = bridge.receiver.as_ref() {
                if !rx.is_empty() || rx.is_closed() {
                    return true;
                }
            } else {
                return false;
            }
            if start.elapsed() > Duration::from_secs(2) {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn bridge_drains_synthetic_source() {
        // Note: this test does NOT call initialize() because that touches SPI.
        // We construct the bridge bits directly.
        let mut bridge =
            AsyncSourceBridge::new("input", Box::new(|| Box::new(CountingSource { count: 5 })));

        // Manually initialize like initialize() does but without SPI cursor load.
        let factory = bridge.source_factory.take().unwrap();
        let (tx, rx) = mpsc::channel::<Result<SourceItem, String>>(DEFAULT_CHANNEL_CAPACITY);
        bridge.receiver = Some(rx);
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let mut source = factory();
                let mut stream = source.open(Cursor::None).await.unwrap();
                while let Some(item) = stream.next().await {
                    if tx.send(item).await.is_err() {
                        return;
                    }
                }
            });
        });
        bridge.worker_thread = Some(handle);
        bridge.initialized = true;

        // Wait for the producer to fill the channel.
        assert!(wait_for_records(&mut bridge), "no records produced");

        // Drain via poll().
        let (batch, batch_id) = bridge.poll(10).unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(batch[0]["id"], 1);
        assert_eq!(batch[4]["id"], 5);
        assert!(batch_id.is_some());

        // Pending cursor map should have one entry.
        assert_eq!(bridge.pending_cursors.len(), 1);
        match bridge.pending_cursors.get(&batch_id.unwrap()) {
            Some(Cursor::Numeric(n)) => assert_eq!(*n, 5),
            other => panic!("expected Numeric(5), got {:?}", other),
        }

        // Next poll: empty (source exhausted).
        let (batch2, batch_id2) = bridge.poll(10).unwrap();
        assert!(batch2.is_empty());
        assert!(batch_id2.is_none());
    }

    #[test]
    fn bridge_poll_respects_batch_size() {
        let mut bridge =
            AsyncSourceBridge::new("input", Box::new(|| Box::new(CountingSource { count: 10 })));

        let factory = bridge.source_factory.take().unwrap();
        let (tx, rx) = mpsc::channel::<Result<SourceItem, String>>(DEFAULT_CHANNEL_CAPACITY);
        bridge.receiver = Some(rx);
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let mut source = factory();
                let mut stream = source.open(Cursor::None).await.unwrap();
                while let Some(item) = stream.next().await {
                    if tx.send(item).await.is_err() {
                        return;
                    }
                }
            });
        });
        bridge.worker_thread = Some(handle);
        bridge.initialized = true;

        assert!(wait_for_records(&mut bridge));

        let (batch1, id1) = bridge.poll(3).unwrap();
        assert_eq!(batch1.len(), 3);
        assert!(id1.is_some());

        let (batch2, id2) = bridge.poll(3).unwrap();
        assert_eq!(batch2.len(), 3);
        assert!(id2.is_some());
        assert_ne!(id1, id2);
    }

    #[test]
    fn cursor_pruning_on_commit() {
        // Construct bridge state directly without spawning a real source.
        let mut bridge =
            AsyncSourceBridge::new("input", Box::new(|| Box::new(CountingSource { count: 0 })));
        // Pretend we emitted 3 batches.
        bridge.pending_cursors.insert(1, Cursor::Numeric(10));
        bridge.pending_cursors.insert(2, Cursor::Numeric(20));
        bridge.pending_cursors.insert(3, Cursor::Numeric(30));
        bridge.next_batch_id = 4;

        // Simulate the prune behavior of commit() without touching SPI.
        let batch_id = 2;
        let removed = bridge.pending_cursors.remove(&batch_id);
        bridge.pending_cursors.retain(|&id, _| id > batch_id);

        assert!(matches!(removed, Some(Cursor::Numeric(20))));
        // batch_id 1 should have been pruned, 3 retained.
        assert_eq!(bridge.pending_cursors.len(), 1);
        assert!(bridge.pending_cursors.contains_key(&3));
        assert!(!bridge.pending_cursors.contains_key(&1));
    }

    #[test]
    fn poll_before_initialize_errors() {
        let mut bridge =
            AsyncSourceBridge::new("input", Box::new(|| Box::new(CountingSource { count: 0 })));
        let err = bridge.poll(10).unwrap_err();
        assert!(err.contains("not initialized"));
    }
}
