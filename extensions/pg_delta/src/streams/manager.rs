//! Stream manager - coordinates all active streams.

use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use super::export::ExportStream;
use super::ingest::IngestStream;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Stream configuration from database.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields populated from DB, not all used yet
pub struct StreamConfig {
    pub id: i32,
    pub name: String,
    pub direction: String,
    pub uri: String,
    pub pg_table: String,
    pub mode: String,
    pub poll_interval_ms: i32,
    pub batch_size: i32,
    pub flush_interval_ms: i32,
    pub start_version: Option<i64>,
    pub tracking_column: Option<String>,
    pub key_columns: Option<Vec<String>>,
    pub storage_options: Option<serde_json::Value>,
}

/// Stream state tracking.
#[derive(Debug)]
pub struct StreamState {
    pub config: StreamConfig,
    pub last_poll: Option<Instant>,
    pub last_error: Option<String>,
    pub error_count: i32,
}

/// Main stream manager.
pub struct StreamManager {
    streams: HashMap<String, StreamState>,
    /// Tokio runtime - created lazily only when needed
    runtime: Option<tokio::runtime::Runtime>,
}

impl StreamManager {
    pub fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Don't create tokio runtime yet - create it lazily when first stream is processed
        // This avoids shutdown issues when there are no streams
        Ok(Self {
            streams: HashMap::new(),
            runtime: None,
        })
    }

    /// Get or create the tokio runtime.
    fn get_runtime(
        &mut self,
    ) -> Result<&tokio::runtime::Runtime, Box<dyn std::error::Error + Send + Sync>> {
        if self.runtime.is_none() {
            log!("pg_delta: creating tokio runtime");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            self.runtime = Some(runtime);
        }
        Ok(self.runtime.as_ref().unwrap())
    }

    /// Shutdown the runtime gracefully.
    pub fn shutdown(mut self) {
        if let Some(runtime) = self.runtime.take() {
            log!("pg_delta: shutting down tokio runtime");
            // Shutdown with a short timeout
            runtime.shutdown_timeout(Duration::from_secs(2));
            log!("pg_delta: tokio runtime shut down");
        } else {
            log!("pg_delta: no tokio runtime to shut down");
        }
    }

    /// Load stream configurations from database.
    pub fn load_streams(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let configs = load_stream_configs()?;

        for config in configs {
            if !self.streams.contains_key(&config.name) {
                log!("pg_delta: loaded stream '{}'", config.name);
                self.streams.insert(
                    config.name.clone(),
                    StreamState {
                        config,
                        last_poll: None,
                        last_error: None,
                        error_count: 0,
                    },
                );
            }
        }

        Ok(())
    }

    /// Reload stream configurations (called on SIGHUP).
    pub fn reload_configs(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let configs = load_stream_configs()?;
        let config_names: std::collections::HashSet<String> =
            configs.iter().map(|c| c.name.clone()).collect();

        // Remove streams that no longer exist
        self.streams.retain(|name, _| {
            if !config_names.contains(name) {
                log!("pg_delta: removed stream '{}'", name);
                false
            } else {
                true
            }
        });

        // Add or update streams
        for config in configs {
            if let Some(state) = self.streams.get_mut(&config.name) {
                state.config = config;
            } else {
                log!("pg_delta: added stream '{}'", config.name);
                self.streams.insert(
                    config.name.clone(),
                    StreamState {
                        config,
                        last_poll: None,
                        last_error: None,
                        error_count: 0,
                    },
                );
            }
        }

        Ok(())
    }

    /// Process one tick of all streams.
    pub fn tick(&mut self) {
        // Check for shutdown before doing any work
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            return;
        }

        let now = Instant::now();

        // First pass: collect configs for streams that need polling
        let streams_to_poll: Vec<(String, StreamConfig)> = self
            .streams
            .iter_mut()
            .filter_map(|(name, state)| {
                let poll_interval = Duration::from_millis(state.config.poll_interval_ms as u64);
                let should_poll = state
                    .last_poll
                    .is_none_or(|last| now.duration_since(last) >= poll_interval);

                if should_poll {
                    state.last_poll = Some(now);
                    Some((name.clone(), state.config.clone()))
                } else {
                    None
                }
            })
            .collect();

        // If no streams to poll, return early (no need to create runtime)
        if streams_to_poll.is_empty() {
            return;
        }

        // Ensure runtime exists before processing streams
        if let Err(e) = self.get_runtime() {
            log!("pg_delta: failed to create runtime: {}", e);
            return;
        }

        // Collect results first, then update state (to avoid borrow issues)
        let mut results: Vec<(String, Result<i64, String>)> = Vec::new();

        // Second pass: process each stream
        for (name, config) in streams_to_poll {
            // Check for shutdown between each stream
            if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                log!("pg_delta: shutdown requested, stopping stream processing");
                return;
            }

            // Process the stream using the runtime
            let result = {
                let runtime = self.runtime.as_ref().unwrap();
                match config.direction.as_str() {
                    "ingest" => {
                        let ingest = IngestStream::new(config.clone());
                        runtime.block_on(async {
                            tokio::time::timeout(Duration::from_secs(30), ingest.poll())
                                .await
                                .map_err(|_| "ingest poll timeout".to_string())?
                                .map_err(|e| e.to_string())
                        })
                    }
                    "export" => {
                        let export = ExportStream::new(config.clone());
                        runtime.block_on(async {
                            tokio::time::timeout(Duration::from_secs(30), export.poll())
                                .await
                                .map_err(|_| "export poll timeout".to_string())?
                                .map_err(|e| e.to_string())
                        })
                    }
                    _ => {
                        log!("pg_delta: unknown direction for stream '{}'", name);
                        continue;
                    }
                }
            };

            results.push((name, result));
        }

        // Third pass: update state based on results
        for (name, result) in results {
            if let Some(state) = self.streams.get_mut(&name) {
                match result {
                    Ok(rows) => {
                        if rows > 0 {
                            log!("pg_delta: stream '{}' processed {} rows", name, rows);
                        }
                        state.last_error = None;
                        state.error_count = 0;

                        // Update progress in database
                        if let Err(e) = update_stream_progress(&name, rows) {
                            log!("pg_delta: failed to update progress for '{}': {}", name, e);
                        }
                    }
                    Err(e) => {
                        state.error_count += 1;
                        state.last_error = Some(e.clone());
                        log!(
                            "pg_delta: stream '{}' error (count={}): {}",
                            name,
                            state.error_count,
                            e
                        );

                        // Record error in database
                        if let Err(e2) = record_stream_error(&name, &e) {
                            log!("pg_delta: failed to record error: {}", e2);
                        }

                        // If too many errors, stop the stream
                        if state.error_count >= 10 {
                            log!(
                                "pg_delta: stopping stream '{}' due to repeated errors",
                                name
                            );
                            if let Err(e) = stop_stream_in_db(&name) {
                                log!("pg_delta: failed to stop stream: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Run the stream manager main loop.
pub fn run_stream_manager() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut manager = StreamManager::new()?;

    // Initial load of streams
    manager.load_streams()?;

    log!(
        "pg_delta: stream manager running with {} streams",
        manager.streams.len()
    );

    // Main loop - keep it simple and responsive to shutdown
    // Use std::thread::sleep instead of BackgroundWorker::wait_latch
    // because wait_latch may not return immediately on SIGTERM
    loop {
        // Check for shutdown FIRST before doing any work
        if BackgroundWorker::sigterm_received() {
            log!("pg_delta: SIGTERM received, initiating shutdown");
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
            break;
        }

        // Also check our own shutdown flag
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            log!("pg_delta: shutdown flag set, exiting");
            break;
        }

        // Check for config reload
        if BackgroundWorker::sighup_received() {
            log!("pg_delta: received SIGHUP, reloading configuration");
            if let Err(e) = manager.reload_configs() {
                log!("pg_delta: failed to reload configs: {}", e);
            }
        }

        // Process streams (only if not shutting down)
        if !SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
            manager.tick();
        }

        // Small sleep to avoid busy-waiting
        // This also allows checking for shutdown signals frequently
        std::thread::sleep(Duration::from_millis(100));
    }

    log!("pg_delta: main loop exited, cleaning up");

    // Explicitly shutdown the tokio runtime to release resources
    manager.shutdown();

    log!("pg_delta: cleanup complete");

    Ok(())
}

// =============================================================================
// Database Operations
// =============================================================================

/// Load stream configurations from delta.streams table.
fn load_stream_configs() -> Result<Vec<StreamConfig>, Box<dyn std::error::Error + Send + Sync>> {
    let mut configs = Vec::new();

    // Use BackgroundWorker::transaction to ensure we have a proper transaction context
    // This is required for SPI operations in background workers
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        // First check if the delta schema and streams table exist
        let table_exists = Spi::connect(|client| {
            let result = client.select(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'delta' AND table_name = 'streams'
                )
                "#,
                None,
                &[],
            )?;
            let exists = result.first().get::<bool>(1)?.unwrap_or(false);
            Ok::<bool, pgrx::spi::Error>(exists)
        });

        // If table doesn't exist (extension not created yet), return empty list
        match table_exists {
            Ok(false) => return,
            Err(_) => return, // Treat any error as "not ready yet"
            Ok(true) => {}    // Continue with loading
        }

        let load_result = Spi::connect(|client| {
            let result = client.select(
                r#"
                SELECT id, name, direction, uri, pg_table, mode,
                       COALESCE(poll_interval_ms, 10000) as poll_interval_ms,
                       COALESCE(batch_size, 10000) as batch_size,
                       COALESCE(flush_interval_ms, 60000) as flush_interval_ms,
                       start_version, tracking_column, key_columns,
                       storage_options
                FROM delta.streams
                WHERE enabled = true AND status = 'running'
                "#,
                None,
                &[],
            )?;

            for row in result {
                // Columns: id, name, direction, uri, pg_table, mode, poll_interval_ms, batch_size, flush_interval_ms, start_version, tracking_column, storage_options
                let config = StreamConfig {
                    id: row.get::<i32>(1)?.unwrap_or(0),
                    name: row.get::<String>(2)?.unwrap_or_default(),
                    direction: row.get::<String>(3)?.unwrap_or_default(),
                    uri: row.get::<String>(4)?.unwrap_or_default(),
                    pg_table: row.get::<String>(5)?.unwrap_or_default(),
                    mode: row.get::<String>(6)?.unwrap_or_default(),
                    poll_interval_ms: row.get::<i32>(7)?.unwrap_or(10000),
                    batch_size: row.get::<i32>(8)?.unwrap_or(10000),
                    flush_interval_ms: row.get::<i32>(9)?.unwrap_or(60000),
                    start_version: row.get::<i64>(10)?,
                    tracking_column: row.get::<String>(11)?,
                    key_columns: None, // TODO: Parse from array
                    storage_options: row.get::<pgrx::JsonB>(12)?.map(|j| j.0),
                };
                configs.push(config);
            }

            Ok::<(), pgrx::spi::Error>(())
        });

        if let Err(e) = load_result {
            log!("pg_delta: failed to load stream configs: {:?}", e);
        }
    }));

    Ok(configs)
}

/// Update stream progress in database.
fn update_stream_progress(stream_name: &str, rows_synced: i64) -> Result<(), String> {
    let stream_name = stream_name.to_string();
    let mut result = Ok(());

    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        result = Spi::run_with_args(
            r#"
            INSERT INTO delta.stream_progress (stream_id, rows_synced, last_sync_at)
            SELECT id, $2, now()
            FROM delta.streams WHERE name = $1
            ON CONFLICT (stream_id) DO UPDATE SET
                rows_synced = delta.stream_progress.rows_synced + $2,
                last_sync_at = now()
            "#,
            &[stream_name.clone().into(), rows_synced.into()],
        )
        .map_err(|e| format!("Failed to update progress: {:?}", e));
    }));

    result
}

/// Record stream error in database.
fn record_stream_error(stream_name: &str, error_message: &str) -> Result<(), String> {
    let stream_name = stream_name.to_string();
    let error_message = error_message.to_string();
    let mut result = Ok(());

    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        if let Err(e) = Spi::run_with_args(
            r#"
            INSERT INTO delta.stream_errors (stream_id, error_type, error_message)
            SELECT id, 'runtime', $2
            FROM delta.streams WHERE name = $1
            "#,
            &[stream_name.clone().into(), error_message.clone().into()],
        ) {
            result = Err(format!("Failed to record error: {:?}", e));
            return;
        }

        // Update stream status
        if let Err(e) = Spi::run_with_args(
            r#"
            UPDATE delta.streams
            SET error_message = $2
            WHERE name = $1
            "#,
            &[stream_name.clone().into(), error_message.clone().into()],
        ) {
            result = Err(format!("Failed to update stream status: {:?}", e));
        }
    }));

    result
}

/// Stop a stream in the database.
fn stop_stream_in_db(stream_name: &str) -> Result<(), String> {
    let stream_name = stream_name.to_string();
    let mut result = Ok(());

    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        result = Spi::run_with_args(
            "UPDATE delta.streams SET status = 'error', enabled = false WHERE name = $1",
            &[stream_name.clone().into()],
        )
        .map_err(|e| format!("Failed to stop stream: {:?}", e));
    }));

    result
}
