//! Async-safe bridge between Tokio tasks and PostgreSQL SPI
//!
//! Provides channel-based communication for executing SPI queries from async contexts.

use crate::types::{ColumnType, SpiError, SpiParam, SpiResult};
use tokio::sync::{mpsc, oneshot};

/// A query request sent to the SPI executor
pub struct SpiRequest {
    /// The SQL query to execute
    pub query: String,
    /// Query parameters
    pub params: Vec<SpiParam>,
    /// Column types to extract (determines how to read each column)
    pub column_types: Vec<ColumnType>,
    /// Channel to send the response
    pub response_tx: oneshot::Sender<Result<SpiResult, SpiError>>,
}

/// Handle for tokio tasks to send queries to the SPI executor
///
/// This is the async-safe side of the bridge. Clone and share across tasks.
#[derive(Clone)]
pub struct SpiBridge {
    tx: mpsc::Sender<SpiRequest>,
}

impl SpiBridge {
    /// Create a new SPI bridge with the given channel capacity
    ///
    /// Returns both the bridge (for async tasks) and receiver (for SPI executor).
    pub fn new(capacity: usize) -> (Self, SpiReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        (SpiBridge { tx }, SpiReceiver { rx })
    }

    /// Execute a query and return results
    pub async fn query(
        &self,
        sql: &str,
        params: Vec<SpiParam>,
        column_types: Vec<ColumnType>,
    ) -> Result<SpiResult, SpiError> {
        let (response_tx, response_rx) = oneshot::channel();

        let request = SpiRequest {
            query: sql.to_string(),
            params,
            column_types,
            response_tx,
        };

        self.tx
            .send(request)
            .await
            .map_err(|_| SpiError::ChannelClosed)?;

        response_rx.await.map_err(|_| SpiError::ChannelClosed)?
    }

    /// Execute a query that returns a single i64 value
    pub async fn query_one_i64(&self, sql: &str, params: Vec<SpiParam>) -> Result<i64, SpiError> {
        let result = self.query(sql, params, vec![ColumnType::Int64]).await?;
        result.first().ok_or(SpiError::NoRows)?.get_i64(0)
    }

    /// Execute a query that returns a single i32 value
    pub async fn query_one_i32(&self, sql: &str, params: Vec<SpiParam>) -> Result<i32, SpiError> {
        let result = self.query(sql, params, vec![ColumnType::Int32]).await?;
        result.first().ok_or(SpiError::NoRows)?.get_i32(0)
    }

    /// Execute a query that returns a single string value
    pub async fn query_one_string(
        &self,
        sql: &str,
        params: Vec<SpiParam>,
    ) -> Result<Option<String>, SpiError> {
        let result = self.query(sql, params, vec![ColumnType::Text]).await?;
        Ok(result.first().and_then(|r| r.get_string(0)))
    }

    /// Execute a query that doesn't return results
    pub async fn execute(&self, sql: &str, params: Vec<SpiParam>) -> Result<(), SpiError> {
        self.query(sql, params, vec![]).await?;
        Ok(())
    }
}

/// Receiver side of the SPI bridge, used by the executor
///
/// This runs on the background worker's main thread where SPI access is valid.
pub struct SpiReceiver {
    rx: mpsc::Receiver<SpiRequest>,
}

impl SpiReceiver {
    /// Try to receive a request without blocking
    pub fn try_recv(&mut self) -> Option<SpiRequest> {
        self.rx.try_recv().ok()
    }

    /// Check if there are pending requests (for polling)
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

/// Create a new SpiBridge and SpiReceiver pair
///
/// Convenience function that calls SpiBridge::new().
pub fn create_bridge(capacity: usize) -> (SpiBridge, SpiReceiver) {
    SpiBridge::new(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SpiRow, SpiValue};

    #[tokio::test]
    async fn test_bridge_creation() {
        let (bridge, _receiver) = SpiBridge::new(10);
        // Bridge should be cloneable
        let _bridge2 = bridge.clone();
    }

    #[tokio::test]
    async fn test_create_bridge_function() {
        let (_bridge, _receiver) = create_bridge(10);
    }

    #[tokio::test]
    async fn test_receiver_is_empty() {
        let (_bridge, receiver) = SpiBridge::new(10);
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn test_query_channel_closed() {
        let (bridge, receiver) = SpiBridge::new(10);
        // Drop receiver to close channel
        drop(receiver);

        let result = bridge
            .query("SELECT 1", vec![], vec![ColumnType::Int32])
            .await;
        assert!(matches!(result, Err(SpiError::ChannelClosed)));
    }

    #[tokio::test]
    async fn test_request_sent_to_receiver() {
        let (bridge, mut receiver) = SpiBridge::new(10);

        // Spawn task to send query
        let handle = tokio::spawn(async move {
            // This will block waiting for response, but we'll timeout
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                bridge.query("SELECT 1", vec![], vec![ColumnType::Int32]),
            )
            .await
        });

        // Give time for message to be sent
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Should receive the request
        let request = receiver.try_recv();
        assert!(request.is_some());
        let request = request.unwrap();
        assert_eq!(request.query, "SELECT 1");

        // Send a response
        let result = SpiResult {
            rows: vec![SpiRow {
                columns: vec![SpiValue::Int32(1)],
            }],
        };
        let _ = request.response_tx.send(Ok(result));

        // Task should complete
        let _ = handle.await;
    }
}
