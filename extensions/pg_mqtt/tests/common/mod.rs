//! Test harness utilities for pg_mqtt integration tests
//!
//! Provides server readiness detection, graceful skipping, and SQL execution helpers.

use std::net::TcpStream;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Once;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

/// Default MQTT server address
pub const MQTT_ADDR: &str = "127.0.0.1:1883";

/// Default PostgreSQL connection parameters for pgrx-managed instance
pub const PG_HOST: &str = "localhost";
pub const PG_PORT: u16 = 28816;
pub const PG_DB: &str = "pg_mqtt";

/// Check if server is currently accepting connections
pub fn is_server_running(addr: &str) -> bool {
    TcpStream::connect(addr).is_ok()
}

// Broker readiness state: 0 = unknown, 1 = ready, 2 = not ready
static BROKER_READY: AtomicU8 = AtomicU8::new(0);
static BROKER_CHECK: Once = Once::new();

/// Check broker readiness (cached after first call)
pub fn ensure_broker_ready(addr: &str) -> bool {
    BROKER_CHECK.call_once(|| {
        let ready = wait_for_broker_ready(addr, Duration::from_secs(15));
        BROKER_READY.store(if ready { 1 } else { 2 }, Ordering::SeqCst);
    });
    BROKER_READY.load(Ordering::SeqCst) == 1
}

/// Wait for the MQTT broker to be fully ready (TCP + MQTT protocol level)
///
/// The broker may accept TCP connections before it's ready to handle MQTT packets
/// (SPI bridge needs to initialize). This helper attempts actual MQTT CONNECT
/// handshakes with retries to ensure the broker is fully operational.
fn wait_for_broker_ready(addr: &str, timeout: Duration) -> bool {
    use std::io::{Read as _, Write as _};
    let start = Instant::now();

    eprintln!("Waiting for MQTT broker to be fully ready at {}...", addr);

    while start.elapsed() < timeout {
        if let Ok(mut stream) = TcpStream::connect(addr) {
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let _ = stream.set_write_timeout(Some(Duration::from_secs(2)));

            // Send a minimal MQTT 5.0 CONNECT packet
            let connect_packet: Vec<u8> = vec![
                0x10, // CONNECT packet type
                0x15, // Remaining length = 21
                // Variable header
                0x00, 0x04, b'M', b'Q', b'T', b'T', // Protocol Name
                0x05, // Protocol Version 5
                0x02, // Connect Flags (clean start)
                0x00, 0x3C, // Keep Alive = 60s
                0x00, // Properties length = 0
                // Payload: Client ID
                0x00, 0x08, // Client ID length = 8
                b'_', b'w', b'a', b'r', b'm', b'u', b'p', b'_',
            ];

            if stream.write_all(&connect_packet).is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = stream.read(&mut buf) {
                    // Check for CONNACK (0x20)
                    if n >= 2 && buf[0] == 0x20 {
                        // Send DISCONNECT
                        let _ = stream.write_all(&[0xE0, 0x02, 0x00, 0x00]);
                        eprintln!(
                            "MQTT broker ready after {:.1}s",
                            start.elapsed().as_secs_f64()
                        );
                        return true;
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    eprintln!("MQTT broker not ready after {:?}", timeout);
    false
}

/// Create a tokio runtime for async operations
fn runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

/// Execute SQL against the pgrx-managed PostgreSQL instance and return the first column of the first row
pub fn run_sql(sql: &str) -> Result<String, String> {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, PG_PORT, PG_DB);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error: {}", e))?;

        if rows.is_empty() {
            Ok(String::new())
        } else {
            // Return first column of first row as string
            let row = &rows[0];
            let val: String = if let Ok(v) = row.try_get::<_, i64>(0) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, i32>(0) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, bool>(0) {
                v.to_string()
            } else if let Ok(v) = row.try_get::<_, String>(0) {
                v
            } else {
                String::new()
            };
            Ok(val)
        }
    })
}

/// Execute SQL without returning results
pub fn execute_sql(sql: &str) -> Result<(), String> {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, PG_PORT, PG_DB);
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        client
            .execute(sql, &[])
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        Ok(())
    })
}

/// Macro to skip test if server is not running
#[macro_export]
macro_rules! skip_if_no_server {
    ($addr:expr) => {
        if !$crate::common::is_server_running($addr) {
            eprintln!("SKIPPED: pg_mqtt server not running at {}", $addr);
            return;
        }
        // Ensure broker is fully ready (not just TCP-accepting).
        // This is cached via std::sync::Once so only the first test pays the wait cost.
        if !$crate::common::ensure_broker_ready($addr) {
            eprintln!("SKIPPED: pg_mqtt broker not fully ready at {}", $addr);
            return;
        }
    };
}
