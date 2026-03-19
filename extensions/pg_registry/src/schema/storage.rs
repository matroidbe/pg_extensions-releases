//! Schema storage operations via SPI

use pgrx::prelude::*;
use sha2::{Digest, Sha256};

/// Compute SHA256 fingerprint of a schema definition
fn compute_fingerprint(schema_def: &serde_json::Value) -> String {
    // Normalize JSON by serializing with sorted keys
    let normalized = serde_json::to_string(schema_def).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Get the next version for a subject
fn get_next_version(subject: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM pgregistry.schemas WHERE subject = $1",
        &[subject.into()],
    )
    .unwrap_or(Some(1))
    .unwrap_or(1)
}

/// Register a new schema
pub fn register_schema(subject: &str, schema_def: pgrx::JsonB, description: Option<&str>) -> i32 {
    let fingerprint = compute_fingerprint(&schema_def.0);
    let version = get_next_version(subject);

    // Check if this exact schema already exists for this subject
    let existing_id = Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgregistry.schemas WHERE subject = $1 AND fingerprint = $2",
        &[subject.into(), fingerprint.clone().into()],
    )
    .ok()
    .flatten();

    if let Some(id) = existing_id {
        return id;
    }

    // Insert new schema
    let schema_json = serde_json::to_string(&schema_def.0).expect("Failed to serialize schema");

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgregistry.schemas (subject, version, schema_def, fingerprint, description)
        VALUES ($1, $2, $3::jsonb, $4, $5)
        RETURNING id
        "#,
        &[
            subject.into(),
            version.into(),
            schema_json.into(),
            fingerprint.into(),
            description.into(),
        ],
    )
    .expect("Failed to insert schema")
    .expect("No ID returned from insert")
}

/// Get a schema by ID
pub fn get_schema(schema_id: i32) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT schema_def FROM pgregistry.schemas WHERE id = $1",
        &[schema_id.into()],
    )
    .ok()
    .flatten()
}

/// Get the latest schema for a subject - returns (schema_id, version, schema_def)
pub fn get_latest_schema(subject: &str) -> Option<pgrx::JsonB> {
    // Return just the schema_def for simplicity - the caller can query for more details
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT schema_def FROM pgregistry.schemas WHERE subject = $1 ORDER BY version DESC LIMIT 1",
        &[subject.into()],
    )
    .ok()
    .flatten()
}

/// Get the schema bound to a topic
pub fn get_topic_schema(topic_name: &str, schema_type: &str) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
        SELECT s.schema_def
        FROM pgregistry.topic_schemas ts
        JOIN pgregistry.schemas s ON ts.schema_id = s.id
        WHERE ts.topic_name = $1 AND ts.schema_type = $2
        "#,
        &[topic_name.into(), schema_type.into()],
    )
    .ok()
    .flatten()
}

/// Bind a schema to a topic
pub fn bind_schema_to_topic(
    topic_name: &str,
    schema_id: i32,
    schema_type: &str,
    validation_mode: &str,
) -> i32 {
    // Validate schema_type
    if schema_type != "key" && schema_type != "value" {
        pgrx::error!(
            "schema_type must be 'key' or 'value', got '{}'",
            schema_type
        );
    }

    // Validate validation_mode
    if validation_mode != "STRICT" && validation_mode != "LOG" {
        pgrx::error!(
            "validation_mode must be 'STRICT' or 'LOG', got '{}'",
            validation_mode
        );
    }

    // Verify schema exists
    let schema_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgregistry.schemas WHERE id = $1)",
        &[schema_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false);

    if !schema_exists {
        pgrx::error!("Schema with id {} does not exist", schema_id);
    }

    // Upsert binding
    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgregistry.topic_schemas (topic_name, schema_type, schema_id, validation_mode)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (topic_name, schema_type)
        DO UPDATE SET schema_id = EXCLUDED.schema_id, validation_mode = EXCLUDED.validation_mode
        RETURNING id
        "#,
        &[
            topic_name.into(),
            schema_type.into(),
            schema_id.into(),
            validation_mode.into(),
        ],
    )
    .expect("Failed to bind schema to topic")
    .expect("No ID returned from upsert")
}

/// Unbind a schema from a topic
pub fn unbind_schema_from_topic(topic_name: &str, schema_type: &str) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgregistry.topic_schemas
            WHERE topic_name = $1 AND schema_type = $2
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted
        "#,
        &[topic_name.into(), schema_type.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0);

    deleted > 0
}

/// Drop a schema by ID
pub fn drop_schema(schema_id: i32) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgregistry.schemas
            WHERE id = $1
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted
        "#,
        &[schema_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0);

    deleted > 0
}

// ============================================================================
// Table-Topic Binding Functions
// ============================================================================

/// Detect the primary key column of a table
fn detect_primary_key(table_name: &str) -> Option<String> {
    // Parse schema.table
    let (schema, table) = if let Some(dot_pos) = table_name.find('.') {
        (&table_name[..dot_pos], &table_name[dot_pos + 1..])
    } else {
        ("public", table_name)
    };

    // Cast attname to text because it's of type 'name' which isn't compatible with String
    Spi::get_one_with_args::<String>(
        r#"
        SELECT a.attname::text
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass
          AND i.indisprimary
        LIMIT 1
        "#,
        &[schema.into(), table.into()],
    )
    .ok()
    .flatten()
}

/// Detect the auto-increment (SERIAL/BIGSERIAL) column of a table
fn detect_serial_column(table_name: &str) -> Option<String> {
    // Parse schema.table
    let (schema, table) = if let Some(dot_pos) = table_name.find('.') {
        (&table_name[..dot_pos], &table_name[dot_pos + 1..])
    } else {
        ("public", table_name)
    };

    // Look for columns with a default that references a sequence
    // Cast to text because information_schema uses sql_identifier type
    Spi::get_one_with_args::<String>(
        r#"
        SELECT column_name::text
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
          AND column_default LIKE 'nextval%'
        ORDER BY ordinal_position
        LIMIT 1
        "#,
        &[schema.into(), table.into()],
    )
    .ok()
    .flatten()
}

/// Get the schema_id for a table if there's an existing schema binding
fn get_schema_id_for_table(table_name: &str) -> Option<i32> {
    // Check if there's a schema registered with a subject matching the table name
    // Convention: subject = "tablename-value"
    let subject = format!(
        "{}-value",
        table_name.rsplit('.').next().unwrap_or(table_name)
    );

    Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgregistry.schemas WHERE subject = $1 ORDER BY version DESC LIMIT 1",
        &[subject.into()],
    )
    .ok()
    .flatten()
}

/// Check if pg_kafka extension is installed
fn is_pg_kafka_installed() -> bool {
    Spi::get_one::<bool>("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_kafka')")
        .ok()
        .flatten()
        .unwrap_or(false)
}

/// Configure pg_kafka topic for table binding
fn configure_pg_kafka_topic(
    table_name: &str,
    kafka_topic: &str,
    mode: &str,
    offset_column: &Option<String>,
    key_column: &Option<String>,
    kafka_offset_column: &Option<String>,
) {
    if !is_pg_kafka_installed() {
        pgrx::notice!(
            "pg_kafka extension not installed. Install it to enable Kafka protocol access:\n\
             CREATE EXTENSION pg_kafka;"
        );
        return;
    }

    // Check if topic already exists in pg_kafka
    let topic_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgkafka.topics WHERE name = $1)",
        &[kafka_topic.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false);

    if topic_exists {
        // Topic exists - enable writes on it
        let offset_col = offset_column.as_deref().unwrap_or("id");

        // First update the topic to be source-backed if it isn't already
        let _ = Spi::run_with_args(
            r#"
            UPDATE pgkafka.topics
            SET source_table = $2,
                offset_column = $3,
                value_expr = 'row_to_json(' || split_part($2, '.', 2) || ')::text'
            WHERE name = $1 AND source_table IS NULL
            "#,
            &[kafka_topic.into(), table_name.into(), offset_col.into()],
        );

        // Then enable writes
        if mode == "stream" {
            let _ = Spi::run_with_args(
                "SELECT pgkafka.enable_topic_writes($1, 'stream', NULL, NULL)",
                &[kafka_topic.into()],
            );
        } else {
            let key_col = key_column.as_deref();
            let offset_col = kafka_offset_column.as_deref();
            let _ = Spi::run_with_args(
                "SELECT pgkafka.enable_topic_writes($1, 'table', $2, $3)",
                &[kafka_topic.into(), key_col.into(), offset_col.into()],
            );
        }

        pgrx::notice!(
            "Enabled writes on existing pg_kafka topic '{}'",
            kafka_topic
        );
    } else {
        // Create new topic from table
        // Both modes need an integer offset column with UNIQUE constraint
        // For stream mode: use the detected serial column
        // For table mode: default to "id" (tables need an id BIGSERIAL UNIQUE column)
        let offset_col = offset_column.as_deref().unwrap_or("id");

        // Create topic from table with row_to_json as the value expression
        let _ = Spi::run_with_args(
            r#"
            SELECT pgkafka.create_topic_from_table(
                $1,
                $2,
                $3,
                NULL,
                'row_to_json(' || split_part($2, '.', 2) || ')::text'
            )
            "#,
            &[kafka_topic.into(), table_name.into(), offset_col.into()],
        );

        // Enable writes on the new topic
        if mode == "stream" {
            let _ = Spi::run_with_args(
                "SELECT pgkafka.enable_topic_writes($1, 'stream', NULL, NULL)",
                &[kafka_topic.into()],
            );
        } else {
            let key_col = key_column.as_deref();
            let offset_col = kafka_offset_column.as_deref();
            let _ = Spi::run_with_args(
                "SELECT pgkafka.enable_topic_writes($1, 'table', $2, $3)",
                &[kafka_topic.into(), key_col.into(), offset_col.into()],
            );
        }

        pgrx::notice!(
            "Created pg_kafka topic '{}' from table '{}' with {} mode",
            kafka_topic,
            table_name,
            mode
        );
    }
}

/// Bind a table to a Kafka topic
pub fn bind_table_to_topic(table_name: &str, kafka_topic: &str, mode: &str) -> i32 {
    // Validate mode
    if mode != "stream" && mode != "table" {
        pgrx::error!("mode must be 'stream' or 'table', got '{}'", mode);
    }

    // Verify table exists
    let (schema, table) = if let Some(dot_pos) = table_name.find('.') {
        (&table_name[..dot_pos], &table_name[dot_pos + 1..])
    } else {
        ("public", table_name)
    };

    let table_exists = Spi::get_one_with_args::<bool>(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        "#,
        &[schema.into(), table.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false);

    if !table_exists {
        pgrx::error!("Table '{}' does not exist", table_name);
    }

    // Detect columns based on mode
    let (key_column, offset_column, kafka_offset_column) = if mode == "stream" {
        // Stream mode: need an auto-increment column for offsets
        let offset_col =
            detect_serial_column(table_name).or_else(|| detect_primary_key(table_name));
        if offset_col.is_none() {
            pgrx::error!(
                "Table '{}' has no SERIAL/BIGSERIAL column for stream mode. \
                 Add an auto-increment column or use a table with one.",
                table_name
            );
        }
        (None, offset_col, None)
    } else {
        // Table mode: need a primary key for upserts
        let pk = detect_primary_key(table_name);
        if pk.is_none() {
            pgrx::error!(
                "Table '{}' has no PRIMARY KEY for table (upsert) mode. \
                 Add a primary key constraint.",
                table_name
            );
        }

        // For table mode, we need a kafka_offset column
        // Check if there's a kafka_offset column, if not suggest creating one
        let has_kafka_offset = Spi::get_one_with_args::<bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = 'kafka_offset'
            )
            "#,
            &[schema.into(), table.into()],
        )
        .ok()
        .flatten()
        .unwrap_or(false);

        let kafka_offset_col = if has_kafka_offset {
            Some("kafka_offset".to_string())
        } else {
            pgrx::warning!(
                "Table '{}' has no 'kafka_offset' column. Consider adding one: \
                 ALTER TABLE {} ADD COLUMN kafka_offset BIGINT;",
                table_name,
                table_name
            );
            None
        };

        (pk, None, kafka_offset_col)
    };

    // Try to find an existing schema for this table
    let schema_id = get_schema_id_for_table(table_name);

    // Insert binding
    let binding_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgregistry.table_topic_bindings
            (table_name, schema_id, kafka_topic, mode, key_column, offset_column, kafka_offset_column)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (table_name)
        DO UPDATE SET
            schema_id = EXCLUDED.schema_id,
            kafka_topic = EXCLUDED.kafka_topic,
            mode = EXCLUDED.mode,
            key_column = EXCLUDED.key_column,
            offset_column = EXCLUDED.offset_column,
            kafka_offset_column = EXCLUDED.kafka_offset_column
        RETURNING id
        "#,
        &[
            table_name.into(),
            schema_id.into(),
            kafka_topic.into(),
            mode.into(),
            key_column.clone().into(),
            offset_column.clone().into(),
            kafka_offset_column.clone().into(),
        ],
    )
    .expect("Failed to bind table to topic")
    .expect("No ID returned from upsert");

    // Configure pg_kafka if installed
    configure_pg_kafka_topic(
        table_name,
        kafka_topic,
        mode,
        &offset_column,
        &key_column,
        &kafka_offset_column,
    );

    binding_id
}

/// Unbind a table from its Kafka topic
pub fn unbind_table_from_topic(table_name: &str) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgregistry.table_topic_bindings
            WHERE table_name = $1
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted
        "#,
        &[table_name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0);

    deleted > 0
}

/// Get binding info for a table
pub fn get_table_binding(table_name: &str) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
        SELECT jsonb_build_object(
            'id', id,
            'table_name', table_name,
            'schema_id', schema_id,
            'kafka_topic', kafka_topic,
            'mode', mode,
            'key_column', key_column,
            'offset_column', offset_column,
            'kafka_offset_column', kafka_offset_column,
            'created_at', created_at
        )
        FROM pgregistry.table_topic_bindings
        WHERE table_name = $1
        "#,
        &[table_name.into()],
    )
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    // Only import the pure functions we're testing - not the Spi-dependent ones
    use super::compute_fingerprint;

    #[test]
    fn test_compute_fingerprint_deterministic() {
        let schema1 = serde_json::json!({"type": "string"});
        let schema2 = serde_json::json!({"type": "string"});

        assert_eq!(compute_fingerprint(&schema1), compute_fingerprint(&schema2));
    }

    #[test]
    fn test_compute_fingerprint_different_for_different_schemas() {
        let schema1 = serde_json::json!({"type": "string"});
        let schema2 = serde_json::json!({"type": "integer"});

        assert_ne!(compute_fingerprint(&schema1), compute_fingerprint(&schema2));
    }
}
