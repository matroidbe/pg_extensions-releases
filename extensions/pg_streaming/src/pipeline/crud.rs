//! Pipeline CRUD operations

use crate::dsl::parse::parse_pipeline_value;
use crate::dsl::validate::validate_definition;
use pgrx::prelude::*;

/// Create a new pipeline from a JSON definition
pub fn create_pipeline_impl(name: &str, definition: pgrx::JsonB) -> i32 {
    if name.is_empty() {
        pgrx::error!("Pipeline name cannot be empty");
    }

    // Parse and validate the definition
    let def = match parse_pipeline_value(&definition.0) {
        Ok(d) => d,
        Err(e) => pgrx::error!("{}", e),
    };

    if let Err(e) = validate_definition(&def) {
        pgrx::error!("Validation failed: {}", e);
    }

    // Insert into pipelines table
    let pipeline_id = Spi::get_one_with_args::<i32>(
        "INSERT INTO pgstreams.pipelines (name, definition) VALUES ($1, $2) RETURNING id",
        &[name.into(), definition.into()],
    );

    let pipeline_id = match pipeline_id {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to create pipeline: no ID returned"),
        Err(e) => pgrx::error!("Failed to create pipeline: {}", e),
    };

    // Insert initial version
    let _ = Spi::run_with_args(
        "INSERT INTO pgstreams.pipeline_versions (pipeline_id, version, definition) \
         SELECT $1, 1, definition FROM pgstreams.pipelines WHERE id = $1",
        &[pipeline_id.into()],
    );

    pipeline_id
}

/// Update a pipeline definition (stops and restarts if running)
pub fn update_pipeline_impl(name: &str, definition: pgrx::JsonB) {
    // Parse and validate the new definition
    let def = match parse_pipeline_value(&definition.0) {
        Ok(d) => d,
        Err(e) => pgrx::error!("{}", e),
    };

    if let Err(e) = validate_definition(&def) {
        pgrx::error!("Validation failed: {}", e);
    }

    // Get pipeline id and current state
    let row = Spi::get_two_with_args::<i32, String>(
        "SELECT id, state FROM pgstreams.pipelines WHERE name = $1",
        &[name.into()],
    );

    let (pipeline_id, state) = match row {
        Ok((Some(id), Some(state))) => (id, state),
        _ => pgrx::error!("Pipeline '{}' not found", name),
    };

    let was_running = state == "running";

    // Stop if running
    if was_running {
        let _ = Spi::run_with_args(
            "UPDATE pgstreams.pipelines SET state = 'stopped', stopped_at = now(), \
             worker_id = NULL, updated_at = now() WHERE id = $1",
            &[pipeline_id.into()],
        );
    }

    // Update definition and insert new version in one go
    let _ = Spi::run_with_args(
        "UPDATE pgstreams.pipelines SET definition = $2, updated_at = now() WHERE id = $1",
        &[pipeline_id.into(), pgrx::JsonB(definition.0.clone()).into()],
    );

    let _ = Spi::run_with_args(
        "INSERT INTO pgstreams.pipeline_versions (pipeline_id, version, definition) \
         SELECT $1, COALESCE(MAX(version), 0) + 1, definition \
         FROM pgstreams.pipeline_versions pv \
         JOIN pgstreams.pipelines p ON p.id = $1 \
         WHERE pv.pipeline_id = $1 \
         GROUP BY p.definition",
        &[pipeline_id.into()],
    );

    // Restart if it was running
    if was_running {
        let _ = Spi::run_with_args(
            "UPDATE pgstreams.pipelines SET state = 'running', started_at = now(), \
             updated_at = now() WHERE id = $1",
            &[pipeline_id.into()],
        );
    }
}

/// Drop a pipeline by name
pub fn drop_pipeline_impl(name: &str) {
    // Fetch definition before deletion so we can clean up CDC resources
    let def_json = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT definition FROM pgstreams.pipelines WHERE name = $1",
        &[name.into()],
    );

    let result = Spi::get_one_with_args::<i32>(
        "DELETE FROM pgstreams.pipelines WHERE name = $1 RETURNING id",
        &[name.into()],
    );

    match result {
        Ok(Some(_)) => {}
        _ => pgrx::error!("Pipeline '{}' not found", name),
    }

    // Clean up connector offsets
    let _ = Spi::run_with_args(
        "DELETE FROM pgstreams.connector_offsets WHERE pipeline = $1",
        &[name.into()],
    );

    // If CDC input, drop the replication slot
    if let Ok(Some(jb)) = def_json {
        if let Ok(def) = parse_pipeline_value(&jb.0) {
            if let crate::dsl::types::InputConfig::Cdc(c) = &def.input {
                let slot = c
                    .slot
                    .clone()
                    .unwrap_or_else(|| format!("pgstreams_{}", name.replace('-', "_")));
                let _ = Spi::run_with_args(
                    "SELECT pg_drop_replication_slot($1)",
                    &[slot.as_str().into()],
                );
            }
        }
    }
}
