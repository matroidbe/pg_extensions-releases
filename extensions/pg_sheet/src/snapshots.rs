//! Snapshot management for pg_sheet.
//!
//! Snapshots capture the entire overlay state at a point in time.
//! They can be restored (replacing current overlay) or diffed (comparing changes).

use pgrx::prelude::*;

/// Create a snapshot of the current overlay state.
///
/// Returns the snapshot UUID.
pub fn create_snapshot_impl(sheet_name: &str, label: Option<&str>) -> Result<String, String> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    let snap_table = format!("pgsheet.\"_snap_{}\"", sheet_name);

    // Generate snapshot ID
    let snapshot_id = Spi::get_one::<pgrx::Uuid>("SELECT gen_random_uuid()")
        .map_err(|e| e.to_string())?
        .ok_or("Failed to generate UUID")?;

    // Count cells
    let cell_count = Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {}", overlay_table))
        .map_err(|e| e.to_string())?
        .unwrap_or(0);

    // Copy overlay data to snapshot
    Spi::run_with_args(
        &format!(
            r#"INSERT INTO {} (snapshot_id, entity_id, col_name, value, formula)
               SELECT $1, entity_id, col_name, value, formula FROM {}"#,
            snap_table, overlay_table
        ),
        &[snapshot_id.into()],
    )
    .map_err(|e| e.to_string())?;

    // Record snapshot metadata
    Spi::run_with_args(
        r#"INSERT INTO pgsheet._snapshots (id, sheet_name, label, cell_count)
           VALUES ($1, $2, $3, $4)"#,
        &[
            snapshot_id.into(),
            sheet_name.into(),
            label.map(|s| s.to_string()).into(),
            cell_count.into(),
        ],
    )
    .map_err(|e| e.to_string())?;

    Ok(snapshot_id.to_string())
}

/// Restore a snapshot, replacing the current overlay state.
pub fn restore_snapshot_impl(sheet_name: &str, snapshot_id: pgrx::Uuid) -> Result<bool, String> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    let snap_table = format!("pgsheet.\"_snap_{}\"", sheet_name);

    // Verify snapshot exists
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgsheet._snapshots WHERE id = $1 AND sheet_name = $2)",
        &[snapshot_id.into(), sheet_name.into()],
    )
    .map_err(|e| e.to_string())?;

    if exists != Some(true) {
        return Err(format!("Snapshot not found for sheet '{}'", sheet_name));
    }

    // Clear current overlay
    Spi::run(&format!("DELETE FROM {}", overlay_table)).map_err(|e| e.to_string())?;

    // Restore from snapshot
    Spi::run_with_args(
        &format!(
            r#"INSERT INTO {} (entity_id, col_name, value, formula)
               SELECT entity_id, col_name, value, formula
               FROM {} WHERE snapshot_id = $1"#,
            overlay_table, snap_table
        ),
        &[snapshot_id.into()],
    )
    .map_err(|e| e.to_string())?;

    Ok(true)
}

/// Diff a snapshot against the current overlay state.
///
/// Returns rows showing what changed: added, removed, modified cells.
pub fn diff_snapshot_impl(
    sheet_name: &str,
    snapshot_id: pgrx::Uuid,
) -> Result<
    TableIterator<
        'static,
        (
            name!(entity_id, pgrx::Uuid),
            name!(col_name, String),
            name!(change_type, String),
            name!(snapshot_value, Option<String>),
            name!(current_value, Option<String>),
        ),
    >,
    String,
> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    let snap_table = format!("pgsheet.\"_snap_{}\"", sheet_name);

    let rows: Vec<(pgrx::Uuid, String, String, Option<String>, Option<String>)> =
        Spi::connect(|client| {
            let mut results = Vec::new();

            // Full outer join to find all differences.
            // The snapshot_id filter must be in the JOIN condition (not WHERE)
            // so that rows only in the overlay ("added") are not filtered out.
            let query = format!(
                r#"SELECT
                    COALESCE(s.entity_id, o.entity_id) AS entity_id,
                    COALESCE(s.col_name, o.col_name) AS col_name,
                    CASE
                        WHEN s.entity_id IS NULL THEN 'added'
                        WHEN o.entity_id IS NULL THEN 'removed'
                        WHEN COALESCE(s.value, s.formula, '') != COALESCE(o.value, o.formula, '') THEN 'modified'
                        ELSE 'unchanged'
                    END AS change_type,
                    COALESCE(s.value, s.formula) AS snapshot_value,
                    COALESCE(o.value, o.formula) AS current_value
                FROM (SELECT * FROM {} WHERE snapshot_id = $1) s
                FULL OUTER JOIN {} o
                    ON s.entity_id = o.entity_id AND s.col_name = o.col_name
                WHERE s.entity_id IS NULL
                   OR o.entity_id IS NULL
                   OR COALESCE(s.value, s.formula, '') != COALESCE(o.value, o.formula, '')"#,
                snap_table, overlay_table
            );

            let table = client.select(&query, None, &[snapshot_id.into()])?;
            for row in table {
                let eid: pgrx::Uuid = row.get(1)?.unwrap();
                let col: String = row.get(2)?.unwrap_or_default();
                let change: String = row.get(3)?.unwrap_or_default();
                let snap_val: Option<String> = row.get(4)?;
                let curr_val: Option<String> = row.get(5)?;
                results.push((eid, col, change, snap_val, curr_val));
            }
            Ok::<_, pgrx::spi::Error>(results)
        })
        .map_err(|e| e.to_string())?;

    Ok(TableIterator::new(rows))
}
