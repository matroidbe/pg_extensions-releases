//! Cell locking for collaborative editing.
//!
//! Locks prevent concurrent edits to the same cell. Locks are per-user
//! and auto-expire (cleaned up by the application layer or a cron job).

use pgrx::prelude::*;

/// Lock a cell for the current user.
pub fn lock_cell_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
) -> Result<bool, String> {
    let lock_table = format!("pgsheet.\"_lock_{}\"", sheet_name);

    // Check if already locked by someone else
    let existing_lock = match Spi::get_one_with_args::<String>(
        &format!(
            "SELECT locked_by FROM {} WHERE entity_id = $1 AND col_name = $2",
            lock_table
        ),
        &[entity_id.into(), col_name.into()],
    ) {
        Ok(val) => val,
        Err(pgrx::spi::Error::InvalidPosition) => None,
        Err(e) => return Err(e.to_string()),
    };

    let current_user = Spi::get_one::<String>("SELECT current_setting('app.user_id', true)")
        .map_err(|e| e.to_string())?
        .unwrap_or_default();

    if let Some(locker) = existing_lock {
        if locker != current_user {
            return Err(format!("Cell already locked by user '{}'", locker));
        }
        // Already locked by us — refresh timestamp
        Spi::run_with_args(
            &format!(
                "UPDATE {} SET locked_at = now() WHERE entity_id = $1 AND col_name = $2",
                lock_table
            ),
            &[entity_id.into(), col_name.into()],
        )
        .map_err(|e| e.to_string())?;
        return Ok(true);
    }

    // Acquire lock
    Spi::run_with_args(
        &format!(
            "INSERT INTO {} (entity_id, col_name) VALUES ($1, $2)",
            lock_table
        ),
        &[entity_id.into(), col_name.into()],
    )
    .map_err(|e| e.to_string())?;

    Ok(true)
}

/// Unlock a cell. Only the locking user (or force) can unlock.
pub fn unlock_cell_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    force: bool,
) -> Result<bool, String> {
    let lock_table = format!("pgsheet.\"_lock_{}\"", sheet_name);

    if force {
        Spi::run_with_args(
            &format!(
                "DELETE FROM {} WHERE entity_id = $1 AND col_name = $2",
                lock_table
            ),
            &[entity_id.into(), col_name.into()],
        )
        .map_err(|e| e.to_string())?;
        return Ok(true);
    }

    let current_user = Spi::get_one::<String>("SELECT current_setting('app.user_id', true)")
        .map_err(|e| e.to_string())?
        .unwrap_or_default();

    let deleted = Spi::get_one_with_args::<i64>(
        &format!(
            r#"WITH deleted AS (
                DELETE FROM {} WHERE entity_id = $1 AND col_name = $2 AND locked_by = $3
                RETURNING 1
            ) SELECT COUNT(*) FROM deleted"#,
            lock_table
        ),
        &[entity_id.into(), col_name.into(), current_user.into()],
    )
    .map_err(|e| e.to_string())?;

    Ok(deleted.unwrap_or(0) > 0)
}

/// List all locked cells in a sheet.
pub fn locked_cells_impl(
    sheet_name: &str,
) -> Result<
    TableIterator<
        'static,
        (
            name!(entity_id, pgrx::Uuid),
            name!(col_name, String),
            name!(locked_by, String),
            name!(locked_at, Option<pgrx::datum::TimestampWithTimeZone>),
        ),
    >,
    String,
> {
    let lock_table = format!("pgsheet.\"_lock_{}\"", sheet_name);

    let rows: Vec<(
        pgrx::Uuid,
        String,
        String,
        Option<pgrx::datum::TimestampWithTimeZone>,
    )> = Spi::connect(|client| {
        let mut results = Vec::new();
        let query = format!(
            "SELECT entity_id, col_name, locked_by, locked_at FROM {}",
            lock_table
        );
        let table = client.select(&query, None, &[])?;
        for row in table {
            results.push((
                row.get(1)?.unwrap(),
                row.get(2)?.unwrap_or_default(),
                row.get(3)?.unwrap_or_default(),
                row.get(4)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .map_err(|e| e.to_string())?;

    Ok(TableIterator::new(rows))
}

/// Expire locks older than a given interval (e.g., '30 minutes').
pub fn expire_locks_impl(sheet_name: &str, older_than: &str) -> Result<i64, String> {
    let lock_table = format!("pgsheet.\"_lock_{}\"", sheet_name);

    let count = Spi::get_one::<i64>(&format!(
        r#"WITH deleted AS (
            DELETE FROM {} WHERE locked_at < now() - interval '{}'
            RETURNING 1
        ) SELECT COUNT(*) FROM deleted"#,
        lock_table,
        older_than.replace('\'', "''")
    ))
    .map_err(|e| e.to_string())?
    .unwrap_or(0);

    Ok(count)
}
