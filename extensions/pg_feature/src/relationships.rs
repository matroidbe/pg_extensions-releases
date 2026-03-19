//! Relationship management for pg_feature
//!
//! Relationships define how tables are connected for feature aggregation.
//! They can be auto-discovered from foreign keys or manually registered.

use crate::schema::{parse_table_ref, verify_table_exists, TableRef};
use crate::PgFeatureError;
use pgrx::prelude::*;

/// Escape a string literal for use in SQL
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// A relationship between two tables
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Relationship {
    pub id: i64,
    pub parent: TableRef,
    pub parent_column: String,
    pub child: TableRef,
    pub child_column: String,
    pub name: Option<String>,
}

impl Relationship {
    /// Generate a default name for the relationship
    #[allow(dead_code)]
    pub fn default_name(&self) -> String {
        format!("{}_to_{}", self.child.name, self.parent.name)
    }
}

/// Add a relationship between two tables
pub(crate) fn add_relationship_impl(
    parent_table: &str,
    parent_column: &str,
    child_table: &str,
    child_column: &str,
    relationship_name: Option<&str>,
) -> Result<i64, PgFeatureError> {
    let parent_ref = parse_table_ref(parent_table)?;
    let child_ref = parse_table_ref(child_table)?;

    // Verify tables exist
    if !verify_table_exists(&parent_ref)? {
        return Err(PgFeatureError::TableNotFound(parent_ref.qualified_name()));
    }
    if !verify_table_exists(&child_ref)? {
        return Err(PgFeatureError::TableNotFound(child_ref.qualified_name()));
    }

    // Insert the relationship
    let id = Spi::connect_mut(|client| {
        let rel_name_sql = match relationship_name {
            Some(name) => format!("'{}'", escape_literal(name)),
            None => "NULL".to_string(),
        };
        let sql = format!(
            r#"
            INSERT INTO pgft.relationships
                (parent_schema, parent_table, parent_column, child_schema, child_table, child_column, relationship_name)
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {})
            ON CONFLICT (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
            DO UPDATE SET relationship_name = EXCLUDED.relationship_name, created_at = NOW()
            RETURNING id
            "#,
            escape_literal(&parent_ref.schema),
            escape_literal(&parent_ref.name),
            escape_literal(parent_column),
            escape_literal(&child_ref.schema),
            escape_literal(&child_ref.name),
            escape_literal(child_column),
            rel_name_sql
        );
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            Ok(row
                .get::<i64>(1)?
                .ok_or_else(|| PgFeatureError::SqlError("Failed to get inserted id".to_string()))?)
        } else {
            Err(PgFeatureError::SqlError(
                "Insert returned no rows".to_string(),
            ))
        }
    })?;

    Ok(id)
}

/// List all registered relationships
#[allow(clippy::type_complexity)]
pub(crate) fn list_relationships_impl() -> Result<
    Vec<(
        i64,
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
    )>,
    PgFeatureError,
> {
    let relationships = Spi::connect(|client| {
        let result = client.select(
            r#"
            SELECT id, parent_schema, parent_table, parent_column,
                   child_schema, child_table, child_column, relationship_name
            FROM pgft.relationships
            ORDER BY id
            "#,
            None,
            &[],
        )?;

        let mut rels = Vec::new();
        for row in result {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let parent_schema: String = row.get(2)?.unwrap_or_default();
            let parent_table: String = row.get(3)?.unwrap_or_default();
            let parent_column: String = row.get(4)?.unwrap_or_default();
            let child_schema: String = row.get(5)?.unwrap_or_default();
            let child_table: String = row.get(6)?.unwrap_or_default();
            let child_column: String = row.get(7)?.unwrap_or_default();
            let relationship_name: Option<String> = row.get(8)?;
            rels.push((
                id,
                parent_schema,
                parent_table,
                parent_column,
                child_schema,
                child_table,
                child_column,
                relationship_name,
            ));
        }
        Ok::<_, pgrx::spi::Error>(rels)
    })?;

    Ok(relationships)
}

/// Remove a relationship by ID
pub(crate) fn remove_relationship_impl(relationship_id: i64) -> Result<bool, PgFeatureError> {
    let deleted = Spi::connect_mut(|client| {
        let sql = format!(
            "DELETE FROM pgft.relationships WHERE id = {} RETURNING id",
            relationship_id
        );
        let result = client.select(&sql, None, &[])?;
        // Count the rows returned
        let mut count = 0;
        for _ in result {
            count += 1;
        }
        Ok::<_, pgrx::spi::Error>(count > 0)
    })?;

    Ok(deleted)
}

/// Auto-discover relationships from foreign key constraints
pub(crate) fn discover_relationships_impl(schema_name: &str) -> Result<i32, PgFeatureError> {
    let discovered = Spi::connect_mut(|client| {
        // Query foreign key constraints
        // Note: Cast sql_identifier columns to text for Rust compatibility
        let query = format!(
            r#"
            SELECT
                kcu.table_schema::text AS child_schema,
                kcu.table_name::text AS child_table,
                kcu.column_name::text AS child_column,
                ccu.table_schema::text AS parent_schema,
                ccu.table_name::text AS parent_table,
                ccu.column_name::text AS parent_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND kcu.table_schema = '{}'
              AND NOT EXISTS (
                  SELECT 1 FROM pgft.relationships r
                  WHERE r.parent_schema = ccu.table_schema::text
                    AND r.parent_table = ccu.table_name::text
                    AND r.parent_column = ccu.column_name::text
                    AND r.child_schema = kcu.table_schema::text
                    AND r.child_table = kcu.table_name::text
                    AND r.child_column = kcu.column_name::text
              )
            ORDER BY kcu.table_name, kcu.column_name
            "#,
            escape_literal(schema_name)
        );
        let result = client.select(&query, None, &[])?;

        // Collect rows first since we need to run inserts
        let mut rows = Vec::new();
        for row in result {
            let child_schema: String = row.get(1)?.unwrap_or_default();
            let child_table: String = row.get(2)?.unwrap_or_default();
            let child_column: String = row.get(3)?.unwrap_or_default();
            let parent_schema: String = row.get(4)?.unwrap_or_default();
            let parent_table: String = row.get(5)?.unwrap_or_default();
            let parent_column: String = row.get(6)?.unwrap_or_default();
            rows.push((
                child_schema,
                child_table,
                child_column,
                parent_schema,
                parent_table,
                parent_column,
            ));
        }

        let mut count = 0;
        for (child_schema, child_table, child_column, parent_schema, parent_table, parent_column) in
            rows
        {
            // Insert the relationship
            let insert_sql = format!(
                r#"
                INSERT INTO pgft.relationships
                    (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}')
                ON CONFLICT (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
                DO NOTHING
                "#,
                escape_literal(&parent_schema),
                escape_literal(&parent_table),
                escape_literal(&parent_column),
                escape_literal(&child_schema),
                escape_literal(&child_table),
                escape_literal(&child_column)
            );
            Spi::run(&insert_sql)?;
            count += 1;
        }

        Ok::<_, pgrx::spi::Error>(count)
    })?;

    Ok(discovered)
}

/// Get all relationships for a target table (where target is the parent)
#[allow(dead_code)]
pub(crate) fn get_child_relationships(
    target: &TableRef,
) -> Result<Vec<Relationship>, PgFeatureError> {
    let relationships = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT id, parent_schema, parent_table, parent_column,
                   child_schema, child_table, child_column, relationship_name
            FROM pgft.relationships
            WHERE parent_schema = '{}' AND parent_table = '{}'
            ORDER BY child_table, child_column
            "#,
            escape_literal(&target.schema),
            escape_literal(&target.name)
        );
        let result = client.select(&sql, None, &[])?;

        let mut rels = Vec::new();
        for row in result {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let parent_schema: String = row.get(2)?.unwrap_or_default();
            let parent_table: String = row.get(3)?.unwrap_or_default();
            let parent_column: String = row.get(4)?.unwrap_or_default();
            let child_schema: String = row.get(5)?.unwrap_or_default();
            let child_table: String = row.get(6)?.unwrap_or_default();
            let child_column: String = row.get(7)?.unwrap_or_default();
            let relationship_name: Option<String> = row.get(8)?;

            rels.push(Relationship {
                id,
                parent: TableRef::new(&parent_schema, &parent_table),
                parent_column,
                child: TableRef::new(&child_schema, &child_table),
                child_column,
                name: relationship_name,
            });
        }
        Ok::<_, pgrx::spi::Error>(rels)
    })?;

    Ok(relationships)
}

/// Get all relationships in the registry
pub(crate) fn get_all_relationships() -> Result<Vec<Relationship>, PgFeatureError> {
    let relationships = Spi::connect(|client| {
        let result = client.select(
            r#"
            SELECT id, parent_schema, parent_table, parent_column,
                   child_schema, child_table, child_column, relationship_name
            FROM pgft.relationships
            ORDER BY id
            "#,
            None,
            &[],
        )?;

        let mut rels = Vec::new();
        for row in result {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let parent_schema: String = row.get(2)?.unwrap_or_default();
            let parent_table: String = row.get(3)?.unwrap_or_default();
            let parent_column: String = row.get(4)?.unwrap_or_default();
            let child_schema: String = row.get(5)?.unwrap_or_default();
            let child_table: String = row.get(6)?.unwrap_or_default();
            let child_column: String = row.get(7)?.unwrap_or_default();
            let relationship_name: Option<String> = row.get(8)?;

            rels.push(Relationship {
                id,
                parent: TableRef::new(&parent_schema, &parent_table),
                parent_column,
                child: TableRef::new(&child_schema, &child_table),
                child_column,
                name: relationship_name,
            });
        }
        Ok::<_, pgrx::spi::Error>(rels)
    })?;

    Ok(relationships)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relationship_default_name() {
        let rel = Relationship {
            id: 1,
            parent: TableRef::new("public", "customers"),
            parent_column: "customer_id".to_string(),
            child: TableRef::new("public", "orders"),
            child_column: "customer_id".to_string(),
            name: None,
        };
        assert_eq!(rel.default_name(), "orders_to_customers");
    }
}
