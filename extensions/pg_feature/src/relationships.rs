//! Relationship management for pg_feature
//!
//! Relationships define how tables are connected for feature aggregation.
//! They can be auto-discovered from foreign keys or manually registered.

use crate::schema::{parse_table_ref, verify_table_exists, TableRef};
use crate::PgFeatureError;
use pgrx::prelude::*;

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

    // get_one_with_args writes through a read-write SPI call; a select would be
    // read-only, and fail, whenever this is the transaction's first write
    let id = Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pgft.relationships
            (parent_schema, parent_table, parent_column, child_schema, child_table, child_column, relationship_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
        DO UPDATE SET relationship_name = EXCLUDED.relationship_name, created_at = NOW()
        RETURNING id::bigint
        "#,
        &[
            parent_ref.schema.as_str().into(),
            parent_ref.name.as_str().into(),
            parent_column.into(),
            child_ref.schema.as_str().into(),
            child_ref.name.as_str().into(),
            child_column.into(),
            relationship_name.into(),
        ],
    )?
    .ok_or_else(|| PgFeatureError::SqlError("Insert returned no id".to_string()))?;

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
    let deleted = Spi::get_one_with_args::<bool>(
        "WITH d AS (DELETE FROM pgft.relationships WHERE id = $1 RETURNING 1)
         SELECT COUNT(*) > 0 FROM d",
        &[relationship_id.into()],
    )?;
    Ok(deleted.unwrap_or(false))
}

/// Auto-discover relationships from foreign key constraints
///
/// Registers every single-column foreign key whose child table is in `schema_name`;
/// the parent may live in any schema. Composite foreign keys are skipped: a
/// relationship joins on one column. Returns the number of new relationships.
pub(crate) fn discover_relationships_impl(schema_name: &str) -> Result<i32, PgFeatureError> {
    let discovered = Spi::get_one_with_args::<i64>(
        r#"
        WITH fks AS (
            SELECT pn.nspname::text AS parent_schema, pc.relname::text AS parent_table,
                   pa.attname::text AS parent_column,
                   cn.nspname::text AS child_schema, cc.relname::text AS child_table,
                   ca.attname::text AS child_column
            FROM pg_constraint con
            JOIN pg_class cc ON cc.oid = con.conrelid
            JOIN pg_namespace cn ON cn.oid = cc.relnamespace
            JOIN pg_class pc ON pc.oid = con.confrelid
            JOIN pg_namespace pn ON pn.oid = pc.relnamespace
            JOIN pg_attribute ca ON ca.attrelid = con.conrelid AND ca.attnum = con.conkey[1]
            JOIN pg_attribute pa ON pa.attrelid = con.confrelid AND pa.attnum = con.confkey[1]
            WHERE con.contype = 'f'
              AND cardinality(con.conkey) = 1
              AND cn.nspname = $1
        ),
        ins AS (
            INSERT INTO pgft.relationships
                (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
            SELECT * FROM fks ORDER BY child_table, child_column
            ON CONFLICT (parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
            DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) FROM ins
        "#,
        &[schema_name.into()],
    )?;
    Ok(discovered.unwrap_or(0) as i32)
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
