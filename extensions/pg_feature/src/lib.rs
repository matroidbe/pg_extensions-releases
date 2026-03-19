//! pg_feature: Automated feature engineering for PostgreSQL
//!
//! This extension implements Deep Feature Synthesis (DFS) as pure SQL,
//! bringing Featuretools-style automated feature engineering directly
//! into PostgreSQL without any Python dependencies.

use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;
use std::ffi::CString;

mod dfs;
mod primitives;
mod relationships;
mod schema;
mod sql_gen;
mod time_indexes;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Documentation
// =============================================================================

/// Returns the extension documentation (README.md) as a string.
/// This enables runtime discovery of extension capabilities via pg_mcp.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Error Types
// =============================================================================

#[derive(thiserror::Error, Debug)]
pub enum PgFeatureError {
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}.{1}")]
    ColumnNotFound(String, String),

    #[error("Relationship not found: {0}")]
    RelationshipNotFound(String),

    #[error("Time index not found for table: {0}")]
    TimeIndexNotFound(String),

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("SQL execution error: {0}")]
    SqlError(String),

    #[error("Schema introspection error: {0}")]
    SchemaError(String),

    #[error("Feature generation error: {0}")]
    FeatureGenError(String),

    #[error("SPI error: {0}")]
    SpiError(String),
}

impl From<pgrx::spi::Error> for PgFeatureError {
    fn from(err: pgrx::spi::Error) -> Self {
        PgFeatureError::SpiError(err.to_string())
    }
}

// =============================================================================
// GUC Settings
// =============================================================================

static DEFAULT_DEPTH: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(2);

static DEFAULT_AGG_PRIMITIVES: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

static DEFAULT_TRANS_PRIMITIVES: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

static MAX_FEATURES: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(500);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_int_guc(
        c"pg_feature.default_depth",
        c"Default max_depth for Deep Feature Synthesis",
        c"Controls how many primitives can be stacked (default: 2)",
        &DEFAULT_DEPTH,
        1,
        10,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_feature.default_agg_primitives",
        c"Default aggregation primitives",
        c"Comma-separated list of aggregation primitives (default: sum,mean,count,max,min)",
        &DEFAULT_AGG_PRIMITIVES,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_feature.default_trans_primitives",
        c"Default transform primitives",
        c"Comma-separated list of transform primitives (default: year,month,weekday)",
        &DEFAULT_TRANS_PRIMITIVES,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_feature.max_features",
        c"Maximum number of features to generate",
        c"Prevents feature explosion with complex schemas (default: 500)",
        &MAX_FEATURES,
        10,
        10000,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
}

// =============================================================================
// Schema Bootstrap SQL
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Time index registry (which column represents "when data became known")
CREATE TABLE IF NOT EXISTS pgft.time_indexes (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    time_column TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(schema_name, table_name)
);

-- Relationship metadata (supplements foreign keys)
CREATE TABLE IF NOT EXISTS pgft.relationships (
    id SERIAL PRIMARY KEY,
    parent_schema TEXT NOT NULL,
    parent_table TEXT NOT NULL,
    parent_column TEXT NOT NULL,
    child_schema TEXT NOT NULL,
    child_table TEXT NOT NULL,
    child_column TEXT NOT NULL,
    relationship_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(parent_schema, parent_table, parent_column, child_schema, child_table, child_column)
);

-- Feature definitions (generated features catalog)
CREATE TABLE IF NOT EXISTS pgft.feature_definitions (
    id SERIAL PRIMARY KEY,
    project_name TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_type TEXT NOT NULL,
    primitive TEXT NOT NULL,
    base_column TEXT,
    source_table TEXT,
    depth INT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Generated feature matrices metadata
CREATE TABLE IF NOT EXISTS pgft.feature_matrices (
    id SERIAL PRIMARY KEY,
    project_name TEXT NOT NULL UNIQUE,
    target_schema TEXT NOT NULL,
    target_table TEXT NOT NULL,
    target_id_column TEXT NOT NULL,
    cutoff_time TIMESTAMPTZ,
    cutoff_times_table TEXT,
    cutoff_interval TEXT,
    windows TEXT[],
    output_schema TEXT NOT NULL,
    output_name TEXT NOT NULL,
    output_type TEXT NOT NULL DEFAULT 'table',
    feature_count INT NOT NULL,
    row_count BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_time_indexes_table ON pgft.time_indexes(schema_name, table_name);
CREATE INDEX IF NOT EXISTS idx_relationships_parent ON pgft.relationships(parent_schema, parent_table);
CREATE INDEX IF NOT EXISTS idx_relationships_child ON pgft.relationships(child_schema, child_table);
CREATE INDEX IF NOT EXISTS idx_feature_definitions_project ON pgft.feature_definitions(project_name);
CREATE INDEX IF NOT EXISTS idx_feature_matrices_project ON pgft.feature_matrices(project_name);
"#,
    name = "bootstrap_schema",
    bootstrap
);

// =============================================================================
// Helper Functions
// =============================================================================

/// Get default aggregation primitives from GUC or use built-in defaults
pub(crate) fn get_default_agg_primitives() -> Vec<String> {
    DEFAULT_AGG_PRIMITIVES
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_else(|| {
            vec![
                "count".to_string(),
                "sum".to_string(),
                "mean".to_string(),
                "max".to_string(),
                "min".to_string(),
            ]
        })
}

/// Get default transform primitives from GUC or use built-in defaults
pub(crate) fn get_default_trans_primitives() -> Vec<String> {
    DEFAULT_TRANS_PRIMITIVES
        .get()
        .and_then(|s: CString| s.into_string().ok())
        .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
        .unwrap_or_else(|| {
            vec![
                "year".to_string(),
                "month".to_string(),
                "weekday".to_string(),
            ]
        })
}

/// Get max features limit from GUC
pub(crate) fn get_max_features() -> i32 {
    MAX_FEATURES.get()
}

/// Get default depth from GUC
pub(crate) fn get_default_depth() -> i32 {
    DEFAULT_DEPTH.get()
}

// =============================================================================
// Time Index SQL Functions
// =============================================================================

/// Register a time index for a table
///
/// The time index specifies which column represents "when data became known".
/// This is used for cutoff time filtering to prevent data leakage in ML.
///
/// # Arguments
/// * `table_name` - Fully qualified table name ('schema.table' or 'table')
/// * `time_column` - Name of the timestamp/date column
///
/// # Returns
/// The ID of the created time index record
#[pg_extern]
fn set_time_index(table_name: &str, time_column: &str) -> i64 {
    match time_indexes::set_time_index_impl(table_name, time_column) {
        Ok(id) => id,
        Err(e) => pgrx::error!("Failed to set time index: {}", e),
    }
}

/// List all registered time indexes
#[pg_extern]
fn list_time_indexes() -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(schema_name, String),
        name!(table_name, String),
        name!(time_column, String),
    ),
> {
    match time_indexes::list_time_indexes_impl() {
        Ok(indexes) => TableIterator::new(indexes),
        Err(e) => pgrx::error!("Failed to list time indexes: {}", e),
    }
}

/// Remove a time index for a table
///
/// # Arguments
/// * `table_name` - Fully qualified table name ('schema.table' or 'table')
///
/// # Returns
/// true if the time index was found and removed
#[pg_extern]
fn remove_time_index(table_name: &str) -> bool {
    match time_indexes::remove_time_index_impl(table_name) {
        Ok(removed) => removed,
        Err(e) => pgrx::error!("Failed to remove time index: {}", e),
    }
}

/// Auto-discover time indexes by looking for timestamp columns
///
/// Scans tables in the given schema for columns that look like time indexes
/// (e.g., columns named 'created_at', 'updated_at', 'timestamp', etc.)
///
/// # Arguments
/// * `schema_name` - Schema to scan (default: 'public')
///
/// # Returns
/// Number of time indexes discovered and registered
#[pg_extern]
fn discover_time_indexes(schema_name: default!(Option<&str>, "'public'")) -> i32 {
    let schema = schema_name.unwrap_or("public");
    match time_indexes::discover_time_indexes_impl(schema) {
        Ok(count) => count,
        Err(e) => pgrx::error!("Failed to discover time indexes: {}", e),
    }
}

// =============================================================================
// Relationship SQL Functions
// =============================================================================

/// Add a relationship between two tables
///
/// Relationships define how tables are connected for feature aggregation.
/// The parent table is the "one" side, child table is the "many" side.
///
/// # Arguments
/// * `parent_table` - Parent table ('schema.table' or 'table')
/// * `parent_column` - Primary key column in parent table
/// * `child_table` - Child table ('schema.table' or 'table')
/// * `child_column` - Foreign key column in child table
/// * `relationship_name` - Optional descriptive name
///
/// # Returns
/// The ID of the created relationship record
#[pg_extern]
fn add_relationship(
    parent_table: &str,
    parent_column: &str,
    child_table: &str,
    child_column: &str,
    relationship_name: default!(Option<&str>, "NULL"),
) -> i64 {
    match relationships::add_relationship_impl(
        parent_table,
        parent_column,
        child_table,
        child_column,
        relationship_name,
    ) {
        Ok(id) => id,
        Err(e) => pgrx::error!("Failed to add relationship: {}", e),
    }
}

/// List all registered relationships
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_relationships() -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(parent_schema, String),
        name!(parent_table, String),
        name!(parent_column, String),
        name!(child_schema, String),
        name!(child_table, String),
        name!(child_column, String),
        name!(relationship_name, Option<String>),
    ),
> {
    match relationships::list_relationships_impl() {
        Ok(rels) => TableIterator::new(rels),
        Err(e) => pgrx::error!("Failed to list relationships: {}", e),
    }
}

/// Remove a relationship by ID
///
/// # Arguments
/// * `relationship_id` - ID of the relationship to remove
///
/// # Returns
/// true if the relationship was found and removed
#[pg_extern]
fn remove_relationship(relationship_id: i64) -> bool {
    match relationships::remove_relationship_impl(relationship_id) {
        Ok(removed) => removed,
        Err(e) => pgrx::error!("Failed to remove relationship: {}", e),
    }
}

/// Auto-discover relationships from foreign key constraints
///
/// Scans tables in the given schema for foreign key constraints
/// and registers them as relationships.
///
/// # Arguments
/// * `schema_name` - Schema to scan (default: 'public')
///
/// # Returns
/// Number of relationships discovered and registered
#[pg_extern]
fn discover_relationships(schema_name: default!(Option<&str>, "'public'")) -> i32 {
    let schema = schema_name.unwrap_or("public");
    match relationships::discover_relationships_impl(schema) {
        Ok(count) => count,
        Err(e) => pgrx::error!("Failed to discover relationships: {}", e),
    }
}

// =============================================================================
// Feature Synthesis SQL Functions
// =============================================================================

/// Generate features using Deep Feature Synthesis
///
/// This is the main entry point for automated feature engineering.
/// It generates features from related tables using aggregation and transform
/// primitives, respecting cutoff times to prevent data leakage.
///
/// # Arguments
/// * `project_name` - Unique name for this feature set
/// * `target_table` - Table to generate features for ('schema.table' or 'table')
/// * `target_id` - Primary key column in target table
/// * `cutoff_time` - Single cutoff time for all entities (optional)
/// * `cutoff_times_table` - Table with per-entity cutoff times (optional)
/// * `cutoff_time_column` - Column name in cutoff_times_table (default: 'cutoff_time')
/// * `cutoff_interval` - Interval for auto-generating cutoffs from data (e.g., '1 month')
/// * `windows` - Rolling window intervals for time-relative features (e.g., ['30 days', '90 days'])
/// * `tables` - Tables to include in feature generation (optional, defaults to all related)
/// * `agg_primitives` - Aggregation primitives to use (optional, uses GUC default)
/// * `trans_primitives` - Transform primitives to use (optional, uses GUC default)
/// * `max_depth` - Maximum depth for stacking primitives (optional, uses GUC default)
/// * `output_name` - Name for output materialized view (optional, defaults to project_name)
/// * `if_exists` - 'error' or 'replace' (default: 'error')
///
/// # Returns
/// Summary table with project_name, feature_count, row_count, output_name
///
/// # Output Objects
/// Always creates a materialized view and auxiliary views:
/// - `{output_name}` - Materialized view with features (refresh with REFRESH MATERIALIZED VIEW)
/// - `{output_name}_train` - View excluding cutoff_time (ML ready, use SELECT *)
/// - `{output_name}_live` - View with cutoff = now() (real-time inference)
#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn synthesize_features(
    project_name: &str,
    target_table: &str,
    target_id: &str,
    cutoff_time: default!(Option<TimestampWithTimeZone>, "NULL"),
    cutoff_times_table: default!(Option<&str>, "NULL"),
    cutoff_time_column: default!(Option<&str>, "'cutoff_time'"),
    cutoff_interval: default!(Option<&str>, "NULL"),
    windows: default!(Option<Vec<String>>, "NULL"),
    tables: default!(Option<Vec<String>>, "NULL"),
    agg_primitives: default!(Option<Vec<String>>, "NULL"),
    trans_primitives: default!(Option<Vec<String>>, "NULL"),
    max_depth: default!(Option<i32>, "NULL"),
    output_name: default!(Option<&str>, "NULL"),
    if_exists: default!(Option<&str>, "'error'"),
) -> TableIterator<
    'static,
    (
        name!(project_name, String),
        name!(feature_count, i32),
        name!(row_count, i64),
        name!(output_name, String),
    ),
> {
    let depth = max_depth.unwrap_or_else(get_default_depth);
    let agg_prims = agg_primitives.unwrap_or_else(get_default_agg_primitives);
    let trans_prims = trans_primitives.unwrap_or_else(get_default_trans_primitives);
    let out_name = output_name.unwrap_or(project_name);
    let exists_behavior = if_exists.unwrap_or("error");
    let cutoff_col = cutoff_time_column.unwrap_or("cutoff_time");

    // Validate if_exists
    if !["error", "replace"].contains(&exists_behavior) {
        pgrx::error!(
            "if_exists must be 'error' or 'replace', got '{}'",
            exists_behavior
        );
    }

    match dfs::synthesize_features_impl(
        project_name,
        target_table,
        target_id,
        cutoff_time,
        cutoff_times_table,
        cutoff_col,
        cutoff_interval,
        windows.as_deref(),
        tables.as_deref(),
        &agg_prims,
        &trans_prims,
        depth,
        out_name,
        exists_behavior,
    ) {
        Ok(result) => TableIterator::once((
            result.project_name,
            result.feature_count,
            result.row_count,
            result.output_name,
        )),
        Err(e) => pgrx::error!("Feature synthesis failed: {}", e),
    }
}

/// Generate evenly-spaced cutoff times for time-series feature engineering
///
/// Creates a table with cutoff times for each entity at regular intervals.
/// This is useful for creating training sets for time-series ML models.
///
/// # Arguments
/// * `target_table` - Table containing entities
/// * `target_id` - Primary key column
/// * `start_time` - First cutoff time
/// * `window_size` - Interval between cutoffs (e.g., '1 month', '1 week')
/// * `output_table` - Name for the output cutoff times table
/// * `end_time` - Last cutoff time (optional if num_windows is specified)
/// * `num_windows` - Number of cutoff windows (optional if end_time is specified)
///
/// # Returns
/// Number of cutoff records created
#[pg_extern]
fn make_temporal_cutoffs(
    target_table: &str,
    target_id: &str,
    start_time: TimestampWithTimeZone,
    window_size: &str,
    output_table: &str,
    end_time: default!(Option<TimestampWithTimeZone>, "NULL"),
    num_windows: default!(Option<i32>, "NULL"),
) -> i64 {
    // Must have either end_time or num_windows
    if end_time.is_none() && num_windows.is_none() {
        pgrx::error!("Must specify either end_time or num_windows");
    }

    match dfs::make_temporal_cutoffs_impl(
        target_table,
        target_id,
        start_time,
        end_time,
        window_size,
        num_windows,
        output_table,
    ) {
        Ok(count) => count,
        Err(e) => pgrx::error!("Failed to create temporal cutoffs: {}", e),
    }
}

/// List features generated for a project
///
/// # Arguments
/// * `project_name` - Name of the project
///
/// # Returns
/// Table with feature_name, feature_type, primitive, base_column, depth, description
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_features(
    project_name: &str,
) -> TableIterator<
    'static,
    (
        name!(feature_name, String),
        name!(feature_type, String),
        name!(primitive, String),
        name!(base_column, Option<String>),
        name!(source_table, Option<String>),
        name!(depth, i32),
        name!(description, Option<String>),
    ),
> {
    match dfs::list_features_impl(project_name) {
        Ok(features) => TableIterator::new(features),
        Err(e) => pgrx::error!("Failed to list features: {}", e),
    }
}

/// Get a human-readable explanation of how a feature was derived
///
/// # Arguments
/// * `project_name` - Name of the project
/// * `feature_name` - Name of the feature to explain
///
/// # Returns
/// Human-readable description of the feature derivation
#[pg_extern]
fn explain_feature(project_name: &str, feature_name: &str) -> String {
    match dfs::explain_feature_impl(project_name, feature_name) {
        Ok(explanation) => explanation,
        Err(e) => pgrx::error!("Failed to explain feature: {}", e),
    }
}

// =============================================================================
// Tests
// =============================================================================

// Note: GUC-related tests are in pg_tests since they require pgrx runtime

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads() {
        // Verify schema was created
        let schema_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgft')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(schema_exists, "pgft schema should exist");
    }

    #[pg_test]
    fn test_metadata_tables_exist() {
        // Verify time_indexes table exists
        let time_indexes_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgft' AND table_name = 'time_indexes')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(time_indexes_exists, "pgft.time_indexes should exist");

        // Verify relationships table exists
        let relationships_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgft' AND table_name = 'relationships')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(relationships_exists, "pgft.relationships should exist");

        // Verify feature_definitions table exists
        let feature_defs_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgft' AND table_name = 'feature_definitions')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(feature_defs_exists, "pgft.feature_definitions should exist");
    }

    #[pg_test]
    fn test_feature_matrices_has_new_columns() {
        // Verify feature_matrices table has the new columns
        let cutoff_interval_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'pgft' AND table_name = 'feature_matrices'
             AND column_name = 'cutoff_interval')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(
            cutoff_interval_exists,
            "pgft.feature_matrices should have cutoff_interval column"
        );

        let windows_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'pgft' AND table_name = 'feature_matrices'
             AND column_name = 'windows')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(
            windows_exists,
            "pgft.feature_matrices should have windows column"
        );
    }

    #[pg_test]
    fn test_synthesize_with_windows() {
        // Create test tables
        Spi::run(
            "CREATE TABLE test_customers (
                id SERIAL PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )",
        )
        .unwrap();

        Spi::run(
            "CREATE TABLE test_orders (
                id SERIAL PRIMARY KEY,
                customer_id INT REFERENCES test_customers(id),
                amount NUMERIC,
                order_date TIMESTAMPTZ
            )",
        )
        .unwrap();

        // Insert test data
        Spi::run("INSERT INTO test_customers (name) VALUES ('Alice'), ('Bob')").unwrap();

        Spi::run(
            "INSERT INTO test_orders (customer_id, amount, order_date) VALUES
             (1, 100, '2024-01-15'),
             (1, 200, '2024-02-15'),
             (1, 150, '2024-03-15'),
             (2, 50, '2024-01-20'),
             (2, 75, '2024-03-01')",
        )
        .unwrap();

        // Setup relationships and time indexes
        Spi::run(
            "SELECT pgft.add_relationship('test_customers', 'id', 'test_orders', 'customer_id')",
        )
        .unwrap();

        Spi::run("SELECT pgft.set_time_index('test_orders', 'order_date')").unwrap();

        // Create cutoffs table
        Spi::run(
            "CREATE TABLE test_cutoffs AS
             SELECT id AS id, '2024-04-01'::timestamptz AS cutoff_time
             FROM test_customers",
        )
        .unwrap();

        // Synthesize features with windows
        Spi::run(
            "SELECT * FROM pgft.synthesize_features(
                project_name := 'test_windows',
                target_table := 'test_customers',
                target_id := 'id',
                cutoff_times_table := 'test_cutoffs',
                windows := ARRAY['30 days', '90 days'],
                agg_primitives := ARRAY['count', 'sum'],
                max_depth := 1,
                output_name := 'test_features'
            )",
        )
        .unwrap();

        // Verify feature count includes window variants
        let feature_count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgft.feature_definitions WHERE project_name = 'test_windows'",
        )
        .unwrap()
        .unwrap_or(0);

        // Should have: COUNT (all-time, 30d, 90d) + SUM_amount (all-time, 30d, 90d) = 6 features
        assert!(
            feature_count >= 6,
            "Should have at least 6 features (count and sum with windows), got {}",
            feature_count
        );

        // Verify window-suffixed features exist
        let has_30d = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgft.feature_definitions
             WHERE project_name = 'test_windows' AND feature_name LIKE '%_30d')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(has_30d, "Should have 30d window features");

        let has_90d = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pgft.feature_definitions
             WHERE project_name = 'test_windows' AND feature_name LIKE '%_90d')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(has_90d, "Should have 90d window features");

        // Verify _train view was created
        let train_view_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.views
             WHERE table_name = 'test_features_train')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(train_view_exists, "test_features_train view should exist");

        // Verify _live view was created
        let live_view_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.views
             WHERE table_name = 'test_features_live')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(live_view_exists, "test_features_live view should exist");

        // Verify _train view excludes cutoff_time
        let train_has_cutoff = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns
             WHERE table_name = 'test_features_train' AND column_name = 'cutoff_time')",
        )
        .unwrap()
        .unwrap_or(true);
        assert!(
            !train_has_cutoff,
            "_train view should not have cutoff_time column"
        );

        // Verify main output is a materialized view (not a table)
        let is_matview = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname = 'test_features')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(is_matview, "test_features should be a materialized view");

        // Clean up
        Spi::run("DROP MATERIALIZED VIEW IF EXISTS test_features CASCADE").unwrap();
        Spi::run("DROP VIEW IF EXISTS test_features_train CASCADE").unwrap();
        Spi::run("DROP VIEW IF EXISTS test_features_live CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_cutoffs CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_orders CASCADE").unwrap();
        Spi::run("DROP TABLE IF EXISTS test_customers CASCADE").unwrap();
        Spi::run("DELETE FROM pgft.relationships").unwrap();
        Spi::run("DELETE FROM pgft.time_indexes").unwrap();
        Spi::run("DELETE FROM pgft.feature_definitions").unwrap();
        Spi::run("DELETE FROM pgft.feature_matrices").unwrap();
    }

    #[pg_test]
    fn test_window_suffix_generation() {
        // Test that window suffixes are correctly generated
        // This is a unit-ish test that verifies the naming convention

        use crate::dfs::window_to_suffix;

        assert_eq!(window_to_suffix("30 days"), "30d");
        assert_eq!(window_to_suffix("90 days"), "90d");
        assert_eq!(window_to_suffix("1 month"), "1m");
        assert_eq!(window_to_suffix("3 months"), "3m");
        assert_eq!(window_to_suffix("1 week"), "1w");
        assert_eq!(window_to_suffix("1 year"), "1y");
    }
}

/// This module is required by `cargo pgrx test` invocations.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
