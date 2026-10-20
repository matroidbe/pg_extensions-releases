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
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect()
        })
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
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect()
        })
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

/// List the available primitives
///
/// # Returns
/// Table with name, primitive_type ('aggregation' | 'transform'), input
/// ('rows' | 'time_index' | 'column'), input_types, needs_time_index, description
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_primitives() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(primitive_type, String),
        name!(input, String),
        name!(input_types, Option<String>),
        name!(needs_time_index, bool),
        name!(description, String),
    ),
> {
    let all = primitives::all_aggregation_primitives()
        .iter()
        .chain(primitives::all_transform_primitives());
    let rows: Vec<_> = all
        .map(|p| {
            let input = match p.input() {
                primitives::Input::Rows => "rows",
                primitives::Input::TimeIndex => "time_index",
                primitives::Input::Column => "column",
            };
            let types = p
                .applicable_types()
                .iter()
                .map(|t| format!("{:?}", t).to_lowercase())
                .collect::<Vec<_>>();
            (
                p.name().to_string(),
                if p.is_aggregation() {
                    "aggregation"
                } else {
                    "transform"
                }
                .to_string(),
                input.to_string(),
                (!types.is_empty()).then(|| types.join(", ")),
                p.needs_time_index(),
                p.description().to_string(),
            )
        })
        .collect();
    TableIterator::new(rows)
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

    /// customers <- orders <- items, orders time-indexed on ordered_at
    fn setup_shop(prefix: &str) {
        Spi::run(&format!(
            "CREATE TABLE {p}_customers (id INT PRIMARY KEY, name TEXT, signup TIMESTAMPTZ);
             CREATE TABLE {p}_orders (id INT PRIMARY KEY, customer_id INT REFERENCES {p}_customers(id),
                                      amount NUMERIC, ordered_at TIMESTAMPTZ);
             CREATE TABLE {p}_items (id INT PRIMARY KEY, order_id INT REFERENCES {p}_orders(id), qty INT);
             INSERT INTO {p}_customers VALUES
               (1, 'Alice', '2024-01-01'), (2, 'Bob', '2024-02-10'), (3, 'Carol', '2024-03-20');
             INSERT INTO {p}_orders VALUES
               (10, 1, 100, '2024-01-15'), (11, 1, NULL, '2024-02-15'), (12, 1, 150, '2024-03-15'),
               (20, 2, 50, '2024-02-20'), (21, 2, 75, '2024-03-01');
             INSERT INTO {p}_items VALUES
               (100, 10, 1), (101, 10, 2), (102, 11, 5), (103, 12, 3), (200, 20, 4);
             SELECT pgft.add_relationship('{p}_customers', 'id', '{p}_orders', 'customer_id');
             SELECT pgft.add_relationship('{p}_orders', 'id', '{p}_items', 'order_id');
             SELECT pgft.set_time_index('{p}_orders', 'ordered_at');",
            p = prefix
        ))
        .unwrap();
    }

    fn count(sql: &str) -> i64 {
        Spi::get_one::<i64>(sql).unwrap().unwrap_or(-1)
    }

    fn exists(relname: &str) -> bool {
        Spi::get_one::<bool>(&format!("SELECT to_regclass('{}') IS NOT NULL", relname))
            .unwrap()
            .unwrap_or(false)
    }

    /// #168: with cutoff_interval the matrix reads the generated cutoffs table;
    /// dropping that table (CASCADE) took the matrix and _train with it
    #[pg_test]
    fn test_auto_cutoff_matrix_survives() {
        setup_shop("ac");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features(
                project_name := 'ac_proj', target_table := 'ac_customers', target_id := 'id',
                cutoff_interval := '1 month', agg_primitives := ARRAY['count', 'sum'],
                max_depth := 1, output_name := 'ac_features')",
        )
        .unwrap();
        assert!(exists("ac_features"), "matrix dropped");
        assert!(exists("ac_features_train"), "_train dropped");
        assert!(exists("ac_features_live"));
        assert!(exists("pgft._cutoffs_ac_proj"));
        assert!(count("SELECT COUNT(*) FROM ac_features") > 0);
        Spi::run("REFRESH MATERIALIZED VIEW ac_features").unwrap();
        assert_eq!(
            Spi::get_one::<String>(
                "SELECT cutoff_times_table FROM pgft.feature_matrices WHERE project_name = 'ac_proj'"
            )
            .unwrap()
            .as_deref(),
            Some("pgft._cutoffs_ac_proj")
        );
    }

    /// #168: _live must compute features exactly like the matrix (it used
    /// COUNT(*) where the matrix used COUNT(col) on a nullable column)
    #[pg_test]
    fn test_live_matches_matrix() {
        setup_shop("lm");
        Spi::run(
            "CREATE TABLE lm_cutoffs AS SELECT id, '2100-01-01'::timestamptz AS cutoff_time FROM lm_customers;
             SELECT * FROM pgft.synthesize_features(
                project_name := 'lm_proj', target_table := 'lm_customers', target_id := 'id',
                cutoff_times_table := 'lm_cutoffs',
                agg_primitives := ARRAY['count', 'sum', 'mean', 'max', 'num_unique'],
                trans_primitives := ARRAY['year'],
                max_depth := 2, output_name := 'lm_features')",
        )
        .unwrap();
        // Identical columns in identical order, identical values
        assert_eq!(
            count("SELECT COUNT(*) FROM (SELECT * FROM lm_features_train EXCEPT SELECT * FROM lm_features_live) d"),
            0
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM (SELECT * FROM lm_features_live EXCEPT SELECT * FROM lm_features_train) d"),
            0
        );
        // Alice has 3 orders, one with a NULL amount: COUNT counts rows
        assert_eq!(
            count(r#"SELECT "lm_orders_COUNT" FROM lm_features_live WHERE id = 1"#),
            3
        );
        // Carol has none
        assert_eq!(
            count(r#"SELECT "lm_orders_COUNT" FROM lm_features_train WHERE id = 3"#),
            0
        );
        assert_eq!(
            count(r#"SELECT "lm_orders_NUM_UNIQUE_amount" FROM lm_features_train WHERE id = 3"#),
            0
        );
    }

    /// #168: depth-2 features were listed by _train but never computed
    #[pg_test]
    fn test_depth2_features_computed() {
        setup_shop("d2");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features(
                project_name := 'd2_proj', target_table := 'd2_customers', target_id := 'id',
                agg_primitives := ARRAY['count', 'sum', 'max'], trans_primitives := ARRAY[]::text[],
                max_depth := 2, output_name := 'd2_features')",
        )
        .unwrap();
        // Alice's orders hold 3, 5 and 3 items: sum 11, max 5; Bob's single order holds 4
        assert_eq!(
            count(
                r#"SELECT "d2_orders_SUM_d2_items_SUM_qty"::bigint FROM d2_features WHERE id = 1"#
            ),
            11
        );
        assert_eq!(
            count(
                r#"SELECT "d2_orders_MAX_d2_items_SUM_qty"::bigint FROM d2_features WHERE id = 1"#
            ),
            5
        );
        assert_eq!(
            count(r#"SELECT "d2_orders_SUM_d2_items_COUNT"::bigint FROM d2_features WHERE id = 1"#),
            4
        );
        // Bob's order 21 has no items: COUNT 0 per order, so the max is still 1
        assert_eq!(
            count(r#"SELECT "d2_orders_MAX_d2_items_COUNT"::bigint FROM d2_features WHERE id = 2"#),
            1
        );
        let defs = count("SELECT COUNT(*) FROM pgft.feature_definitions WHERE project_name = 'd2_proj' AND depth = 2");
        assert!(defs > 0);
    }

    /// Windows were ignored without per-entity cutoffs
    #[pg_test]
    fn test_single_cutoff_windows() {
        setup_shop("sw");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features(
                project_name := 'sw_proj', target_table := 'sw_customers', target_id := 'id',
                cutoff_time := '2024-03-10'::timestamptz, windows := ARRAY['30 days'],
                agg_primitives := ARRAY['count'], trans_primitives := ARRAY[]::text[],
                max_depth := 1, output_name := 'sw_features')",
        )
        .unwrap();
        // As of 2024-03-10 Alice has 2 orders, 1 of them within 30 days
        assert_eq!(
            count(r#"SELECT "sw_orders_COUNT" FROM sw_features WHERE id = 1"#),
            2
        );
        assert_eq!(
            count(r#"SELECT "sw_orders_COUNT_30d" FROM sw_features WHERE id = 1"#),
            1
        );
        assert_eq!(
            count(r#"SELECT "sw_orders_COUNT_30d" FROM sw_features WHERE id = 2"#),
            2
        );
    }

    /// An entity has no row at cutoffs before its own time index
    #[pg_test]
    fn test_target_time_index_filters_entities() {
        setup_shop("tt");
        Spi::run(
            "SELECT pgft.set_time_index('tt_customers', 'signup');
             SELECT * FROM pgft.synthesize_features(
                project_name := 'tt_proj', target_table := 'tt_customers', target_id := 'id',
                cutoff_time := '2024-02-01'::timestamptz, agg_primitives := ARRAY['count'],
                max_depth := 1, output_name := 'tt_features')",
        )
        .unwrap();
        assert_eq!(count("SELECT COUNT(*) FROM tt_features"), 1);
    }

    #[pg_test]
    fn test_if_exists_error_and_replace() {
        setup_shop("ie");
        let synth = |if_exists: &str, prims: &str| {
            Spi::run(&format!(
                "CREATE TABLE IF NOT EXISTS ie_cutoffs AS SELECT id, now() AS cutoff_time FROM ie_customers;
                 SELECT * FROM pgft.synthesize_features(
                    project_name := 'ie_proj', target_table := 'ie_customers', target_id := 'id',
                    cutoff_times_table := 'ie_cutoffs', agg_primitives := ARRAY[{prims}],
                    max_depth := 1, output_name := 'ie_features', if_exists := '{if_exists}')"
            ))
        };
        synth("error", "'count'").unwrap();
        // Rerun with different features replaces the views, including _live
        synth("replace", "'count', 'sum'").unwrap();
        assert!(Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.columns
             WHERE table_name = 'ie_features_live' AND column_name = 'ie_orders_SUM_amount')"
        )
        .unwrap()
        .unwrap());
    }

    #[pg_test]
    #[should_panic(expected = "already exists")]
    fn test_if_exists_error_refuses_existing_output() {
        setup_shop("ee");
        let sql = "SELECT * FROM pgft.synthesize_features(
            project_name := 'ee_proj', target_table := 'ee_customers', target_id := 'id',
            cutoff_interval := '1 month', agg_primitives := ARRAY['count'],
            max_depth := 1, output_name := 'ee_features')";
        Spi::run(sql).unwrap();
        Spi::run(sql).unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "unknown aggregation primitive")]
    fn test_unknown_primitive_rejected() {
        setup_shop("up");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features('up_proj', 'up_customers', 'id',
                agg_primitives := ARRAY['average'])",
        )
        .unwrap();
    }

    /// Every primitive renders valid SQL in every cutoff mode (grouped and lateral)
    #[pg_test]
    fn test_all_primitives_all_modes() {
        setup_shop("ap");
        Spi::run(
            "ALTER TABLE ap_orders ADD COLUMN gift BOOLEAN DEFAULT false, ADD COLUMN channel TEXT DEFAULT 'web';
             SELECT pgft.set_time_index('ap_customers', 'signup');
             CREATE TABLE ap_cutoffs AS SELECT id, '2024-03-01'::timestamptz AS cutoff_time FROM ap_customers;",
        )
        .unwrap();
        let modes = [
            ("none", ""),
            ("single", "cutoff_time := '2024-03-01'::timestamptz,"),
            ("table", "cutoff_times_table := 'ap_cutoffs',"),
            ("auto", "cutoff_interval := '1 month',"),
        ];
        for (name, cutoff) in modes {
            Spi::run(&format!(
                "SELECT * FROM pgft.synthesize_features(
                    project_name := 'ap_{name}', target_table := 'ap_customers', target_id := 'id',
                    {cutoff}
                    windows := ARRAY['30 days'],
                    agg_primitives := (SELECT array_agg(name) FROM pgft.list_primitives() WHERE primitive_type = 'aggregation'),
                    trans_primitives := (SELECT array_agg(name) FROM pgft.list_primitives() WHERE primitive_type = 'transform'),
                    max_depth := 2, output_name := 'ap_{name}_features')"
            ))
            .unwrap();
            assert!(
                count(&format!("SELECT COUNT(*) FROM ap_{name}_features")) > 0,
                "{name}"
            );
        }
        assert!(count("SELECT COUNT(*) FROM ap_table_features_live") > 0);
        assert!(count("SELECT COUNT(*) FROM ap_auto_features_live") > 0);
    }

    #[pg_test]
    fn test_list_primitives() {
        assert_eq!(
            count("SELECT COUNT(*) FROM pgft.list_primitives() WHERE name IN ('count', 'time_since_last', 'year')"),
            3
        );
        assert_eq!(
            Spi::get_one::<String>("SELECT input FROM pgft.list_primitives() WHERE name = 'count'")
                .unwrap()
                .as_deref(),
            Some("rows")
        );
    }

    /// Registry writes went through read-only SPI: they failed whenever they were
    /// the transaction's first write (any plain `SELECT pgft.add_relationship(...)`
    /// in autocommit). The test transaction already holds an xid, so this guards
    /// the calls rather than reproducing that failure.
    #[pg_test]
    fn test_registry_writes_as_first_statement() {
        let id = Spi::get_one::<i64>(
            "SELECT pgft.add_relationship('pg_catalog.pg_class', 'oid',
                                          'pg_catalog.pg_attribute', 'attrelid')",
        )
        .unwrap()
        .unwrap();
        assert!(
            Spi::get_one::<bool>(&format!("SELECT pgft.remove_relationship({id})"))
                .unwrap()
                .unwrap()
        );
        assert!(
            !Spi::get_one::<bool>("SELECT pgft.remove_time_index('public.no_such_table')")
                .unwrap()
                .unwrap()
        );
    }

    #[pg_test]
    fn test_discover_time_indexes() {
        Spi::run(
            "CREATE SCHEMA dti;
             CREATE TABLE dti.events (id INT, updated_at TIMESTAMPTZ, occurred_at TIMESTAMPTZ);
             CREATE TABLE dti.daily (id INT, date DATE);
             CREATE TABLE dti.plain (id INT, created_at TEXT);",
        )
        .unwrap();
        assert_eq!(count("SELECT pgft.discover_time_indexes('dti')"), 2);
        // updated_at moves when a row changes, so it is the last choice
        assert_eq!(
            Spi::get_one::<String>(
                "SELECT time_column FROM pgft.time_indexes
                 WHERE schema_name = 'dti' AND table_name = 'events'"
            )
            .unwrap()
            .as_deref(),
            Some("occurred_at")
        );
    }

    /// FKs into another schema were missed; composite FKs became a cross product
    /// of single-column relationships
    #[pg_test]
    fn test_discover_relationships_cross_schema_and_composite() {
        Spi::run(
            "CREATE SCHEMA drp; CREATE SCHEMA drc;
             CREATE TABLE drp.customers (id INT PRIMARY KEY);
             CREATE TABLE drc.orders (id INT PRIMARY KEY, customer_id INT REFERENCES drp.customers(id));
             CREATE TABLE drc.ab (a INT, b INT, PRIMARY KEY (a, b));
             CREATE TABLE drc.abc (id INT PRIMARY KEY, a INT, b INT, FOREIGN KEY (a, b) REFERENCES drc.ab(a, b));",
        )
        .unwrap();
        assert_eq!(count("SELECT pgft.discover_relationships('drc')"), 1);
        assert_eq!(
            count(
                "SELECT COUNT(*) FROM pgft.relationships
                 WHERE parent_schema = 'drp' AND parent_table = 'customers' AND parent_column = 'id'
                   AND child_schema = 'drc' AND child_table = 'orders' AND child_column = 'customer_id'"
            ),
            1
        );
        // A second run finds nothing new
        assert_eq!(count("SELECT pgft.discover_relationships('drc')"), 0);
    }

    #[pg_test]
    fn test_make_temporal_cutoffs_num_windows() {
        setup_shop("mt");
        assert_eq!(
            count(
                "SELECT pgft.make_temporal_cutoffs('mt_customers', 'id', '2024-01-01', '1 month',
                                                   'mt_cutoffs', num_windows := 3)"
            ),
            9
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM mt_cutoffs WHERE cutoff_time = '2024-03-01'"),
            3
        );
    }

    #[pg_test]
    #[should_panic(expected = "num_windows must be at least 1")]
    fn test_make_temporal_cutoffs_rejects_zero_windows() {
        setup_shop("mz");
        Spi::run(
            "SELECT pgft.make_temporal_cutoffs('mz_customers', 'id', '2024-01-01', '1 month',
                                               'mz_cutoffs', num_windows := 0)",
        )
        .unwrap();
    }

    /// Regenerating cutoffs silently dropped the matrix built on them (CASCADE)
    #[pg_test]
    #[should_panic(expected = "depend on it")]
    fn test_make_temporal_cutoffs_keeps_dependent_matrix() {
        setup_shop("mk");
        Spi::run(
            "SELECT pgft.make_temporal_cutoffs('mk_customers', 'id', '2024-01-01', '1 month',
                                               'mk_cutoffs', num_windows := 2);
             SELECT * FROM pgft.synthesize_features('mk_proj', 'mk_customers', 'id',
                cutoff_times_table := 'mk_cutoffs', max_depth := 1);
             SELECT pgft.make_temporal_cutoffs('mk_customers', 'id', '2024-01-01', '1 month',
                                               'mk_cutoffs', num_windows := 3);",
        )
        .unwrap();
    }

    /// AVG_TIME_BETWEEN subtracted two dates, which is an integer, not an interval
    #[pg_test]
    fn test_date_time_index() {
        Spi::run(
            "CREATE TABLE dt_customers (id INT PRIMARY KEY);
             CREATE TABLE dt_visits (id INT PRIMARY KEY, customer_id INT REFERENCES dt_customers(id), day DATE);
             INSERT INTO dt_customers VALUES (1);
             INSERT INTO dt_visits VALUES (1, 1, '2024-01-01'), (2, 1, '2024-01-03'), (3, 1, '2024-01-05');
             SELECT pgft.add_relationship('dt_customers', 'id', 'dt_visits', 'customer_id');
             SELECT pgft.set_time_index('dt_visits', 'day');
             SELECT * FROM pgft.synthesize_features('dt_proj', 'dt_customers', 'id',
                cutoff_time := '2024-02-01', max_depth := 1,
                agg_primitives := ARRAY['avg_time_between', 'time_since_last', 'trend', 'last'],
                windows := ARRAY['30 days']);",
        )
        .unwrap();
        assert_eq!(
            count(r#"SELECT "dt_visits_AVG_TIME_BETWEEN"::bigint FROM dt_proj"#),
            2 * 86400
        );
    }

    /// Keywords such as `desc` were left unquoted in the generated views
    #[pg_test]
    fn test_keyword_identifiers() {
        Spi::run(
            r#"CREATE TABLE kw ("desc" INT PRIMARY KEY, "user" TEXT);
               CREATE TABLE kw_rows (id INT PRIMARY KEY, "desc" INT REFERENCES kw("desc"),
                                     "do" NUMERIC, "window" TIMESTAMPTZ);
               INSERT INTO kw VALUES (1, 'a');
               INSERT INTO kw_rows VALUES (1, 1, 2, '2024-01-01'), (2, 1, 3, '2024-01-02');
               SELECT pgft.add_relationship('kw', 'desc', 'kw_rows', 'desc');
               SELECT pgft.set_time_index('kw_rows', 'window');
               SELECT * FROM pgft.synthesize_features('kw_proj', 'kw', 'desc',
                  cutoff_interval := '1 day', max_depth := 1);"#,
        )
        .unwrap();
        assert!(count("SELECT COUNT(*) FROM kw_proj_train") > 0);
        assert_eq!(count("SELECT COUNT(*) FROM kw_proj_live"), 1);
    }

    /// `tables` listing only related tables dropped every relationship of the target
    #[pg_test]
    fn test_tables_filter_implies_target() {
        setup_shop("tf");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features('tf_proj', 'tf_customers', 'id',
                tables := ARRAY['tf_orders'], agg_primitives := ARRAY['count'],
                trans_primitives := ARRAY[]::text[])",
        )
        .unwrap();
        assert_eq!(
            count("SELECT feature_count FROM pgft.feature_matrices WHERE project_name = 'tf_proj'"),
            1
        );
        assert_eq!(
            count(r#"SELECT "tf_orders_COUNT" FROM tf_proj WHERE id = 1"#),
            3
        );
    }

    #[pg_test]
    #[should_panic(expected = "Column not found: public.vc_customers.nope")]
    fn test_unknown_target_id_rejected() {
        setup_shop("vc");
        Spi::run("SELECT * FROM pgft.synthesize_features('vc_proj', 'vc_customers', 'nope')")
            .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "only one of cutoff_time, cutoff_times_table and cutoff_interval")]
    fn test_conflicting_cutoffs_rejected() {
        setup_shop("cc");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features('cc_proj', 'cc_customers', 'id',
                cutoff_time := '2024-03-01', cutoff_interval := '1 month')",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "cutoff_interval must be a positive interval")]
    fn test_non_positive_cutoff_interval_rejected() {
        setup_shop("ni");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features('ni_proj', 'ni_customers', 'id',
                cutoff_interval := '-1 month')",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "window '-1 day' is not a positive interval")]
    fn test_negative_window_rejected() {
        setup_shop("nw");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features('nw_proj', 'nw_customers', 'id',
                windows := ARRAY['-1 day'])",
        )
        .unwrap();
    }

    #[pg_test]
    #[should_panic(expected = "invalid input syntax for type interval")]
    fn test_invalid_window_rejected() {
        // Rejected up front, before any SQL is generated
        setup_shop("iw");
        Spi::run(
            "SELECT * FROM pgft.synthesize_features('iw_proj', 'iw_customers', 'id',
                windows := ARRAY['banana'])",
        )
        .unwrap();
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
