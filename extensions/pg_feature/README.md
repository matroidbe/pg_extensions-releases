# pg_feature - Automated Feature Engineering

Deep Feature Synthesis (DFS) as pure SQL. Automated feature engineering directly in PostgreSQL without Python dependencies.

## Installation

```bash
cargo pgrx install --release
```

## Schema

`pgft`

## SQL API

### set_time_index

Register a time index for a table to enable cutoff time filtering for ML.

```sql
SELECT pgft.set_time_index('orders', 'created_at');
```

**Parameters:**
- `table_name TEXT` - Table name (schema.table or just table)
- `time_column TEXT` - Column containing timestamp

**Returns:** `BIGINT` - Time index ID

### list_time_indexes

List all registered time indexes.

```sql
SELECT * FROM pgft.list_time_indexes();
```

**Returns:** `TABLE (id BIGINT, schema_name TEXT, table_name TEXT, time_column TEXT)`

### remove_time_index

Remove a time index for a table.

```sql
SELECT pgft.remove_time_index('orders');
```

**Parameters:**
- `table_name TEXT` - Table name

**Returns:** `BOOLEAN` - True if removed

### discover_time_indexes

Auto-discover time indexes by scanning for timestamp columns (created_at, updated_at, timestamp, etc.).

```sql
SELECT pgft.discover_time_indexes('public');
```

**Parameters:**
- `schema_name TEXT DEFAULT 'public'` - Schema to scan

**Returns:** `INTEGER` - Number of time indexes discovered

### add_relationship

Add a relationship between two tables for feature synthesis.

```sql
SELECT pgft.add_relationship('customers', 'id', 'orders', 'customer_id', 'customer_orders');
```

**Parameters:**
- `parent_table TEXT` - Parent table (one side)
- `parent_column TEXT` - Parent key column
- `child_table TEXT` - Child table (many side)
- `child_column TEXT` - Foreign key column
- `relationship_name TEXT DEFAULT NULL` - Optional name

**Returns:** `BIGINT` - Relationship ID

### list_relationships

List all registered relationships.

```sql
SELECT * FROM pgft.list_relationships();
```

**Returns:** `TABLE (id, parent_schema, parent_table, parent_column, child_schema, child_table, child_column, relationship_name)`

### remove_relationship

Remove a relationship by ID.

```sql
SELECT pgft.remove_relationship(1);
```

**Parameters:**
- `relationship_id BIGINT` - Relationship ID

**Returns:** `BOOLEAN` - True if removed

### discover_relationships

Auto-discover relationships from foreign key constraints.

```sql
SELECT pgft.discover_relationships('public');
```

**Parameters:**
- `schema_name TEXT DEFAULT 'public'` - Schema to scan

**Returns:** `INTEGER` - Number of relationships discovered

### synthesize_features

Main entry point for automated feature engineering using Deep Feature Synthesis.

```sql
-- Basic usage with single cutoff
SELECT * FROM pgft.synthesize_features(
    'customer_features',
    'customers',
    'id',
    cutoff_time => '2024-01-01'::timestamptz,
    agg_primitives => ARRAY['sum', 'mean', 'count'],
    max_depth => 2
);

-- Advanced: auto-generate cutoffs with rolling windows for ML training
SELECT * FROM pgft.synthesize_features(
    project_name := 'churn_model',
    target_table := 'public.customers',
    target_id := 'customer_id',
    cutoff_interval := '1 month',  -- Auto-generate monthly cutoffs
    windows := ARRAY['30 days', '90 days', '365 days'],  -- Rolling windows
    max_depth := 2,
    output_name := 'public.churn_features'
);
-- Creates:
--   churn_features (materialized view) - refresh with REFRESH MATERIALIZED VIEW
--   churn_features_train (view) - excludes cutoff_time, ML ready
--   churn_features_live (view) - uses now() as cutoff for inference
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `target_table TEXT` - Entity table to generate features for
- `target_id TEXT` - Primary key column
- `cutoff_time TIMESTAMPTZ DEFAULT NULL` - Single cutoff time
- `cutoff_times_table TEXT DEFAULT NULL` - Table with per-entity cutoffs
- `cutoff_time_column TEXT DEFAULT 'cutoff_time'` - Column name in cutoff table
- `cutoff_interval TEXT DEFAULT NULL` - Interval for auto-generating cutoffs from data (e.g., '1 month')
- `windows TEXT[] DEFAULT NULL` - Rolling window intervals for time-relative features (e.g., ['30 days', '90 days'])
- `tables TEXT[] DEFAULT NULL` - Tables to include (NULL = all related)
- `agg_primitives TEXT[] DEFAULT NULL` - Aggregation primitives (sum, mean, count, min, max, std)
- `trans_primitives TEXT[] DEFAULT NULL` - Transform primitives (year, month, day, hour)
- `max_depth INTEGER DEFAULT NULL` - Maximum feature depth
- `output_name TEXT DEFAULT NULL` - Name for output materialized view
- `if_exists TEXT DEFAULT 'error'` - 'error' or 'replace'

**Returns:** `TABLE (project_name, feature_count, row_count, output_name)`

**Output Objects (when using per-entity cutoffs):**
- `{output_name}` - Materialized view with `cutoff_time` column (for train/test splitting, refresh with `REFRESH MATERIALIZED VIEW`)
- `{output_name}_train` - View selecting from materialized view, excluding `cutoff_time` (ML ready, use `SELECT *`)
- `{output_name}_live` - View with `cutoff = now()` (for real-time inference)

### make_temporal_cutoffs

Generate evenly-spaced cutoff times for time-series feature engineering.

```sql
SELECT pgft.make_temporal_cutoffs(
    'customers',
    'id',
    '2023-01-01'::timestamptz,
    '1 month',
    'customer_cutoffs',
    num_windows => 12
);
```

**Parameters:**
- `target_table TEXT` - Entity table
- `target_id TEXT` - Primary key column
- `start_time TIMESTAMPTZ` - Start of time range
- `window_size TEXT` - Interval between cutoffs (e.g., '1 month')
- `output_table TEXT` - Table to store cutoffs
- `end_time TIMESTAMPTZ DEFAULT NULL` - End of time range
- `num_windows INTEGER DEFAULT NULL` - Number of windows (alternative to end_time)

**Returns:** `BIGINT` - Number of cutoff rows created

### list_features

List features generated for a project.

```sql
SELECT * FROM pgft.list_features('customer_features');
```

**Parameters:**
- `project_name TEXT` - Project identifier

**Returns:** `TABLE (feature_name, feature_type, primitive, base_column, source_table, depth, description)`

### explain_feature

Get human-readable explanation of how a feature was derived.

```sql
SELECT pgft.explain_feature('customer_features', 'orders.SUM(amount)');
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `feature_name TEXT` - Feature name

**Returns:** `TEXT` - Human-readable explanation

## Configuration

```sql
SET pg_feature.default_depth = 2;
SET pg_feature.default_agg_primitives = 'sum,mean,count,min,max';
SET pg_feature.default_trans_primitives = 'year,month,day';
SET pg_feature.max_features = 500;
```
