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

Auto-discover time indexes: timestamp or date columns with a common time column name, one per table.
Preference order: `created_at`, `occurred_at`, `event_time`, `event_timestamp`, `transaction_time`,
`order_date`, `purchase_date`, `timestamp`, `datetime`, `date`, `time`, and `updated_at` last (it moves
when a row changes). Tables that already have a time index are skipped.

```sql
SELECT pgft.discover_time_indexes('public');
```

**Parameters:**
- `schema_name TEXT DEFAULT 'public'` - Schema to scan

**Returns:** `INTEGER` - Number of time indexes registered

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

Auto-discover relationships from the foreign keys of tables in `schema_name` (the referenced table may
be in any schema). Composite foreign keys are skipped: a relationship joins on one column.

```sql
SELECT pgft.discover_relationships('public');
```

**Parameters:**
- `schema_name TEXT DEFAULT 'public'` - Schema to scan

**Returns:** `INTEGER` - Number of new relationships registered

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
- `tables TEXT[] DEFAULT NULL` - Related tables to include (NULL = all related); the target is always included
- `agg_primitives TEXT[] DEFAULT NULL` - Aggregation primitives (see [Primitives](#primitives); unknown names are an error)
- `trans_primitives TEXT[] DEFAULT NULL` - Transform primitives (see [Primitives](#primitives))
- `max_depth INTEGER DEFAULT NULL` - Maximum feature depth (0-2; deeper behaves as 2)
- `output_name TEXT DEFAULT NULL` - Name for output materialized view
- `if_exists TEXT DEFAULT 'error'` - 'error' or 'replace'

Pass at most one of `cutoff_time`, `cutoff_times_table` and `cutoff_interval`. `cutoff_interval` and
every entry of `windows` must be positive intervals. The cutoff table needs the `target_id` column and
the `cutoff_time_column`.

**Returns:** `TABLE (project_name, feature_count, row_count, output_name)`

**Output Objects (when using per-entity cutoffs):**
- `{output_name}` - Materialized view with `cutoff_time` column (for train/test splitting, refresh with `REFRESH MATERIALIZED VIEW`)
- `{output_name}_train` - View selecting from materialized view, excluding `cutoff_time` (ML ready, use `SELECT *`)
- `{output_name}_live` - View with `cutoff = now()` (for real-time inference). Computed with exactly the
  same SQL expressions as the matrix, so a model scores on the features it was trained on

With `cutoff_interval`, the generated cutoffs are kept in `pgft."_cutoffs_<project_name>"` (the matrix
reads from it) and replaced on the next run.

**Cutoff semantics:**
- A child row counts iff its time index is `<= cutoff`; a window `w` also requires `> cutoff - w`.
  Without a cutoff, windows and `time_since*` are relative to `now()`
- If the **target** has a time index, an entity only appears at cutoffs at or after its own time
- Windows are only generated for child tables that have a time index
- At depth 2, cutoff and windows apply to both the child and the grandchild rows

**Feature names** (columns are in this order, deterministically):

| Feature | Name | Example |
|---------|------|---------|
| transform of a target column | `{column}_{PRIM}` | `signup_date_YEAR` |
| row count / time-index aggregation | `{child}_{PRIM}` | `orders_COUNT`, `orders_TIME_SINCE_LAST` |
| column aggregation | `{child}_{PRIM}_{column}` | `orders_SUM_amount` |
| depth 2 (stacked) | `{child}_{OUTER}_{grandchild}_{INNER}[_{column}]` | `orders_MEAN_order_items_SUM_quantity` |
| windowed | name + `_{suffix}` | `orders_COUNT_30d` |

`COUNT` counts rows (one feature per relationship). Primary keys and relationship key columns of
child tables are never aggregated. When a parent has two relationships to the same table, `{child}`
becomes `{child}_{fk_column}`. Names over 63 bytes are shortened with a hash suffix; the full name is
kept in the feature's description.

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

An existing `output_table` is replaced, unless a feature matrix was built on it: then the call fails
(drop or rebuild that matrix first) instead of silently dropping the matrix. `num_windows` must be at
least 1 and `window_size` positive.

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
SELECT pgft.explain_feature('customer_features', 'orders_SUM_amount');
```

**Parameters:**
- `project_name TEXT` - Project identifier
- `feature_name TEXT` - Feature name

**Returns:** `TEXT` - Human-readable explanation

### list_primitives

List the available primitives.

```sql
SELECT * FROM pgft.list_primitives();
```

**Returns:** `TABLE (name, primitive_type, input, input_types, needs_time_index, description)`

## Primitives

**Aggregation** (applied to the rows of a child table, per target entity):

| Name | Applies to | Description |
|------|------------|-------------|
| `count` | rows | Number of rows |
| `sum`, `mean`, `std`, `median`, `min`, `max` | numeric columns | |
| `mode` | int/numeric/bool/text columns | Most common value |
| `num_unique` | int/numeric/bool/text/date/timestamp columns | Count of distinct values |
| `percent_true`, `any`, `all` | boolean columns | |
| `first`, `last` | int/numeric/bool/text columns, needs time index | Earliest / latest value |
| `trend` | numeric columns, needs time index | Slope of a linear fit, change per day |
| `time_since_last`, `time_since_first` | time index | Seconds from the latest / earliest row to the cutoff |
| `avg_time_between` | time index | Average seconds between rows |

At depth 2, an inner aggregation with a numeric result is aggregated again with `sum`, `mean`,
`std`, `median`, `min` or `max` (when listed in `agg_primitives`).

**Transform** (applied to target table columns):

| Name | Applies to |
|------|------------|
| `year`, `quarter`, `month`, `week`, `day`, `weekday`, `is_weekend` | date/timestamp |
| `hour`, `minute` | timestamp/time |
| `time_since` | date/timestamp — seconds to the cutoff |
| `absolute` | numeric |
| `num_characters`, `num_words` | text |
| `is_null` | any |

## Configuration

```sql
SET pg_feature.default_depth = 2;
SET pg_feature.default_agg_primitives = 'sum,mean,count,min,max';
SET pg_feature.default_trans_primitives = 'year,month,day';
SET pg_feature.max_features = 500;
```
