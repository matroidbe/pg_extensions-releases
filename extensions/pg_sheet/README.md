# pg_sheet — Domain-Aware Spreadsheet Overlays for PostgreSQL

Transparent overlay tables on top of existing entity tables, enabling spreadsheet-like editing with formulas, snapshots, audit trails, and cell locking — without modifying the source data.

## Quick Start

```sql
CREATE EXTENSION pg_sheet;

-- Create a spreadsheet overlay on an existing table
SELECT pgsheet.create_sheet('pipeline', 'sales', 'deal', 'status IN (''prospect'', ''negotiation'')');

-- Add overlay columns (only exist in the sheet)
SELECT pgsheet.add_column('pipeline', 'confidence', 'numeric');
SELECT pgsheet.add_column('pipeline', 'notes', 'text');

-- Add formula columns (computed in SQL)
SELECT pgsheet.add_formula('pipeline', 'weighted', '={revenue} * {confidence}');
SELECT pgsheet.add_formula('pipeline', 'risk_label', '=IF({confidence} > 0.7, "high", "low")');

-- Set cell values in the overlay
SELECT pgsheet.set_value('pipeline', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'confidence', '0.85');
SELECT pgsheet.set_value('pipeline', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'notes', 'Key account');

-- Set a per-cell formula (Excel-style)
SELECT pgsheet.set_formula('pipeline', 'a0eebc99-...', 'custom', '=SUM(B2:B10)');

-- Read the merged view (source + overlay + formulas)
SELECT * FROM pgsheet."_view_pipeline";
-- Or via the API:
SELECT pgsheet.get_data('pipeline', 100, 0);

-- Take a snapshot before experimenting
SELECT pgsheet.snapshot('pipeline', 'before Q4 adjustments');

-- Make changes...
-- Then diff or restore:
SELECT * FROM pgsheet.diff('pipeline', 'snapshot-uuid-here');
SELECT pgsheet.restore('pipeline', 'snapshot-uuid-here');

-- Cell locking for collaboration
SELECT pgsheet.lock_cell('pipeline', 'a0eebc99-...', 'revenue');
SELECT pgsheet.unlock_cell('pipeline', 'a0eebc99-...', 'revenue');

-- Audit trail
SELECT * FROM pgsheet.cell_history('pipeline', 'a0eebc99-...', 'confidence');
SELECT * FROM pgsheet.sheet_changes('pipeline');

-- Cleanup
SELECT pgsheet.drop_sheet('pipeline');
```

## Architecture

```
┌─────────────────────────────────┐
│  Merged View (_view_pipeline)   │  ← user queries this
│  COALESCE(overlay, source)      │
│  + formula SQL expressions      │
├─────────────────────────────────┤
│  Overlay (_overlay_pipeline)    │  ← per-cell overrides & formulas
│  entity_id | col_name | value   │
├─────────────────────────────────┤
│  Source Table (sales.deal)      │  ← untouched, read-only
└─────────────────────────────────┘
```

## Formula Support

### Column Formulas (SQL-translatable)

Use `{column_name}` references — these compile to SQL and run in PostgreSQL:

```sql
SELECT pgsheet.add_formula('pipeline', 'weighted', '={revenue} * {confidence}');
SELECT pgsheet.add_formula('pipeline', 'label', '=IF({score} > 0.5, "good", "bad")');
SELECT pgsheet.add_formula('pipeline', 'total', '=COALESCE({adjusted}, {revenue})');
```

### Cell Formulas (Excel-style)

Use `A1:B2` cell references — stored in overlay, evaluated client-side by HyperFormula:

```sql
SELECT pgsheet.set_formula('pipeline', 'uuid-here', 'total', '=SUM(B2:B10)');
SELECT pgsheet.set_formula('pipeline', 'uuid-here', 'pct', '=B2/B$1*100');
```

### Supported Functions (SQL translation)

| Excel | SQL | Notes |
|-------|-----|-------|
| SUM | SUM | Aggregate |
| AVERAGE | AVG | Aggregate |
| MIN/MAX | LEAST/GREATEST | Per-row; SUM for aggregate |
| COUNT | COUNT | Aggregate |
| IF | CASE WHEN | Conditional |
| COALESCE | COALESCE | Null handling |
| ABS, ROUND, FLOOR, CEIL | Same | Math |
| UPPER, LOWER, TRIM, LEN | Same | String |
| CONCATENATE | CONCAT | String concat |
| NOW, TODAY | NOW(), CURRENT_DATE | Date |

## API Reference

### Sheet Lifecycle
- `create_sheet(name, source_schema, source_table, filter?)` — Create overlay
- `drop_sheet(name)` — Drop everything
- `list_sheets()` — List all sheets

### Columns
- `add_column(sheet, name, type, default?)` — Add overlay column
- `add_formula(sheet, name, formula)` — Add formula column
- `drop_column(sheet, name)` — Remove overlay/formula column
- `rename_column(sheet, old, new)` — Rename column
- `list_columns(sheet)` — List all columns

### Cells
- `set_value(sheet, entity_id, col, value)` — Set cell value
- `set_formula(sheet, entity_id, col, formula)` — Set cell formula
- `clear_cell(sheet, entity_id, col)` — Revert to source
- `get_cell(sheet, entity_id, col)` — Get cell metadata
- `set_values(sheet, jsonb_array)` — Bulk update
- `get_data(sheet, limit, offset)` — Read merged view
- `parse_formula(formula)` — Validate without storing

### Snapshots
- `snapshot(sheet, label?)` — Capture current state
- `restore(sheet, snapshot_id)` — Restore snapshot
- `diff(sheet, snapshot_id)` — Compare changes
- `list_snapshots(sheet)` — List snapshots

### Audit
- `cell_history(sheet, entity_id, col, limit?)` — Cell change log
- `sheet_changes(sheet, limit?)` — Recent sheet changes

### Locking
- `lock_cell(sheet, entity_id, col)` — Lock for editing
- `unlock_cell(sheet, entity_id, col, force?)` — Release lock
- `locked_cells(sheet)` — List active locks
- `expire_locks(sheet, interval)` — Cleanup stale locks
