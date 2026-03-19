# Scenario Analysis: P&L Impact Under Uncertainty

This example demonstrates combining **pg_prob** (probabilistic sampling) with **pg_ortools** (constraint optimization) to analyze P&L outcomes when project demand is uncertain.

## Use Case

A consulting firm has:
- **Potential projects** - each with a probability of being ordered
- **Consultants** - with skills, availability, and daily costs
- **Goal** - understand the P&L distribution across possible futures

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  pg_prob    │────▶│  pg_ortools  │────▶│  pg_prob    │
│  (sample)   │     │  (optimize)  │     │  (aggregate)│
└─────────────┘     └──────────────┘     └─────────────┘
     │                    │                    │
     ▼                    ▼                    ▼
  N scenarios        N optimal plans       P&L distribution
  (which projects)   (who does what)       (percentiles, histogram)
```

## Key Pattern: Checkpointing

Long-running analytics jobs (hours) need fault tolerance. This example shows:

1. **Progress tracking** - know how many samples completed
2. **Resume on failure** - restart from where we left off
3. **Partial results** - use data even if job didn't finish

## Files

| File | Description |
|------|-------------|
| `01-schema.sql` | Tables for projects, consultants, and run tracking |
| `02-sample-scenarios.sql` | Generate Monte Carlo scenarios using pg_prob |
| `03-optimize-with-checkpoint.sql` | Solve each scenario with checkpointing |
| `04-aggregate-results.sql` | Compute P&L distribution using pg_prob |

## Running the Example

```sql
-- Setup
\i 01-schema.sql

-- Insert sample data (or use your own)
\i 01-schema.sql  -- includes sample data

-- Generate scenarios
\i 02-sample-scenarios.sql

-- Run optimization (can take hours for large sample counts)
\i 03-optimize-with-checkpoint.sql

-- Analyze results
\i 04-aggregate-results.sql
```

## Resuming After Failure

If the optimization job fails partway through:

```sql
-- Check progress
SELECT run_id, completed_samples, total_samples, status, last_error
FROM scenario_runs
WHERE status = 'failed'
ORDER BY started_at DESC
LIMIT 1;

-- Resume (the script handles this automatically)
\i 03-optimize-with-checkpoint.sql
```

## Performance Considerations

| Factor | Guidance |
|--------|----------|
| Sample count | Start with 100-500, increase if needed |
| Problem size | More projects/consultants = slower per solve |
| Runtime | Hours is fine for analytics workloads |
| Memory | Results accumulate; use temp tables if needed |

## Interpreting Results

The final output gives you:

- **P5 P&L** - 5th percentile, worst realistic case
- **P50 P&L** - median outcome
- **P95 P&L** - 95th percentile, best realistic case
- **Expected P&L** - probability-weighted average
- **Volatility** - standard deviation, measure of uncertainty

Use these to make informed decisions about capacity, pricing, and risk.
