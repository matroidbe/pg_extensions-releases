# TechMart Retail Forecasting with Uncertainty

An end-to-end example demonstrating how four pg_extensions work together to build a probabilistic P&L statement with ML-powered revenue forecasts.

## The Business Problem

TechMart, an e-commerce retailer, needs to forecast monthly sales by product category for the upcoming quarter. But point forecasts aren't enough - management needs to understand uncertainty to:

1. **Set realistic targets** with confidence levels
2. **Build a P&L statement** that shows range of possible outcomes
3. **Answer probability questions** like "What's our chance of hitting $80K revenue?"
4. **Perform what-if analysis** on marketing spend, COGS changes, etc.

## The Solution

This example demonstrates the full ML lifecycle inside PostgreSQL:

```
Normalized Data → Feature Engineering → ML Training → Probabilistic P&L
     (SQL)           (pg_feature)        (pg_ml)         (pg_prob)
```

### Extensions Used

| Extension | Purpose | Key Functions |
|-----------|---------|---------------|
| **pg_feature** | Automated feature engineering | `pgft.discover_relationships()`, `set_time_index()` |
| **pg_ml** | ML training with uncertainty | `pgml.setup()`, `create_model()`, `predict_interval()` |
| **pg_prob** | Probabilistic P&L calculations | `pgprob.triangular()`, `prob_above()`, `mean()` |

## Quick Start

### Using VS Code (Recommended)

Open `retail-forecasting.sqlbook` in VS Code and run each cell in order. The SQLBook format provides:
- Markdown documentation between code blocks
- Execute individual sections independently
- See results inline

### Using Docker

```bash
# Build and start PostgreSQL with all extensions
docker compose up -d

# Wait for startup (extensions compile on first run - takes 5-10 min)
docker compose logs -f postgres

# Connect to database
docker compose exec postgres psql -U postgres

# Copy and run sections from retail-forecasting.sqlbook
```

### Manual Setup

If you have the extensions installed locally:

```bash
psql -d your_database
# Then copy/paste sections from retail-forecasting.sqlbook
```

## Files

| File | Description |
|------|-------------|
| `retail-forecasting.sqlbook` | Complete example as VS Code SQL notebook |
| `Dockerfile` | Multi-stage build with pgbrew |
| `docker-compose.yml` | Docker setup for running the example |

## Schema Overview

```
retail.customers         retail.products
├── customer_id (PK)     ├── product_id (PK)
├── customer_name        ├── product_name
├── country              ├── category
├── signup_date          ├── unit_price
└── is_premium           └── cost_price
        │                        │
        │                        │
        ▼                        ▼
retail.orders ──────────► retail.order_items
├── order_id (PK)        ├── item_id (PK)
├── customer_id (FK)     ├── order_id (FK)
├── order_date           ├── product_id (FK)
├── status               ├── quantity
└── total_amount         └── discount_percent
        │
        │ (aggregated)
        ▼
retail.monthly_category_sales ──► retail.ml_features ──► retail.revenue_predictions
├── category                      (lag features,          (pg_prob.dist type)
├── year_month                     moving averages)
├── revenue
└── cost_of_goods                                               │
                                                                ▼
                                                    retail.pnl_forecast
                                                    (probabilistic P&L)
```

## SQLBook Sections

The notebook is organized into 9 sections:

1. **Extensions Setup** - Install and configure pg_feature, pg_ml, pg_prob
2. **Schema Setup** - Create retail schema
3. **Sample Data** - Generate 2+ years of e-commerce data (100 customers, 20 products, ~2500 orders)
4. **Feature Engineering** - Create ML features (lags, moving averages, temporal)
5. **Model Training** - Train XGBoost with conformal prediction for uncertainty
6. **Probabilistic Predictions** - Generate revenue forecasts as distributions
7. **Probabilistic P&L** - Build P&L where every line propagates uncertainty
8. **Business Analysis** - Answer probability questions, VaR, what-if scenarios
9. **Executive Summary** - Dashboard of key metrics and probabilities

## Key Insights

After running this example, you can answer:

### Probability Questions
- "What's the probability we hit $80K revenue?" → **Use `pgprob.prob_above()`**
- "What's our chance of being profitable?" → **Check P(net_income > 0)**

### Risk Analysis
- "What's our worst-case scenario?" → **Value at Risk (VaR) at 5th percentile**
- "How much uncertainty comes from each category?" → **Compare distribution widths**

### What-If Scenarios
- "If we cut marketing to 8%, what happens?" → **Recalculate with different assumptions**
- "If COGS increases 5%, how does profit change?" → **Sensitivity analysis**

## Sample Output

### P&L with Uncertainty Bands

```
Line Item              Pessimistic    Expected      Optimistic
                       (P5)                         (P95)
─────────────────────────────────────────────────────────────
Revenue                $72,000        $85,000       $98,000
- COGS                 $42,000        $48,000       $54,000
= GROSS PROFIT         $28,000        $37,000       $46,000
- TOTAL OPEX           $18,000        $22,000       $26,000
= OPERATING INCOME     $8,000         $15,000       $24,000
- Taxes                $2,000         $3,750        $6,000
= NET INCOME           $6,000         $11,250       $18,000
```

### Key Probabilities

```
Question                           Answer
──────────────────────────────────────────
Prob of hitting $80K revenue       65.2%
Prob of profitable quarter         94.8%
Prob of $15K+ net income           42.3%
```

## The Power of Probabilistic Data

Traditional forecasting gives you: **"We expect $85,000 revenue."**

With pg_prob, you get: **"We expect $85,000 revenue with 90% confidence between $72,000 and $98,000. There's a 65% chance of hitting our $80K target."**

This enables better business decisions because:
- **Targets are realistic** - you know the confidence level
- **Risks are quantified** - VaR shows worst-case scenarios
- **Scenarios are easy** - arithmetic on distributions propagates uncertainty

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     PostgreSQL                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                   │
│  │pg_feature│  │  pg_ml   │  │ pg_prob  │                   │
│  │  (DFS)   │  │(PyCaret) │  │ (Monte   │                   │
│  │          │  │          │  │  Carlo)  │                   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                   │
│       │             │             │                          │
│       ▼             ▼             ▼             ▼           │
│  ┌────────────────────────────────────────────────────┐    │
│  │                   SQL Interface                     │    │
│  │   CREATE EXTENSION, SELECT, INSERT, arithmetic     │    │
│  └────────────────────────────────────────────────────┘    │
│                            │                                │
│                            ▼                                │
│  ┌────────────────────────────────────────────────────┐    │
│  │                  Data (Tables)                      │    │
│  │  customers, orders, ml_features, pnl_forecast      │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

**All computation happens inside PostgreSQL - no external tools needed!**

## Cleanup

```bash
# Stop and remove containers
docker compose down -v
```

## Next Steps

- Modify the sample data to match your domain
- Add more product categories or customer segments
- Experiment with different ML algorithms (`'lightgbm'`, `'rf'`, etc.)
- Create more complex P&L structures (multiple cost centers, regions)
- Build dashboards using the probabilistic queries

## License

This example is part of the pg_extensions project. See the main repository for license details.
