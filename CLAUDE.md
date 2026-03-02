# Coastal Threads Analytics

## Project Type
dbt + Python + Streamlit e-commerce retention analytics pipeline (PostgreSQL backend).

## Stack
- **Database:** PostgreSQL 18 — localhost:5432, user: portfolio, password: portfolio_dev, db: portfolio, schema: `coastal_threads`
- **dbt:** dbt-core 1.11.6 + dbt-postgres 1.10.0. Run via `bash run_dbt.sh <command>` (Python 3.13 wrapper for Windows compatibility)
- **Python:** 3.13 for dbt, Anaconda for Streamlit
- **Dashboard:** Streamlit + Plotly (3 tabs)
- **Packages:** dbt-utils (>=1.3.0), dbt-expectations (calogica, >=0.10.0)

## Commands
```bash
# Data generation
python scripts/generate_synthetic_data.py

# dbt pipeline
bash run_dbt.sh deps
bash run_dbt.sh build --full-refresh
bash run_dbt.sh test

# Dashboard
streamlit run dashboards/app.py
```

## Key Files
| File | Purpose |
|------|---------|
| `scripts/generate_synthetic_data.py` | Generates 30K customers, 55K orders, 95K items, 393K events |
| `dashboards/app.py` | Streamlit dashboard (3 tabs, ~428 lines) |
| `run_dbt.sh` | dbt wrapper using Python 3.13 programmatic invocation |
| `docs/architecture_diagram.md` | Mermaid pipeline + ER diagrams |
| `docs/data_dictionary.md` | Column-level documentation for all mart models |

## Data Architecture

### Sources (3 simulated APIs)
- **Shopify:** raw_customers (30K), raw_orders (55K), raw_order_items (95K), raw_products (200)
- **Stripe:** raw_payments (55K)
- **Klaviyo:** raw_events (393K)

### Layers
| Layer | Materialization | Models |
|-------|----------------|--------|
| Staging | VIEW | 6 models: `stg_shopify__*`, `stg_stripe__*`, `stg_klaviyo__*` |
| Intermediate | TABLE | 4 models: `int_orders_enriched`, `int_customers_rfm_scored`, `int_orders_attributed`, `int_cohorts_monthly` |
| Marts | TABLE | 6 models: `dim_date`, `dim_customers` (SCD2), `dim_products`, `dim_channels`, `fct_orders`, `fct_customer_events` |

### Star Schema (Kimball)
- **Facts:** `fct_orders` (grain: order line item, 95K rows), `fct_customer_events` (grain: event, 393K rows)
- **Dimensions:** `dim_customers` (SCD Type 2), `dim_products`, `dim_date` (2023-07 to 2026-06), `dim_channels` (8 channels, 5 categories)
- **Surrogate keys:** via `dbt_utils.generate_surrogate_key()`

## Business Logic

### RFM Segmentation
- NTILE(5) scoring over recency, frequency, monetary (trailing 12 months)
- 7 segments: Champions, Loyal, New, Promising, Needs Attention, At Risk, Hibernating
- Non-purchasers default to score=1 in all dimensions

### Last-Touch Attribution
- Attributable events: ad_click, email_click, organic_visit, referral_click
- Most recent event before each order via ROW_NUMBER window function
- Coverage: ~79% (test threshold: >=75%)

### Cohort Retention
- Monthly acquisition cohorts with retention flags at 30/60/90/180/365 days
- Visualized as RdYlGn heatmap in dashboard

## Dashboard (Streamlit)
| Tab | Content |
|-----|---------|
| Executive KPIs | Revenue trend (daily + 7-day avg), AOV by month, cumulative customer growth, 5 metric cards |
| RFM Segments | Segment distribution bar, segment x metric heatmap, revenue share donut, detail table |
| Channel Attribution | Revenue by channel, retention by first-touch channel, cohort retention heatmap, channel summary table |

## Testing
- **57 YAML tests:** unique, not_null, relationships (FK integrity), accepted_values
- **3 custom SQL tests:**
  - `assert_rfm_scores_valid.sql` — scores in [1,5]
  - `assert_attribution_coverage.sql` — >=75% coverage
  - `assert_no_orders_before_signup.sql` — temporal integrity

## Conventions
- Staging: `stg_<source>__<entity>`
- Intermediate: `int_<entity>_<verb>`
- Marts: `dim_<entity>`, `fct_<entity>`
- Timestamps: `<event>_at` (UTC)
- IDs: `<entity>_id` (natural), `<entity>_key` (surrogate)
- Booleans: `is_<x>` or `has_<x>`
- All models use `{{ ref() }}` / `{{ source() }}`, never hardcoded table names

## Synthetic Data
- Seed: 42 (deterministic)
- Date range: 2024-01-01 to 2025-12-31
- 8 product categories (fashion DTC), 200 SKUs
- Power-law order frequency (68% non-repeat customers)
- Event types weighted for attribution coverage
- DB URL hardcoded in script: `postgresql://portfolio:portfolio_dev@localhost:5432/portfolio`
