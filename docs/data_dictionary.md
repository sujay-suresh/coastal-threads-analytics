# Data Dictionary

## Mart Models

### dim_customers
Customer dimension with RFM segments and lifetime metrics. SCD Type 2 with effective_from/effective_to.

| Column | Type | Description |
|--------|------|-------------|
| customer_key | varchar | Surrogate key (hash) |
| customer_id | int | Natural key from Shopify |
| email | varchar | Customer email |
| first_name | varchar | First name |
| last_name | varchar | Last name |
| gender | varchar | Gender (F, M, NB) |
| age | int | Customer age |
| state | varchar | US state code |
| signup_at | timestamp | Account creation timestamp |
| is_email_opted_in | boolean | Email marketing opt-in |
| recency_score | int | RFM recency (1-5, 5 = most recent) |
| frequency_score | int | RFM frequency (1-5, 5 = most orders) |
| monetary_score | int | RFM monetary (1-5, 5 = highest spend) |
| rfm_segment | varchar | Segment: Champions, Loyal, New, Promising, At Risk, Hibernating, Needs Attention |
| lifetime_order_count | int | Total orders placed |
| lifetime_revenue | numeric | Total revenue generated |
| avg_order_value | numeric | Average order value |
| first_order_at | timestamp | First order timestamp |
| last_order_at | timestamp | Most recent order timestamp |
| is_repeat_customer | boolean | Has placed 2+ orders |
| effective_from | timestamp | SCD2 record start |
| effective_to | timestamp | SCD2 record end (9999-12-31 for current) |
| is_current | boolean | Current record flag |

### dim_products
Product dimension with category hierarchy.

| Column | Type | Description |
|--------|------|-------------|
| product_key | varchar | Surrogate key (hash) |
| product_id | int | Natural key from Shopify |
| sku | varchar | Stock keeping unit |
| product_name | varchar | Display name |
| category | varchar | Top-level category (8 categories) |
| subcategory | varchar | Subcategory within category |
| base_price | numeric | Base retail price (USD) |
| cost_price | numeric | Cost of goods (USD) |
| margin | numeric | base_price - cost_price |
| margin_pct | numeric | Margin as percentage |
| is_active | boolean | Currently active in catalog |
| created_at | timestamp | Added to catalog |

### dim_date
Date dimension with fiscal periods and flags.

| Column | Type | Description |
|--------|------|-------------|
| date_key | varchar | Surrogate key (hash) |
| date_day | date | Calendar date |
| calendar_year | int | Calendar year |
| calendar_quarter | int | Calendar quarter (1-4) |
| calendar_month | int | Calendar month (1-12) |
| week_of_year | int | ISO week number |
| day_of_week_num | int | Day of week (0=Sun, 6=Sat) |
| day_of_week_name | varchar | Day name |
| month_name | varchar | Month name |
| day_of_month | int | Day of month (1-31) |
| fiscal_year | int | Fiscal year (July start) |
| fiscal_quarter | int | Fiscal quarter |
| is_weekend | boolean | Saturday or Sunday |
| is_holiday | boolean | US holiday flag |

### dim_channels
Marketing channel taxonomy.

| Column | Type | Description |
|--------|------|-------------|
| channel_key | varchar | Surrogate key (hash) |
| channel | varchar | Channel name (paid_social, email, etc.) |
| channel_category | varchar | Grouping: Paid, Organic, Owned, Direct, Earned |
| is_paid | boolean | Whether channel requires ad spend |

### fct_orders
Order fact table. **Grain: one row per order line item.**

| Column | Type | Description |
|--------|------|-------------|
| order_line_key | varchar | Surrogate key (hash) |
| order_item_id | int | Natural key for line item |
| order_id | int | Order identifier |
| customer_key | varchar | FK to dim_customers |
| product_key | varchar | FK to dim_products |
| date_key | varchar | FK to dim_date |
| channel_key | varchar | FK to dim_channels |
| order_at | timestamp | Order placement timestamp |
| order_status | varchar | Order status |
| attributed_channel | varchar | Last-touch attributed channel |
| channel_group | varchar | Channel group (incl. unattributed) |
| quantity | int | Quantity ordered |
| unit_price | numeric | Price per unit after discount |
| revenue | numeric | Line item total (unit_price x quantity) |
| discount_pct | numeric | Discount percentage applied |
| shipping_cost | numeric | Shipping cost (USD) |
| payment_method | varchar | Payment method used |
| payment_status | varchar | Payment status |

### fct_customer_events
Customer event fact table. **Grain: one row per individual event.**

| Column | Type | Description |
|--------|------|-------------|
| event_key | varchar | Surrogate key (hash) |
| event_id | int | Natural key for event |
| customer_key | varchar | FK to dim_customers |
| date_key | varchar | FK to dim_date |
| channel_key | varchar | FK to dim_channels |
| event_type | varchar | Event type (page_view, email_open, ad_click, etc.) |
| channel | varchar | Marketing channel |
| event_at | timestamp | Event timestamp |
| page_url | varchar | Page URL (for page_view events) |
| session_id | varchar | Session identifier |

## Intermediate Models

### int_orders_enriched
Orders joined with item aggregates and payment details. Grain: one row per order.

### int_customers_rfm_scored
Customer-level RFM scoring using NTILE(5) over trailing 12 months. Grain: one row per customer.

### int_orders_attributed
Last-touch attribution: each order mapped to the most recent marketing event before purchase. Grain: one row per order.

### int_cohorts_monthly
Monthly acquisition cohorts with retention flags at 30/60/90/180/365 days. Grain: one row per customer.
