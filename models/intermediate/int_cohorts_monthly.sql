-- int_cohorts_monthly: monthly acquisition cohorts with retention flags
with customers as (

    select * from {{ ref('stg_shopify__customers') }}

),

orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

customer_cohorts as (

    select
        customer_id,
        date_trunc('month', signup_at)::date as cohort_month

    from customers

),

customer_orders as (

    select
        o.customer_id,
        c.cohort_month,
        o.order_id,
        o.order_at,
        extract(day from o.order_at - c.cohort_month::timestamp)::int as days_since_cohort

    from orders as o
    inner join customer_cohorts as c on o.customer_id = c.customer_id

),

retention_flags as (

    select
        customer_id,
        cohort_month,
        count(distinct order_id) as total_orders,
        min(order_at) as first_order_at,
        max(order_at) as last_order_at,
        max(case when days_since_cohort <= 30 then 1 else 0 end)::boolean as is_retained_30d,
        max(case when days_since_cohort <= 60 then 1 else 0 end)::boolean as is_retained_60d,
        max(case when days_since_cohort <= 90 then 1 else 0 end)::boolean as is_retained_90d,
        max(case when days_since_cohort <= 180 then 1 else 0 end)::boolean as is_retained_180d,
        max(case when days_since_cohort <= 365 then 1 else 0 end)::boolean as is_retained_365d,
        max(case when days_since_cohort > 30 then 1 else 0 end)::boolean as has_repeat_purchase

    from customer_orders
    group by customer_id, cohort_month

)

select * from retention_flags
