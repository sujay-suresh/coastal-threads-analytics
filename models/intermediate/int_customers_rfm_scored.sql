-- int_customers_rfm_scored: RFM scoring with NTILE(5) and segment mapping
with customers as (

    select * from {{ ref('stg_shopify__customers') }}

),

orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

order_metrics as (

    select
        customer_id,
        max(order_at) as last_order_at,
        count(*) as order_count_12m,
        sum(0) as placeholder -- will be replaced by real revenue

    from orders
    where order_at >= current_date - interval '365 days'
    group by customer_id

),

order_items as (

    select * from {{ ref('stg_shopify__order_items') }}

),

revenue as (

    select
        o.customer_id,
        sum(oi.total_price) as total_spend_12m

    from orders as o
    inner join order_items as oi on o.order_id = oi.order_id
    where o.order_at >= current_date - interval '365 days'
    group by o.customer_id

),

rfm_base as (

    select
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.gender,
        c.age,
        c.state,
        c.signup_at,
        c.is_email_opted_in,
        om.last_order_at,
        extract(day from current_timestamp - om.last_order_at)::int as days_since_last_order,
        coalesce(om.order_count_12m, 0) as order_count_12m,
        coalesce(r.total_spend_12m, 0) as total_spend_12m

    from customers as c
    left join order_metrics as om on c.customer_id = om.customer_id
    left join revenue as r on c.customer_id = r.customer_id

),

rfm_scored as (

    select
        *,
        case
            when order_count_12m = 0 then 1
            else ntile(5) over (
                partition by case when order_count_12m > 0 then 1 else 0 end
                order by days_since_last_order desc
            )
        end as recency_score,
        case
            when order_count_12m = 0 then 1
            else ntile(5) over (
                partition by case when order_count_12m > 0 then 1 else 0 end
                order by order_count_12m asc
            )
        end as frequency_score,
        case
            when total_spend_12m = 0 then 1
            else ntile(5) over (
                partition by case when total_spend_12m > 0 then 1 else 0 end
                order by total_spend_12m asc
            )
        end as monetary_score

    from rfm_base

),

segmented as (

    select
        *,
        case
            when recency_score = 5 and frequency_score = 5 and monetary_score = 5 then 'Champions'
            when recency_score >= 4 and frequency_score >= 4 then 'Loyal'
            when recency_score = 5 and frequency_score = 1 then 'New'
            when recency_score >= 3 and recency_score <= 4 and frequency_score >= 1 and frequency_score <= 2 then 'Promising'
            when recency_score <= 2 and frequency_score >= 3 then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 then 'Hibernating'
            else 'Needs Attention'
        end as rfm_segment

    from rfm_scored

)

select * from segmented
