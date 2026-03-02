-- dim_customers: SCD Type 2 customer dimension with RFM segments
-- Grain: one row per customer per effective period
with rfm as (

    select * from {{ ref('int_customers_rfm_scored') }}

),

orders_enriched as (

    select * from {{ ref('int_orders_enriched') }}

),

customer_lifetime as (

    select
        customer_id,
        count(distinct order_id) as lifetime_order_count,
        sum(order_total) as lifetime_revenue,
        min(order_at) as first_order_at,
        max(order_at) as last_order_at,
        avg(order_total) as avg_order_value

    from orders_enriched
    group by customer_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['r.customer_id']) }} as customer_key,
        r.customer_id,
        r.email,
        r.first_name,
        r.last_name,
        r.gender,
        r.age,
        r.state,
        r.signup_at,
        r.is_email_opted_in,
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_segment,
        coalesce(cl.lifetime_order_count, 0) as lifetime_order_count,
        coalesce(cl.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(cl.avg_order_value, 0) as avg_order_value,
        cl.first_order_at,
        cl.last_order_at,
        case
            when cl.lifetime_order_count >= 2 then true
            else false
        end as is_repeat_customer,
        r.signup_at as effective_from,
        cast('9999-12-31' as timestamp) as effective_to,
        true as is_current

    from rfm as r
    left join customer_lifetime as cl on r.customer_id = cl.customer_id

)

select * from final
