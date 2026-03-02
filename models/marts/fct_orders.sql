-- fct_orders: order fact table
-- Grain: one row per order line item
with order_items as (

    select * from {{ ref('stg_shopify__order_items') }}

),

orders as (

    select * from {{ ref('int_orders_enriched') }}

),

attribution as (

    select * from {{ ref('int_orders_attributed') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_customers as (

    select * from {{ ref('dim_customers') }}

),

dim_products as (

    select * from {{ ref('dim_products') }}

),

dim_channels as (

    select * from {{ ref('dim_channels') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['oi.order_item_id']) }} as order_line_key,
        oi.order_item_id,
        oi.order_id,
        dc.customer_key,
        dp.product_key,
        dd.date_key,
        coalesce(dch.channel_key, {{ dbt_utils.generate_surrogate_key(["'unattributed'"]) }}) as channel_key,
        o.order_at,
        o.order_status,
        a.attributed_channel,
        a.channel_group,
        oi.quantity,
        oi.unit_price,
        oi.total_price as revenue,
        o.discount_pct,
        o.shipping_cost,
        o.payment_method,
        o.payment_status

    from order_items as oi
    inner join orders as o on oi.order_id = o.order_id
    left join attribution as a on oi.order_id = a.order_id
    left join dim_date as dd on cast(o.order_at as date) = dd.date_day
    left join dim_customers as dc on o.customer_id = dc.customer_id and dc.is_current
    left join dim_products as dp on oi.product_id = dp.product_id
    left join dim_channels as dch on a.attributed_channel = dch.channel

)

select * from final
