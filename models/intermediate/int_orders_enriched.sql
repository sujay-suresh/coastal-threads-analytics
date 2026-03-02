-- int_orders_enriched: join orders + items + payments for order-level metrics
with orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

order_items as (

    select * from {{ ref('stg_shopify__order_items') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

item_summary as (

    select
        order_id,
        count(*) as item_count,
        sum(quantity) as total_quantity,
        sum(total_price) as subtotal

    from order_items
    group by order_id

),

enriched as (

    select
        o.order_id,
        o.customer_id,
        o.order_at,
        o.order_status,
        o.discount_pct,
        o.shipping_cost,
        coalesce(i.item_count, 0) as item_count,
        coalesce(i.total_quantity, 0) as total_quantity,
        coalesce(i.subtotal, 0) as subtotal,
        coalesce(i.subtotal, 0) + o.shipping_cost as order_total,
        p.payment_id,
        p.payment_method,
        p.payment_status,
        p.payment_amount,
        p.payment_at

    from orders as o
    left join item_summary as i on o.order_id = i.order_id
    left join payments as p on o.order_id = p.order_id

)

select * from enriched
