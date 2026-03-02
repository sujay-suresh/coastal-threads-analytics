-- int_orders_attributed: last-touch attribution for each order
with orders as (

    select * from {{ ref('stg_shopify__orders') }}

),

events as (

    select * from {{ ref('stg_klaviyo__events') }}

),

attribution_events as (

    select
        event_id,
        customer_id,
        event_type,
        channel,
        event_at

    from events
    where event_type in ('ad_click', 'email_click', 'organic_visit', 'referral_click')

),

last_touch as (

    select
        o.order_id,
        o.customer_id,
        o.order_at,
        e.event_id as attributed_event_id,
        e.event_type as attributed_event_type,
        e.channel as attributed_channel,
        e.event_at as attributed_event_at,
        row_number() over (
            partition by o.order_id
            order by e.event_at desc
        ) as rn

    from orders as o
    left join attribution_events as e
        on o.customer_id = e.customer_id
        and e.event_at < o.order_at

),

attributed as (

    select
        order_id,
        customer_id,
        order_at,
        attributed_event_id,
        attributed_event_type,
        attributed_channel,
        attributed_event_at,
        case
            when attributed_channel is null then 'unattributed'
            else attributed_channel
        end as channel_group

    from last_touch
    where rn = 1

)

select * from attributed
