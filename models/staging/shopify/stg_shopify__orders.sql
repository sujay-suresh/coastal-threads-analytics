with source as (

    select * from {{ source('shopify', 'raw_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        order_at,
        status as order_status,
        discount_pct,
        shipping_cost,
        is_deleted,
        now() as loaded_at

    from source
    where not is_deleted

)

select * from renamed
