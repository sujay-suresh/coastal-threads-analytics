with source as (

    select * from {{ source('shopify', 'raw_order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        total_price,
        now() as loaded_at

    from source

)

select * from renamed
