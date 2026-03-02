with source as (

    select * from {{ source('shopify', 'raw_products') }}

),

renamed as (

    select
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        base_price,
        cost_price,
        is_active,
        created_at,
        now() as loaded_at

    from source

)

select * from renamed
