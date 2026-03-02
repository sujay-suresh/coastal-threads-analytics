-- dim_products: product hierarchy (category > subcategory > SKU)
-- Grain: one row per product
with products as (

    select * from {{ ref('stg_shopify__products') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        base_price,
        cost_price,
        base_price - cost_price as margin,
        case
            when base_price > 0
            then round(((base_price - cost_price) / base_price * 100)::numeric, 1)
            else 0
        end as margin_pct,
        is_active,
        created_at

    from products

)

select * from final
