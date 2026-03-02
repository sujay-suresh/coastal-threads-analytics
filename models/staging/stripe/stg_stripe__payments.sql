with source as (

    select * from {{ source('stripe', 'raw_payments') }}

),

renamed as (

    select
        payment_id,
        order_id,
        amount as payment_amount,
        currency,
        payment_method,
        status as payment_status,
        created_at as payment_at,
        now() as loaded_at

    from source

)

select * from renamed
