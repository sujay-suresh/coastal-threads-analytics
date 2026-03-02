with source as (

    select * from {{ source('shopify', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        email,
        first_name,
        last_name,
        gender,
        age,
        state,
        signup_at,
        email_opt_in as is_email_opted_in,
        is_deleted,
        now() as loaded_at

    from source
    where not is_deleted

)

select * from renamed
