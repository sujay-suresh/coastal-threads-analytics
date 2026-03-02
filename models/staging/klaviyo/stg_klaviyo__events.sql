with source as (

    select * from {{ source('klaviyo', 'raw_events') }}

),

renamed as (

    select
        event_id,
        customer_id,
        event_type,
        channel,
        event_at,
        page_url,
        session_id,
        now() as loaded_at

    from source

)

select * from renamed
