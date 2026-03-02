-- fct_customer_events: customer event fact table
-- Grain: one row per individual event
with events as (

    select * from {{ ref('stg_klaviyo__events') }}

),

dim_customers as (

    select * from {{ ref('dim_customers') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_channels as (

    select * from {{ ref('dim_channels') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['e.event_id']) }} as event_key,
        e.event_id,
        dc.customer_key,
        dd.date_key,
        coalesce(dch.channel_key, {{ dbt_utils.generate_surrogate_key(["'unknown'"]) }}) as channel_key,
        e.event_type,
        e.channel,
        e.event_at,
        e.page_url,
        e.session_id

    from events as e
    left join dim_customers as dc on e.customer_id = dc.customer_id and dc.is_current
    left join dim_date as dd on cast(e.event_at as date) = dd.date_day
    left join dim_channels as dch on e.channel = dch.channel

)

select * from final
