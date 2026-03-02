-- dim_channels: marketing channel taxonomy
-- Grain: one row per channel
with channels as (

    select distinct channel
    from {{ ref('stg_klaviyo__events') }}

),

taxonomy as (

    select
        {{ dbt_utils.generate_surrogate_key(['channel']) }} as channel_key,
        channel,
        case
            when channel in ('paid_social', 'paid_search') then 'Paid'
            when channel in ('organic_social', 'organic_search') then 'Organic'
            when channel = 'email' then 'Owned'
            when channel = 'direct' then 'Direct'
            when channel in ('referral', 'affiliate') then 'Earned'
            else 'Other'
        end as channel_category,
        case
            when channel in ('paid_social', 'paid_search') then true
            else false
        end as is_paid

    from channels

)

select * from taxonomy
