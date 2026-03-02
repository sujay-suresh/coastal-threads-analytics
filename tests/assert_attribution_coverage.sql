-- Assert that attribution coverage is above 75%
-- (more than 75% of orders have a non-null attributed channel)
with coverage as (

    select
        count(*) as total_orders,
        count(attributed_channel) as attributed_orders,
        round(
            count(attributed_channel)::numeric / count(*)::numeric * 100,
            2
        ) as coverage_pct

    from {{ ref('int_orders_attributed') }}

)

select *
from coverage
where coverage_pct < 75
