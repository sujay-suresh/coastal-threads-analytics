-- dim_date: date dimension with fiscal periods, day_of_week, holidays
-- Grain: one row per calendar date
with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-07-01' as date)",
        end_date="cast('2026-06-30' as date)"
    ) }}

),

dates as (

    select
        cast(date_day as date) as date_day

    from date_spine

),

enriched as (

    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
        date_day,
        extract(year from date_day)::int as calendar_year,
        extract(quarter from date_day)::int as calendar_quarter,
        extract(month from date_day)::int as calendar_month,
        extract(week from date_day)::int as week_of_year,
        extract(dow from date_day)::int as day_of_week_num,
        to_char(date_day, 'Day') as day_of_week_name,
        to_char(date_day, 'Month') as month_name,
        extract(day from date_day)::int as day_of_month,

        -- fiscal year (July start)
        case
            when extract(month from date_day) >= 7
            then extract(year from date_day)::int + 1
            else extract(year from date_day)::int
        end as fiscal_year,
        case
            when extract(month from date_day) >= 7
            then extract(quarter from date_day)::int - 2
            else extract(quarter from date_day)::int + 2
        end as fiscal_quarter,

        -- flags
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end as is_weekend,

        case
            -- US federal holidays (approximate)
            when extract(month from date_day) = 1 and extract(day from date_day) = 1 then true    -- New Year's
            when extract(month from date_day) = 7 and extract(day from date_day) = 4 then true    -- July 4th
            when extract(month from date_day) = 12 and extract(day from date_day) = 25 then true  -- Christmas
            when extract(month from date_day) = 11 and extract(dow from date_day) = 4
                 and extract(day from date_day) between 22 and 28 then true                       -- Thanksgiving
            when extract(month from date_day) = 2 and extract(day from date_day) = 14 then true   -- Valentine's
            else false
        end as is_holiday

    from dates

)

select * from enriched
