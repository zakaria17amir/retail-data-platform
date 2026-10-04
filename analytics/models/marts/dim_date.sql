with

days as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2020-01-01' as date)"
    ) }}
)

select
    cast(date_day as date) as date_day,
    extract(year from date_day) as calendar_year,
    extract(quarter from date_day) as calendar_quarter,
    extract(month from date_day) as calendar_month,
    extract(day from date_day) as day_of_month,
    {{ iso_day_of_week('date_day') }} as iso_day_of_week,
    {{ iso_day_of_week('date_day') }} >= 6 as is_weekend,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start_date
from days
