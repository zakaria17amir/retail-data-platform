{%- set window_days = var('ml_demand_window_days', 180) -%}
{%- set test_days = var('ml_demand_test_days', 28) -%}

with

daily as (
    select
        items.order_purchase_date as date_day,
        products.product_category_name_english as product_category,
        customers.customer_state,
        count(distinct items.order_id) as orders
    from {{ ref('fct_order_items') }} as items
    inner join {{ ref('dim_product') }} as products on items.product_sk = products.product_sk
    inner join {{ ref('fct_orders') }} as orders on items.order_id = orders.order_id
    inner join {{ ref('dim_customer') }} as customers on orders.customer_sk = customers.customer_sk
    where items.is_revenue_order
    group by
        items.order_purchase_date,
        products.product_category_name_english,
        customers.customer_state
),

bounds as (
    select
        max(date_day) as end_date,
        {{ dbt.dateadd('day', 1 - test_days, 'max(date_day)') }} as cutoff_date
    from daily
),

series as (
    select
        daily.product_category,
        daily.customer_state,
        min(daily.date_day) as first_date,
        2 * sum(
            case
                when
                    daily.date_day < bounds.cutoff_date
                    and daily.date_day
                    >= {{ dbt.dateadd('day', -window_days, 'bounds.cutoff_date') }}
                    then 1
                else 0
            end
        ) >= {{ window_days }} as is_modelled
    from daily
    cross join bounds
    group by daily.product_category, daily.customer_state
)

-- column order follows the gold -> ML contract
select  -- noqa: ST06
    dates.date_day as date,  -- noqa: RF04
    series.product_category,
    series.customer_state,
    coalesce(daily.orders, 0) as orders,
    series.is_modelled
from series
cross join bounds
inner join {{ ref('dim_date') }} as dates
    on series.first_date <= dates.date_day and bounds.end_date >= dates.date_day
left join daily
    on
        dates.date_day = daily.date_day
        and series.product_category = daily.product_category
        and series.customer_state = daily.customer_state
