with

orders as (
    select * from {{ ref('fct_orders') }}
),

as_of as (
    select max(order_purchase_date) as as_of_date from orders
),

customers as (
    select
        customer_unique_id,
        count(*) as orders,
        sum(case when is_revenue_order then 1 else 0 end) as frequency,
        sum(case when is_revenue_order then item_revenue else 0 end) as monetary,
        min(case when is_revenue_order then order_purchase_ts_utc end) as first_order_ts_utc,
        max(case when is_revenue_order then order_purchase_ts_utc end) as last_order_ts_utc,
        max(case when is_revenue_order then order_purchase_date end) as last_order_date
    from orders
    group by customer_unique_id
    having sum(case when is_revenue_order then 1 else 0 end) > 0
),

recency as (
    select
        customers.*,
        {{ dbt.datediff('last_order_date', 'as_of_date', 'day') }}
            as recency_days
    from customers
    cross join as_of
)

-- tie-safe scores: equal metric values always share a score (percent_rank buckets; frequency
-- by fixed thresholds because most customers have exactly one order)
select
    customer_unique_id,
    orders,
    first_order_ts_utc,
    last_order_ts_utc,
    recency_days,
    frequency,
    monetary,
    monetary as ltv,
    case
        when frequency >= 4 then 5
        when frequency = 3 then 4
        when frequency = 2 then 3
        else 1
    end as frequency_score,
    least(
        5, 1 + cast(floor(5 * percent_rank() over (order by recency_days desc)) as integer)
    ) as recency_score,
    least(
        5, 1 + cast(floor(5 * percent_rank() over (order by monetary)) as integer)
    ) as monetary_score
from recency
