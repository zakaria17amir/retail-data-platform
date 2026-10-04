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
)

select
    customers.customer_unique_id,
    customers.orders,
    customers.first_order_ts_utc,
    customers.last_order_ts_utc,
    customers.frequency,
    customers.monetary,
    customers.monetary as ltv,
    as_of.as_of_date - customers.last_order_date as recency_days,
    ntile(5) over (
        order by customers.last_order_date, customers.customer_unique_id
    ) as recency_score,
    ntile(5) over (
        order by customers.frequency, customers.customer_unique_id
    ) as frequency_score,
    ntile(5) over (
        order by customers.monetary, customers.customer_unique_id
    ) as monetary_score
from customers
cross join as_of
