with

orders as (
    select * from {{ ref('fct_orders') }}
),

seller_orders as (
    select distinct
        order_id,
        seller_id
    from {{ ref('fct_order_items') }}
    where not is_deleted
),

deliveries as (
    select
        seller_orders.seller_id,
        cast(orders.order_delivered_customer_ts_utc as date) as delivery_date,
        count(*) as n_orders,
        sum(case when orders.is_late then 1 else 0 end) as n_late,
        sum(orders.purchase_to_customer_hours) / 24.0 as delivery_days
    from seller_orders
    inner join orders on seller_orders.order_id = orders.order_id
    where orders.is_delivered and orders.order_delivered_customer_ts_utc is not null
    group by seller_orders.seller_id, cast(orders.order_delivered_customer_ts_utc as date)
),

dataset_end as (
    select max(cast(order_purchase_ts_utc as date)) as end_date
    from orders
),

first_deliveries as (
    select
        seller_id,
        min(delivery_date) as first_date
    from deliveries
    group by seller_id
),

spine as (
    select
        first_deliveries.seller_id,
        dates.date_day
    from first_deliveries
    cross join dataset_end
    inner join {{ ref('dim_date') }} as dates
        on
            first_deliveries.first_date <= dates.date_day
            and dataset_end.end_date >= dates.date_day
),

windowed as (
    select
        spine.seller_id,
        spine.date_day,
        cast(coalesce(sum(deliveries.n_orders), 0) as integer) as seller_orders_90d,
        sum(deliveries.n_late) as n_late,
        sum(deliveries.delivery_days) as delivery_days
    from spine
    left join deliveries
        on
            spine.seller_id = deliveries.seller_id
            and spine.date_day > deliveries.delivery_date
            and {{ dbt.dateadd('day', -90, 'spine.date_day') }} <= deliveries.delivery_date
    group by spine.seller_id, spine.date_day
)

select
    seller_id,
    cast(date_day as timestamp with time zone) as feature_ts,
    seller_orders_90d,
    n_late * 1.0 / nullif(seller_orders_90d, 0) as seller_late_rate_90d,
    delivery_days / nullif(seller_orders_90d, 0) as seller_avg_delivery_days_90d,
    {{ dbt.current_timestamp() }} as created_ts
from windowed
