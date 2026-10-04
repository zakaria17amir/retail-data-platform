with

orders as (
    select * from {{ ref('stg_orders') }}
),

durations as (
    select
        order_id,
        {{ dbt.datediff('order_purchase_ts_utc', 'order_approved_ts_utc', 'second') }}
        / 3600.0 as purchase_to_approved_hours,
        {{ dbt.datediff('order_approved_ts_utc', 'order_delivered_carrier_ts_utc', 'second') }}
        / 3600.0 as approved_to_carrier_hours,
        {{ dbt.datediff(
            'order_delivered_carrier_ts_utc', 'order_delivered_customer_ts_utc', 'second'
        ) }}
        / 3600.0 as carrier_to_customer_hours,
        {{ dbt.datediff('order_purchase_ts_utc', 'order_delivered_customer_ts_utc', 'second') }}
        / 3600.0 as purchase_to_customer_hours
    from orders
),

items as (
    select
        order_id,
        count(*) as item_count,
        sum(price + freight_value) as item_revenue
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value) as payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id
)

select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders._is_deleted as is_deleted,
    orders.order_purchase_ts_local,
    orders.order_purchase_ts_utc,
    orders.order_approved_ts_utc,
    orders.order_delivered_carrier_ts_utc,
    orders.order_delivered_customer_ts_utc,
    orders.order_estimated_delivery_ts_utc,
    durations.purchase_to_approved_hours,
    durations.approved_to_carrier_hours,
    durations.carrier_to_customer_hours,
    durations.purchase_to_customer_hours,
    coalesce(orders.order_status = 'delivered' and not orders._is_deleted, false) as is_delivered,
    coalesce(
        orders.order_status = 'delivered'
        and not orders._is_deleted
        and orders.order_delivered_customer_ts_utc > orders.order_estimated_delivery_ts_utc,
        false
    ) as is_late,
    coalesce(
        orders.order_status not in ('canceled', 'unavailable')
        and not orders._is_deleted
        and items.item_revenue > 0,
        false
    ) as is_revenue_order,
    coalesce(items.item_count, 0) as item_count,
    coalesce(items.item_revenue, 0) as item_revenue,
    coalesce(payments.payment_value, 0) as payment_value
from orders
inner join durations on orders.order_id = durations.order_id
left join items on orders.order_id = items.order_id
left join payments on orders.order_id = payments.order_id
