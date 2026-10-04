with

lifecycle as (
    select * from {{ ref('int_order_lifecycle') }}
),

customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

customers as (
    select * from {{ ref('dim_customer') }}
)

select
    lifecycle.order_id,
    customers.customer_sk,
    lifecycle.customer_id,
    customer_orders.customer_unique_id,
    customer_orders.customer_order_seq,
    customer_orders.is_first_order,
    lifecycle.order_status,
    cast(lifecycle.order_purchase_ts_local as date) as order_purchase_date,
    lifecycle.order_purchase_ts_utc,
    lifecycle.order_approved_ts_utc,
    lifecycle.order_delivered_carrier_ts_utc,
    lifecycle.order_delivered_customer_ts_utc,
    lifecycle.order_estimated_delivery_ts_utc,
    lifecycle.purchase_to_approved_hours,
    lifecycle.approved_to_carrier_hours,
    lifecycle.carrier_to_customer_hours,
    lifecycle.purchase_to_customer_hours,
    lifecycle.is_delivered,
    lifecycle.is_late,
    lifecycle.is_revenue_order,
    lifecycle.item_count,
    lifecycle.item_revenue,
    lifecycle.payment_value
from lifecycle
left join customer_orders on lifecycle.order_id = customer_orders.order_id
left join customers
    on
        lifecycle.customer_id = customers.customer_id
        and {{ point_in_time('customers', 'lifecycle.order_purchase_ts_utc') }}
