with

customers as (
    select
        customer_id,
        customer_unique_id
    from {{ ref('stg_customers') }}
    where is_current
)

select
    orders.order_id,
    orders.customer_id,
    customers.customer_unique_id,
    orders.order_purchase_ts_utc,
    row_number() over (
        partition by customers.customer_unique_id
        order by orders.order_purchase_ts_utc, orders.order_id
    ) as customer_order_seq,
    row_number() over (
        partition by customers.customer_unique_id
        order by orders.order_purchase_ts_utc, orders.order_id
    ) = 1 as is_first_order
from {{ ref('stg_orders') }} as orders
inner join customers on orders.customer_id = customers.customer_id
