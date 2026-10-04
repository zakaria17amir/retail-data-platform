with

items as (
    select
        order_id,
        sum(item_revenue) as revenue
    from {{ ref('fct_order_items') }}
    where is_revenue_order
    group by order_id
),

orders as (
    select
        fct_orders.order_id,
        fct_orders.order_purchase_date,
        dim_customer.customer_state
    from {{ ref('fct_orders') }} as fct_orders
    inner join {{ ref('dim_customer') }} as dim_customer
        on fct_orders.customer_sk = dim_customer.customer_sk
)

select
    orders.order_purchase_date,
    orders.customer_state,
    count(*) as orders,
    sum(items.revenue) as revenue,
    sum(items.revenue) / count(*) as aov
from items
inner join orders on items.order_id = orders.order_id
group by orders.order_purchase_date, orders.customer_state
