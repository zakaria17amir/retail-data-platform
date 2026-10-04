select
    cast({{ dbt.date_trunc('month', 'fct_orders.order_purchase_date') }} as date) as order_month,
    dim_customer.customer_state,
    count(*) as delivered_orders,
    sum(case when fct_orders.is_late then 1 else 0 end) as late_orders,
    sum(case when fct_orders.is_late then 1 else 0 end) / count(*) as late_rate,
    avg(fct_orders.purchase_to_customer_hours) as avg_delivery_hours
from {{ ref('fct_orders') }} as fct_orders
inner join {{ ref('dim_customer') }} as dim_customer
    on fct_orders.customer_sk = dim_customer.customer_sk
where fct_orders.is_delivered
group by 1, 2
