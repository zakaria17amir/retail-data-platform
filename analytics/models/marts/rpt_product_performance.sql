with

items as (
    select
        order_id,
        product_id,
        item_revenue,
        cast({{ dbt.date_trunc('month', 'order_purchase_date') }} as date) as order_month
    from {{ ref('fct_order_items') }}
    where is_revenue_order
),

reviews as (
    select
        order_id,
        avg(review_score) as review_score
    from {{ ref('fct_reviews') }}
    where not is_deleted
    group by order_id
),

product_orders as (
    select distinct
        items.product_id,
        items.order_month,
        items.order_id,
        reviews.review_score,
        fct_orders.is_delivered,
        fct_orders.is_late
    from items
    inner join {{ ref('fct_orders') }} as fct_orders on items.order_id = fct_orders.order_id
    left join reviews on items.order_id = reviews.order_id
),

order_measures as (
    select
        product_id,
        order_month,
        avg(review_score) as avg_review_score,
        sum(case when is_delivered then 1 else 0 end) as delivered_orders,
        sum(case when is_late then 1 else 0 end) as late_orders
    from product_orders
    group by product_id, order_month
),

item_measures as (
    select
        product_id,
        order_month,
        count(*) as units,
        sum(item_revenue) as revenue
    from items
    group by product_id, order_month
)

select
    item_measures.product_id,
    item_measures.order_month,
    item_measures.units,
    item_measures.revenue,
    order_measures.avg_review_score,
    order_measures.delivered_orders,
    order_measures.late_orders,
    order_measures.late_orders / nullif(order_measures.delivered_orders, 0) as late_rate
from item_measures
inner join order_measures
    on
        item_measures.product_id = order_measures.product_id
        and item_measures.order_month = order_measures.order_month
