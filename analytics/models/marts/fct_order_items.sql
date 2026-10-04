with

items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('int_order_lifecycle') }}
),

products as (
    select * from {{ ref('dim_product') }}
),

sellers as (
    select * from {{ ref('dim_seller') }}
)

select
    items.order_id,
    items.order_item_id,
    products.product_sk,
    sellers.seller_sk,
    items.product_id,
    items.seller_id,
    orders.order_purchase_ts_utc,
    items.shipping_limit_ts_utc,
    items.price,
    items.freight_value,
    orders.is_revenue_order,
    cast(orders.order_purchase_ts_local as date) as order_purchase_date,
    items.price + items.freight_value as item_revenue
from items
left join orders on items.order_id = orders.order_id
left join products
    on
        items.product_id = products.product_id
        and {{ point_in_time('products', 'orders.order_purchase_ts_utc') }}
left join sellers
    on
        items.seller_id = sellers.seller_id
        and {{ point_in_time('sellers', 'orders.order_purchase_ts_utc') }}
