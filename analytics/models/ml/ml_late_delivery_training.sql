with

orders as (
    select * from {{ ref('fct_orders') }}
    where order_approved_ts_utc is not null and not is_deleted
),

items as (
    select
        *,
        row_number() over (
            partition by order_id order by price desc, seller_id asc, order_item_id asc
        ) as price_rank
    from {{ ref('fct_order_items') }}
    where not is_deleted
),

order_totals as (
    select
        order_id,
        count(*) as n_items,
        count(distinct seller_id) as n_sellers,
        sum(price) as total_price,
        sum(freight_value) as total_freight
    from items
    group by order_id
),

primary_items as (
    select * from items
    where price_rank = 1
),

payments as (
    select * from {{ ref('fct_payments') }}
    where payment_sequential = 1 and not is_deleted
),

geo as (
    select * from {{ ref('dim_geo') }}
)

-- column order follows the gold -> ML contract
select  -- noqa: ST06
    orders.order_id,
    orders.customer_id,
    orders.customer_unique_id,
    primary_items.seller_id,
    orders.order_status,
    orders.order_purchase_ts_utc,
    orders.order_approved_ts_utc,
    orders.order_estimated_delivery_ts_utc,
    orders.order_delivered_customer_ts_utc,
    case
        when orders.is_delivered and orders.order_delivered_customer_ts_utc is not null
            then orders.is_late
    end as is_late,
    coalesce(order_totals.n_items, 0) as n_items,
    coalesce(order_totals.n_sellers, 0) as n_sellers,
    order_totals.total_price,
    order_totals.total_freight,
    products.product_category_name_english as product_category,
    payments.payment_type,
    payments.payment_installments,
    customers.customer_state,
    customer_geo.lat as customer_lat,
    customer_geo.lng as customer_lng,
    sellers.seller_state,
    seller_geo.lat as seller_lat,
    seller_geo.lng as seller_lng
from orders
left join order_totals on orders.order_id = order_totals.order_id
left join primary_items on orders.order_id = primary_items.order_id
left join {{ ref('dim_product') }} as products on primary_items.product_sk = products.product_sk
left join {{ ref('dim_seller') }} as sellers on primary_items.seller_sk = sellers.seller_sk
left join {{ ref('dim_customer') }} as customers on orders.customer_sk = customers.customer_sk
left join payments on orders.order_id = payments.order_id
left join geo as customer_geo on customers.customer_zip_code_prefix = customer_geo.zip_code_prefix
left join geo as seller_geo on sellers.seller_zip_code_prefix = seller_geo.zip_code_prefix
