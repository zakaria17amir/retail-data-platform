select
    session_id,
    min(event_date) as session_date,
    min(event_ts_utc) as session_start_ts_utc,
    max(event_ts_utc) as session_end_ts_utc,
    count(*) as event_count,
    sum(case when event_type = 'page_view' then 1 else 0 end) as page_view_count,
    sum(case when event_type = 'search' then 1 else 0 end) as search_count,
    sum(case when event_type = 'product_view' then 1 else 0 end) as product_view_count,
    sum(case when event_type = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
    sum(case when event_type = 'checkout_started' then 1 else 0 end) as checkout_started_count,
    sum(case when event_type = 'checkout_started' then 1 else 0 end) > 0 as has_checkout,
    max(customer_id) as customer_id,
    max(order_id) as order_id
from {{ ref('stg_clickstream') }}
group by session_id
