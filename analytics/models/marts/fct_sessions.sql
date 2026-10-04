select
    session_id,
    session_date,
    session_start_ts_utc,
    session_end_ts_utc,
    event_count,
    page_view_count,
    search_count,
    product_view_count,
    add_to_cart_count,
    checkout_started_count,
    has_checkout,
    customer_id,
    order_id
from {{ ref('int_sessions') }}
