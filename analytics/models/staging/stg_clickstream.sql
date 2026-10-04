select
    event_id,
    event_type,
    session_id,
    customer_id,
    device,
    referrer,
    event_ts_local,
    event_ts_utc,
    product_id,
    search_query,
    quantity,
    order_id,
    utm_campaign,
    event_date,
    _bronze_ingest_ts,
    _silver_loaded_at
from {{ source('silver', 'clickstream') }}
