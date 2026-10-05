select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    _source_lsn,
    _source_ts,
    _is_deleted,
    shipping_limit_ts_local,
    shipping_limit_ts_utc,
    _silver_loaded_at
from {{ latest_export(source('silver', 'order_items')) }}
