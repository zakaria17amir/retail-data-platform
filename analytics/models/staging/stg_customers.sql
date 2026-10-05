select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _source_lsn,
    _source_ts,
    _is_deleted,
    _silver_loaded_at,
    valid_from,
    valid_to,
    is_current
from {{ latest_export(source('silver', 'customers')) }}
