select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    _source_lsn,
    _source_ts,
    _is_deleted,
    _silver_loaded_at,
    valid_from,
    valid_to,
    is_current
from {{ source('silver', 'sellers') }}
