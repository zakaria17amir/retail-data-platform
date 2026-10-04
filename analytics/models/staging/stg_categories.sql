select
    product_category_name,
    product_category_name_english,
    _source_lsn,
    _source_ts,
    _is_deleted,
    _silver_loaded_at
from {{ source('silver', 'categories') }}
