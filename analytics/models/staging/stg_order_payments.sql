select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    _source_lsn,
    _source_ts,
    _is_deleted,
    _silver_loaded_at
from {{ latest_export(source('silver', 'order_payments')) }}
