select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    _is_deleted as is_deleted
from {{ ref('stg_order_payments') }}
