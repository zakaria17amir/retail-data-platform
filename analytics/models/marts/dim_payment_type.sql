select distinct payment_type
from {{ ref('stg_order_payments') }}
