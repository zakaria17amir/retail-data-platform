select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'valid_from']) }} as customer_sk,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    valid_from,
    valid_to,
    is_current,
    _is_deleted as is_deleted,
    row_number() over (partition by customer_id order by valid_from) = 1 as is_first_version
from {{ ref('stg_customers') }}
