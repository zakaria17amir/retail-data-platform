select
    {{ dbt_utils.generate_surrogate_key(['seller_id', 'valid_from']) }} as seller_sk,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    valid_from,
    valid_to,
    is_current,
    _is_deleted as is_deleted,
    row_number() over (partition by seller_id order by valid_from) = 1 as is_first_version
from {{ ref('stg_sellers') }}
