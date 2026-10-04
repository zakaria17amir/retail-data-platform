select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'valid_from']) }} as product_sk,
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    valid_from,
    valid_to,
    is_current,
    _is_deleted as is_deleted,
    row_number() over (partition by product_id order by valid_from) = 1 as is_first_version
from {{ ref('stg_products') }}
