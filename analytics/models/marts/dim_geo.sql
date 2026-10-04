select
    zip_code_prefix,
    lat,
    lng,
    n_points,
    state
from {{ ref('stg_zip_centroids') }}
