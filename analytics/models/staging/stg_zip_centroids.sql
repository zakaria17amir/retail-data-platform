select
    zip_code_prefix,
    lat,
    lng,
    n_points,
    state
from {{ source('silver', 'zip_centroids') }}
