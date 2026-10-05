select
    zip_code_prefix,
    lat,
    lng,
    n_points,
    state
from {{ latest_export(source('silver', 'zip_centroids')) }}
