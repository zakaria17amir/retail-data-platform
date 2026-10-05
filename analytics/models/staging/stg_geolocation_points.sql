select
    geolocation_pk,
    zip_code_prefix,
    lat,
    lng,
    city,
    state,
    _source_lsn,
    _source_ts,
    _is_deleted,
    _silver_loaded_at
from {{ latest_export(source('silver', 'geolocation_points')) }}
