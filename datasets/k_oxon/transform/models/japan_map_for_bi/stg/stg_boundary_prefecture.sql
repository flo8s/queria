select
    prefecture_code,
    prefecture_name,
    geom as geometry
from {{ ref('raw_boundary_prefecture') }}
