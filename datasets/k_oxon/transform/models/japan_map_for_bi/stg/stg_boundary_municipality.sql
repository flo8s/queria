select
    lg_code,
    prefecture_code,
    prefecture_name,
    county_name,
    city_name,
    geom as geometry
from {{ ref('raw_boundary_municipality') }}
