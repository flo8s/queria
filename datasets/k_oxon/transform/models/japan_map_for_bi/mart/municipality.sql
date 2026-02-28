select
    lg_code,
    prefecture_code,
    prefecture_name,
    county_name,
    city_name,
    geometry
from {{ ref('stg_boundary_municipality') }}
