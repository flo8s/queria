select
    prefecture_code,
    prefecture_name,
    geometry
from {{ ref('stg_boundary_prefecture') }}
