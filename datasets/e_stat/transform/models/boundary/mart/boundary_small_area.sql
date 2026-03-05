SELECT
    key_code,
    pref_code,
    pref_name,
    city_code,
    city_name,
    area_name,
    area_sqm,
    perimeter_m,
    population,
    households,
    geometry
FROM {{ ref('stg_boundary_small_area') }}
