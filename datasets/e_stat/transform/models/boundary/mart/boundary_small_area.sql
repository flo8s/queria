SELECT
    KEY_CODE AS key_code,
    PREF AS pref_code,
    PREF_NAME AS pref_name,
    CITY AS city_code,
    CITY_NAME AS city_name,
    S_NAME AS area_name,
    AREA AS area_sqm,
    PERIMETER AS perimeter_m,
    JINKO AS population,
    SETAI AS households,
    ST_Simplify(geom, 0.0001) AS geometry
FROM {{ ref('raw_boundary_small_area') }}
