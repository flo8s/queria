{% macro process_municipality_attrs(source_ref) %}

WITH boundary AS (
    SELECT
        N03_007 AS lg_code_5,
        geom
    FROM {{ source_ref }}
    WHERE N03_004 IS NOT NULL
      AND N03_004 != '所属未定地'
),

joined AS (
    SELECT
        m.municipality_code,
        m.lg_code_5,
        m.prefecture_code,
        m.pref AS prefecture_name,
        m.county AS county_name,
        m.city AS city_name,
        m.is_hoppo_city,
        ST_MakeValid(b.geom) AS geom
    FROM boundary b
    INNER JOIN {{ ref('stg_mst_city') }} m
        ON b.lg_code_5 = m.lg_code_5
    WHERE m.is_local_gov = true
)

SELECT
    municipality_code AS lg_code,
    municipality_code AS lg_code_5,
    prefecture_code,
    prefecture_name,
    county_name,
    city_name,
    is_hoppo_city,
    ST_Union(geom) AS geom
FROM joined
GROUP BY
    municipality_code,
    prefecture_code,
    prefecture_name,
    county_name,
    city_name,
    is_hoppo_city

{% endmacro %}
