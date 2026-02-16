{{ config(materialized='table') }}

SELECT
    prefecture_code,
    prefecture_name,
    ST_Union(geom) AS geom
FROM {{ ref('mart_municipality') }}
GROUP BY
    prefecture_code,
    prefecture_name
