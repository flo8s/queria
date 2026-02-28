{{ config(materialized='view') }}

SELECT
    datasource,
    title,
    description,
    cover,
    ducklake_url,
    tags AS tags_json
FROM {{ ref('stg_catalog') }}
