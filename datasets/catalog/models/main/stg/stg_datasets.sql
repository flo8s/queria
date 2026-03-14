{{ config(materialized='view') }}

SELECT
    datasource,
    title,
    description,
    cover,
    ducklake_url,
    tags AS tags_json,
    readme
FROM {{ ref('stg_catalog') }}
