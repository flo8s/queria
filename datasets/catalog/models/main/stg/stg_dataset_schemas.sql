{{ config(materialized='view') }}

SELECT
    r.datasource,
    s.schema_name,
    json_extract_string(r.schemas, '$.' || s.schema_name || '.title') AS title
FROM {{ ref('stg_catalog') }} r,
LATERAL (
    SELECT UNNEST(json_keys(r.schemas)) AS schema_name
) s
