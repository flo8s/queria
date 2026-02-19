{{ config(materialized='view') }}

WITH deps_raw AS (
    SELECT
        datasource,
        dependencies AS deps_json,
        json_array_length(dependencies)::BIGINT AS deps_len
    FROM {{ ref('stg_catalog') }}
    WHERE json_type(dependencies) = 'ARRAY'
      AND json_array_length(dependencies) > 0
)

SELECT
    d.datasource,
    json_extract_string(d.deps_json, '$[' || i.idx || '].alias') AS alias,
    json_extract_string(d.deps_json, '$[' || i.idx || '].ducklake_url') AS ducklake_url
FROM deps_raw d,
LATERAL (
    SELECT UNNEST(generate_series(0::BIGINT, d.deps_len - 1)) AS idx
) i
