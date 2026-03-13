{{ config(materialized='view') }}

WITH schemas_expanded AS (
    SELECT
        r.datasource,
        s.schema_name,
        json_extract(r.schemas, '$.' || s.schema_name || '.tables') AS tables_json
    FROM {{ ref('stg_catalog') }} r,
    LATERAL (
        SELECT UNNEST(json_keys(r.schemas)) AS schema_name
    ) s
    WHERE json_type(json_extract(r.schemas, '$.' || s.schema_name || '.tables')) = 'ARRAY'
),
tables_expanded AS (
    SELECT
        se.datasource,
        se.schema_name,
        t.table_index,
        json_extract(se.tables_json, '$[' || t.table_index || ']') AS tbl
    FROM schemas_expanded se,
    LATERAL (
        SELECT UNNEST(generate_series(0::BIGINT, json_array_length(se.tables_json)::BIGINT - 1)) AS table_index
    ) t
)

SELECT
    datasource,
    'model.' || datasource || '.' || (tbl->>'$.name') AS node_id,
    (table_index + 1)::INTEGER AS node_index,
    tbl->>'$.name' AS name,
    schema_name,
    tbl->>'$.description' AS description,
    tbl->>'$.materialized' AS materialized,
    tbl->>'$.title' AS title,
    tbl->>'$.license' AS license,
    tbl->>'$.license_url' AS license_url,
    tbl->>'$.source_url' AS source_url,
    COALESCE(CAST(tbl->>'$.published' AS BOOLEAN), false) AS is_published,
    tbl->'$.tags' AS tags_json,
    tbl->>'$.sql' AS sql,
    tbl->'$.columns' AS columns_json
FROM tables_expanded
