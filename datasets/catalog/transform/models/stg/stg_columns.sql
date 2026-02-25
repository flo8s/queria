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
        json_extract(se.tables_json, '$[' || t.table_index || ']') AS tbl
    FROM schemas_expanded se,
    LATERAL (
        SELECT UNNEST(generate_series(0::BIGINT, json_array_length(se.tables_json)::BIGINT - 1)) AS table_index
    ) t
),
columns_expanded AS (
    SELECT
        te.datasource,
        'model.' || te.datasource || '.' || (te.tbl->>'$.name') AS node_id,
        te.tbl->>'$.name' AS table_name,
        (c.col_index + 1)::INTEGER AS column_index,
        json_extract(te.tbl, '$.columns[' || c.col_index || ']') AS col
    FROM tables_expanded te,
    LATERAL (
        SELECT UNNEST(generate_series(
            0::BIGINT,
            json_array_length(json_extract(te.tbl, '$.columns'))::BIGINT - 1
        )) AS col_index
    ) c
    WHERE json_type(json_extract(te.tbl, '$.columns')) = 'ARRAY'
      AND json_array_length(json_extract(te.tbl, '$.columns')) > 0
)

SELECT
    datasource,
    node_id,
    table_name,
    col->>'$.name' AS column_name,
    column_index,
    COALESCE(col->>'$.title', '') AS title,
    col->>'$.description' AS description,
    col->>'$.data_type' AS data_type,
    CAST(COALESCE(col->>'$.nullable', 'true') AS BOOLEAN) AS nullable
FROM columns_expanded
