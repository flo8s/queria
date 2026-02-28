{{ config(materialized='view') }}

WITH node_keys AS (
    SELECT
        r.datasource,
        UNNEST(json_keys(r.lineage->'$.nodes')) AS name
    FROM {{ ref('stg_catalog') }} r
    WHERE r.lineage IS NOT NULL
),

catalog_ref AS (
    SELECT datasource, lineage
    FROM {{ ref('stg_catalog') }}
    WHERE lineage IS NOT NULL
)

SELECT
    k.datasource,
    k.name,
    r.lineage->>'$.nodes' || '.' || k.name || '.resource_type' AS resource_type_path,
    (r.lineage->'$.nodes'->k.name)->>'$.resource_type' AS resource_type,
    (r.lineage->'$.nodes'->k.name)->>'$.config.materialized' AS materialized,
    (r.lineage->'$.nodes'->k.name)->'$.fqn' AS fqn_json,
    (r.lineage->'$.nodes'->k.name)->'$.meta' AS meta_json
FROM node_keys k
JOIN catalog_ref r ON k.datasource = r.datasource
