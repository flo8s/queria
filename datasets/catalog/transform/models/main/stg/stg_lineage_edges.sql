{{ config(materialized='view') }}

WITH parent_map_expanded AS (
    SELECT
        r.datasource,
        json_keys(r.lineage->'$.parent_map') AS keys
    FROM {{ ref('stg_catalog') }} r
    WHERE r.lineage IS NOT NULL
),

keys_unnested AS (
    SELECT
        datasource,
        UNNEST(keys) AS child
    FROM parent_map_expanded
),

catalog_ref AS (
    SELECT datasource, lineage
    FROM {{ ref('stg_catalog') }}
    WHERE lineage IS NOT NULL
)

SELECT
    k.datasource,
    k.child,
    p.parent::VARCHAR AS parent
FROM keys_unnested k
JOIN catalog_ref r ON k.datasource = r.datasource,
LATERAL (
    SELECT UNNEST(
        from_json(r.lineage->'$.parent_map'->k.child, '["VARCHAR"]')
    ) AS parent
) p
