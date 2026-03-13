SELECT
    datasource,
    child,
    parent
FROM {{ ref('stg_lineage_edges') }}
