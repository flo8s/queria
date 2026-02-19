SELECT
    datasource,
    name,
    resource_type,
    materialized,
    fqn_json,
    meta_json
FROM {{ ref('stg_lineage_nodes') }}
