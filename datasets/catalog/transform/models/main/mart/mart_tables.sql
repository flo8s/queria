SELECT
    datasource,
    node_id,
    node_index,
    name,
    schema_name,
    description,
    materialized,
    title,
    license,
    license_url,
    source_url,
    is_published,
    tags_json,
    sql
FROM {{ ref('stg_models') }}
