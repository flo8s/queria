SELECT
    datasource,
    schema_name,
    title
FROM {{ ref('stg_dataset_schemas') }}
