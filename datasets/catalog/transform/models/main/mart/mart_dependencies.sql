SELECT
    datasource,
    alias,
    ducklake_url
FROM {{ ref('stg_dataset_dependencies') }}
