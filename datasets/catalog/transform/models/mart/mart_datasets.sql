SELECT
    datasource,
    title,
    description,
    ducklake_url,
    tags_json
FROM {{ ref('stg_datasets') }}
