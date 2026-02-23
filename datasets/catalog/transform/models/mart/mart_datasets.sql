SELECT
    datasource,
    title,
    description,
    cover,
    ducklake_url,
    tags_json
FROM {{ ref('stg_datasets') }}
