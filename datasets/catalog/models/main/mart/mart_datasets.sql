SELECT
    datasource,
    title,
    description,
    cover,
    ducklake_url,
    tags_json,
    readme
FROM {{ ref('stg_datasets') }}
