{{
    config(
        materialized='table',
        contract={'enforced': true}
    )
}}

SELECT 'tsukuba' AS datasource, *
FROM read_json(
    '{{ var("storage_base_url") }}/tsukuba/build/metadata.json',
    columns={
        title: 'VARCHAR',
        description: 'VARCHAR',
        tags: 'JSON',
        ducklake_url: 'VARCHAR',
        schemas: 'JSON',
        dependencies: 'JSON',
        lineage: 'JSON'
    }
)
