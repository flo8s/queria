{{
    config(
        materialized='table',
        contract={'enforced': true}
    )
}}

SELECT 'e_stat' AS datasource, *
FROM read_json(
    '{{ var("storage_base_url") }}/e_stat/build/metadata.json',
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
