{{
    config(
        materialized='table',
        contract={'enforced': true}
    )
}}

SELECT 'tsukuba' AS datasource, *
FROM read_json(
    'https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev/tsukuba/build/metadata.json',
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
