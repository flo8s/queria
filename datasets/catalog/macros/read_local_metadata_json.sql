{% macro read_local_metadata_json(datasource) %}

SELECT '{{ datasource }}' AS datasource, *
FROM read_json(
    '{{ dist_dir() }}/metadata.json',
    columns={
        title: 'VARCHAR',
        description: 'VARCHAR',
        cover: 'VARCHAR',
        tags: 'JSON',
        ducklake_url: 'VARCHAR',
        schemas: 'JSON',
        dependencies: 'JSON',
        lineage: 'JSON'
    }
)

{% endmacro %}
