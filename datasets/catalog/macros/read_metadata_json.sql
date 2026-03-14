{% macro read_metadata_json(datasource) %}

SELECT '{{ datasource }}' AS datasource, *
FROM read_json(
    '{{ var("storage_base_url") }}/{{ datasource }}/metadata.json',
    columns={
        title: 'VARCHAR',
        description: 'VARCHAR',
        cover: 'VARCHAR',
        tags: 'JSON',
        ducklake_url: 'VARCHAR',
        schemas: 'JSON',
        dependencies: 'JSON',
        lineage: 'JSON',
        readme: 'VARCHAR'
    }
)

{% endmacro %}
