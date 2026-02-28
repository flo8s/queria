{{ config(materialized='table') }}

select *
from read_json(
    '{{ var("articles_metadata_url", "https://queria.io/api/articles-metadata") }}',
    format='array',
    columns={
        slug: 'VARCHAR',
        title: 'VARCHAR',
        summary: 'VARCHAR',
        date: 'DATE',
        datasources: 'VARCHAR[]',
        tags: 'VARCHAR[]'
    }
)
