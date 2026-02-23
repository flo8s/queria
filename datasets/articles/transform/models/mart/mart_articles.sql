{{ config(materialized='view') }}

select
    slug,
    title,
    summary,
    date,
    datasources,
    tags,
    concat_ws(' ', title, summary, array_to_string(tags, ' ')) as search_text
from {{ ref('raw_articles') }}
