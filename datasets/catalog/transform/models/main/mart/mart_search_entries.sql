WITH tags_agg AS (
    SELECT
        t.datasource,
        t.name,
        STRING_AGG(tag.value::VARCHAR, ' ') AS tags_text
    FROM {{ ref('mart_tables') }} t,
        LATERAL UNNEST(
            CASE WHEN t.tags_json IS NOT NULL
                 THEN CAST(t.tags_json AS VARCHAR[])
                 ELSE ARRAY[]::VARCHAR[]
            END
        ) AS tag(value)
    GROUP BY t.datasource, t.name
),

tables AS (
    SELECT
        'table' AS entry_type,
        t.datasource,
        t.schema_name,
        t.name AS table_name,
        t.title AS table_title,
        NULL::VARCHAR AS column_name,
        t.description,
        COALESCE(REPLACE(t.name, '_', ' '), '') || ' ' ||
            COALESCE(t.title, '') || ' ' ||
            COALESCE(t.description, '') || ' ' ||
            COALESCE(ta.tags_text, '') AS search_text,
        '/datasets/' || t.datasource || '/' || t.schema_name || '/' || t.name AS href
    FROM {{ ref('mart_tables') }} t
    LEFT JOIN tags_agg ta ON t.datasource = ta.datasource AND t.name = ta.name
),

columns AS (
    SELECT
        'column' AS entry_type,
        c.datasource,
        t.schema_name,
        c.table_name,
        t.title AS table_title,
        c.column_name,
        c.description,
        COALESCE(REPLACE(c.column_name, '_', ' '), '') || ' ' ||
            COALESCE(c.title, '') || ' ' ||
            COALESCE(c.description, '') || ' ' ||
            COALESCE(REPLACE(c.table_name, '_', ' '), '') AS search_text,
        '/datasets/' || c.datasource || '/' || t.schema_name || '/' || c.table_name AS href
    FROM {{ ref('mart_columns') }} c
    LEFT JOIN {{ ref('mart_tables') }} t
        ON c.datasource = t.datasource AND c.table_name = t.name
)

SELECT * FROM tables
UNION ALL
SELECT * FROM columns
