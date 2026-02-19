SELECT
    datasource,
    node_id,
    table_name,
    column_name,
    column_index,
    description,
    data_type
FROM {{ ref('stg_columns') }}
