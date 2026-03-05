{{ config(materialized='table') }}

SELECT *
FROM ST_Read(
    '{{ env_var("BOUNDARY_PARQUET_PATH", "../ingestion/output/boundary_*.parquet") }}'
)
