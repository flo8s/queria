{{ config(materialized='table') }}

SELECT *
FROM read_parquet(
    '{{ env_var("BOUNDARY_PARQUET_PATH", "../ingestion/output/boundary_*.parquet") }}'
)
