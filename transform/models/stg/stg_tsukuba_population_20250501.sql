{{ config(materialized='view') }}

{{ transform_population(ref('raw_tsukuba_population_20250501')) }}
