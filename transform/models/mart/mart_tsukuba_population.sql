{{ config(materialized='view') }}

SELECT * FROM {{ ref('stg_tsukuba_population_20240401') }}
UNION ALL SELECT * FROM {{ ref('stg_tsukuba_population_20240501') }}
UNION ALL SELECT * FROM {{ ref('stg_tsukuba_population_20241001') }}
UNION ALL SELECT * FROM {{ ref('stg_tsukuba_population_20250401') }}
UNION ALL SELECT * FROM {{ ref('stg_tsukuba_population_20250501') }}
UNION ALL SELECT * FROM {{ ref('stg_tsukuba_population_20251001') }}
