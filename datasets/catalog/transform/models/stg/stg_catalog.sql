{{ config(materialized='table') }}

SELECT * FROM {{ ref('raw_tsukuba') }}
UNION ALL
SELECT * FROM {{ ref('raw_e_stat') }}
