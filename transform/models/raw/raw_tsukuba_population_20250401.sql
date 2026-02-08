{{
    config(
        materialized='table'
    )
}}

select *
from {{ read_tsukuba_population_csv('https://www.city.tsukuba.lg.jp/material/files/group/185/082201_population_20250401_new.csv') }}
