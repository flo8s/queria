{{
    config(
        materialized='table'
    )
}}

{{ read_population_csv(
    'https://www.city.tsukuba.lg.jp/material/files/group/16/082201_population_20240501_new.csv'
) }}
