{{
    config(
        materialized='table'
    )
}}

{{ read_population_csv(
    'https://www.city.tsukuba.lg.jp/material/files/group/185/082201_population_20250501_new.csv'
) }}
