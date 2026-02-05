{{
    config(
        materialized='table'
    )
}}

select *
from read_csv_auto(
    'https://www.city.tsukuba.lg.jp/material/files/group/185/082201_population_20250401_new.csv',
    header=true,
    encoding='shift_jis'
)
