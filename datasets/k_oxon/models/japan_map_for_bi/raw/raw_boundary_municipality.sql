{{ config(materialized='table') }}

select *
from ST_Read(
    'https://raw.githubusercontent.com/K-Oxon/Japan-map-for-BI/main/data/administrative_area/municipality/ja_municipality_area.topojson'
)
