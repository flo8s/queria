{{ config(materialized='table') }}

SELECT *
FROM read_csv(
    'https://data.address-br.digital.go.jp/mt_city/mt_city_all.csv.zip',
    header=true,
    auto_detect=true
)
