{{ config(materialized='table') }}

SELECT *
FROM ST_Read(
    '/vsizip/vsicurl/https://nlftp.mlit.go.jp/ksj/gml/data/N03/N03-2025/N03-20250101_GML.zip/N03-20250101.shp'
)
