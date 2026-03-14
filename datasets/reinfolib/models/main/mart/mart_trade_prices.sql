{{ config(materialized='view') }}

select * from {{ ref('stg_trade_prices') }}
