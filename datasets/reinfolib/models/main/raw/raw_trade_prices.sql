{{ config(materialized='table') }}

select *
from {{ source('reinfolib_source', 'trade_prices') }}
