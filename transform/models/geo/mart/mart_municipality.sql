{{ config(materialized='table') }}

{{ process_municipality_attrs(ref('stg_mlit_simplified')) }}
