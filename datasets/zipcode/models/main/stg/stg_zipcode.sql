{{ config(materialized='view') }}

SELECT
    {{ lg_code_with_check_digit('lg_code') }} AS lg_code,
    old_zipcode,
    zipcode,
    prefecture_kana,
    city_kana,
    town_kana,
    prefecture,
    city,
    town,
    has_multiple_zipcodes,
    has_koaza_banchi,
    has_chome,
    has_multiple_towns,
    update_status,
    update_reason
FROM {{ ref('raw_zipcode') }}
