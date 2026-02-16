{{ config(materialized='table') }}

WITH base AS (
    SELECT
        CAST(lg_code AS VARCHAR) AS lg_code,
        LEFT(CAST(lg_code AS VARCHAR), 5) AS lg_code_5,
        LEFT(CAST(lg_code AS VARCHAR), 2) AS prefecture_code,
        pref_name AS pref,
        county_name AS county,
        city_name AS city,
        od_city_name AS ward,
        efct_date,
        ablt_date
    FROM {{ ref('raw_da_abr_city') }}
    WHERE ablt_date IS NULL OR ablt_date = ''
),

designated_city_mapping AS (
    SELECT
        lg_code_5,
        CASE city
            WHEN '札幌市'   THEN '01100'
            WHEN '仙台市'   THEN '04100'
            WHEN 'さいたま市' THEN '11100'
            WHEN '千葉市'   THEN '12100'
            WHEN '横浜市'   THEN '14100'
            WHEN '川崎市'   THEN '14130'
            WHEN '相模原市'  THEN '14150'
            WHEN '新潟市'   THEN '15100'
            WHEN '静岡市'   THEN '22100'
            WHEN '浜松市'   THEN '22130'
            WHEN '名古屋市'  THEN '23100'
            WHEN '京都市'   THEN '26100'
            WHEN '大阪市'   THEN '27100'
            WHEN '堺市'    THEN '27140'
            WHEN '神戸市'   THEN '28100'
            WHEN '岡山市'   THEN '33100'
            WHEN '広島市'   THEN '34100'
            WHEN '北九州市'  THEN '40100'
            WHEN '福岡市'   THEN '40130'
            WHEN '熊本市'   THEN '43100'
        END AS municipality_code
    FROM base
    WHERE ward IS NOT NULL AND ward != ''
),

hoppo_codes AS (
    SELECT unnest(['01695', '01696', '01697', '01698', '01699', '01700']) AS lg_code_5
)

SELECT
    b.lg_code,
    b.lg_code_5,
    b.prefecture_code,
    b.pref,
    b.county,
    b.city,
    b.ward,
    COALESCE(dc.municipality_code, b.lg_code_5) AS municipality_code,
    CASE WHEN b.ward IS NULL OR b.ward = '' THEN true ELSE false END AS is_local_gov,
    CASE WHEN hv.lg_code_5 IS NOT NULL THEN true ELSE false END AS is_hoppo_city
FROM base b
LEFT JOIN designated_city_mapping dc ON b.lg_code_5 = dc.lg_code_5
LEFT JOIN hoppo_codes hv ON b.lg_code_5 = hv.lg_code_5
