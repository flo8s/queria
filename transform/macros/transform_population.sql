{% macro transform_population(source_table) %}
WITH renamed AS (
    SELECT
        "全国地方公共団体コード" as lg_code,
        "地域コード" as area_code,
        "地方公共団体名" as lg_name,
        "調査年月日"::date as reference_date,
        "地域名" as area_name,
        "0-4歳の男性" as male_0_4, "0-4歳の女性" as female_0_4,
        "5-9歳の男性" as male_5_9, "5-9歳の女性" as female_5_9,
        "10-14歳の男性" as male_10_14, "10-14歳の女性" as female_10_14,
        "15-19歳の男性" as male_15_19, "15-19歳の女性" as female_15_19,
        "20-24歳の男性" as male_20_24, "20-24歳の女性" as female_20_24,
        "25-29歳の男性" as male_25_29, "25-29歳の女性" as female_25_29,
        "30-34歳の男性" as male_30_34, "30-34歳の女性" as female_30_34,
        "35-39歳の男性" as male_35_39, "35-39歳の女性" as female_35_39,
        "40-44歳の男性" as male_40_44, "40-44歳の女性" as female_40_44,
        "45-49歳の男性" as male_45_49, "45-49歳の女性" as female_45_49,
        "50-54歳の男性" as male_50_54, "50-54歳の女性" as female_50_54,
        "55-59歳の男性" as male_55_59, "55-59歳の女性" as female_55_59,
        "60-64歳の男性" as male_60_64, "60-64歳の女性" as female_60_64,
        "65-69歳の男性" as male_65_69, "65-69歳の女性" as female_65_69,
        "70-74歳の男性" as male_70_74, "70-74歳の女性" as female_70_74,
        "75-79歳の男性" as male_75_79, "75-79歳の女性" as female_75_79,
        "80-84歳の男性" as male_80_84, "80-84歳の女性" as female_80_84,
        "85歳以上の男性" as male_85_plus, "85歳以上の女性" as female_85_plus
    FROM {{ source_table }}
)
SELECT
    lg_code, area_code, lg_name, reference_date, area_name,
    CASE WHEN age_sex LIKE 'male_%' THEN 'male' ELSE 'female' END as sex,
    CASE
        WHEN age_sex LIKE '%_0_4' THEN '0-4'
        WHEN age_sex LIKE '%_5_9' THEN '5-9'
        WHEN age_sex LIKE '%_10_14' THEN '10-14'
        WHEN age_sex LIKE '%_15_19' THEN '15-19'
        WHEN age_sex LIKE '%_20_24' THEN '20-24'
        WHEN age_sex LIKE '%_25_29' THEN '25-29'
        WHEN age_sex LIKE '%_30_34' THEN '30-34'
        WHEN age_sex LIKE '%_35_39' THEN '35-39'
        WHEN age_sex LIKE '%_40_44' THEN '40-44'
        WHEN age_sex LIKE '%_45_49' THEN '45-49'
        WHEN age_sex LIKE '%_50_54' THEN '50-54'
        WHEN age_sex LIKE '%_55_59' THEN '55-59'
        WHEN age_sex LIKE '%_60_64' THEN '60-64'
        WHEN age_sex LIKE '%_65_69' THEN '65-69'
        WHEN age_sex LIKE '%_70_74' THEN '70-74'
        WHEN age_sex LIKE '%_75_79' THEN '75-79'
        WHEN age_sex LIKE '%_80_84' THEN '80-84'
        WHEN age_sex LIKE '%_85_plus' THEN '85+'
    END as age_group,
    population
FROM renamed
UNPIVOT (
    population FOR age_sex IN (
        male_0_4, female_0_4, male_5_9, female_5_9,
        male_10_14, female_10_14, male_15_19, female_15_19,
        male_20_24, female_20_24, male_25_29, female_25_29,
        male_30_34, female_30_34, male_35_39, female_35_39,
        male_40_44, female_40_44, male_45_49, female_45_49,
        male_50_54, female_50_54, male_55_59, female_55_59,
        male_60_64, female_60_64, male_65_69, female_65_69,
        male_70_74, female_70_74, male_75_79, female_75_79,
        male_80_84, female_80_84, male_85_plus, female_85_plus
    )
)
{% endmacro %}
