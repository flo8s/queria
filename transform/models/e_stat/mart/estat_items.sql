SELECT DISTINCT
    LEFT(cat01, 1) AS category_prefix,
    cat01,
    cat01_metadata->>'$.name' AS item_name
FROM {{ ref('raw_social_demographic_municipal_basic') }}
ORDER BY cat01
