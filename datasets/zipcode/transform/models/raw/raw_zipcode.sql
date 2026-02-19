{{
    config(
        materialized='table'
    )
}}

{{ read_zipcode_csv(
    'zip://https://www.post.japanpost.jp/zipcode/dl/utf/zip/utf_ken_all.zip/utf_ken_all.csv'
) }}
