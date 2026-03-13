{# 5桁の地方公共団体コード (JIS X0402) にチェックディジットを付与して6桁にする
   アルゴリズム: 総務省「全国地方公共団体コード仕様」(モジュラス11)
   https://www.soumu.go.jp/main_content/000137948.pdf
   Sum = d1×6 + d2×5 + d3×4 + d4×3 + d5×2
   Remainder = Sum mod 11
   CheckDigit = 0 (Remainder=1), 1 (Remainder=0), 11-Remainder (その他) #}
{% macro lg_code_with_check_digit(column) %}
{%- set sum_expr -%}
(
    CAST(SUBSTR({{ column }}, 1, 1) AS INTEGER) * 6 +
    CAST(SUBSTR({{ column }}, 2, 1) AS INTEGER) * 5 +
    CAST(SUBSTR({{ column }}, 3, 1) AS INTEGER) * 4 +
    CAST(SUBSTR({{ column }}, 4, 1) AS INTEGER) * 3 +
    CAST(SUBSTR({{ column }}, 5, 1) AS INTEGER) * 2
)
{%- endset -%}
{{ column }} || CAST(
    CASE
        WHEN {{ sum_expr }} % 11 = 0 THEN 1
        WHEN {{ sum_expr }} % 11 = 1 THEN 0
        ELSE 11 - {{ sum_expr }} % 11
    END AS VARCHAR
)
{%- endmacro %}
