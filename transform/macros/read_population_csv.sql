{# 自治体標準オープンデータセット「地域・年齢別人口」のスキーマ定義
   https://www.digital.go.jp/resources/open_data/municipal-standard-data-set-test #}
{% macro read_population_csv(url) %}
select *
from read_csv(
    '{{ url }}',
    header=true,
    encoding='shift_jis',
    null_padding=true,
    dtypes={
        '全国地方公共団体コード': 'VARCHAR',
        '地域コード': 'VARCHAR',
        '調査年月日': 'DATE',
        '備考': 'VARCHAR'
    }
)
{% endmacro %}
