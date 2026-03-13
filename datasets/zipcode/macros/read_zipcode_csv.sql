{# 日本郵便 郵便番号データ (utf_ken_all.csv)
   https://www.post.japanpost.jp/zipcode/dl/utf-zip.html
   UTF-8, ヘッダーなし, 15カラム #}
{% macro read_zipcode_csv(url) %}
select *
from read_csv(
    '{{ url }}',
    header=false,
    columns={
        'lg_code':               'VARCHAR',
        'old_zipcode':           'VARCHAR',
        'zipcode':               'VARCHAR',
        'prefecture_kana':       'VARCHAR',
        'city_kana':             'VARCHAR',
        'town_kana':             'VARCHAR',
        'prefecture':            'VARCHAR',
        'city':                  'VARCHAR',
        'town':                  'VARCHAR',
        'has_multiple_zipcodes': 'INTEGER',
        'has_koaza_banchi':      'INTEGER',
        'has_chome':             'INTEGER',
        'has_multiple_towns':    'INTEGER',
        'update_status':         'INTEGER',
        'update_reason':         'INTEGER'
    }
)
{% endmacro %}
