## データ出典

[K-Oxon](https://github.com/K-Oxon/Japan-map-for-BI) が公開している日本の行政区域境界データと、e-Stat の統計データを組み合わせたデータセットです。

## スキーマ: japan_map_for_bi

日本の行政区域境界ポリゴン（GeoJSON）を収録しています。

- prefecture: 都道府県の境界ポリゴン（prefecture_code, prefecture_name, geometry）
- municipality: 市区町村の境界ポリゴン（lg_code, prefecture_code, prefecture_name, county_name, city_name, geometry）

## スキーマ: e_stat

e-Stat の社会・人口統計体系データを都道府県別・市区町村別に収録しています。
11カテゴリ（人口、土地、経済基盤、行財政、教育、労働、文化・スポーツ、居住、健康・医療、社会保障、安全）のテーブルがあります。

## ライセンス

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
