# データセット追加ガイド

## 全体の流れ

```
queria init → モデル実装 → queria run (dev) → 動作確認 → queria run (prd) → queria freeze
```

1. `queria init` でスカフォルディング
2. `dataset.yml` にメタデータを定義
3. dbt モデルを実装（raw → stg → mart）
4. `queria run` でローカルビルド・確認
5. `queria run --target prd` + `queria freeze` で本番デプロイ

## 1. データセットを作成する

```bash
uv run queria init datasets/my_city
```

対話形式で Title, Description, Tags, DuckLake URL を入力する。
データセット名はアルファベット・数字・アンダースコアのみ使用可能。

生成されるディレクトリ:

```
datasets/my_city/
├── dataset.yml
└── transform/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── raw/
        ├── stg/
        └── mart/
```

## 2. dataset.yml を編集する

dataset.yml はデータセット全体のメタデータを定義するファイル。
フロントエンドのデータセット一覧画面やメタデータ生成に使われる。

### 基本例（tsukuba）

```yaml
title: "つくば市オープンデータ"
description: "つくば市が公開している統計データ"
cover: "🏙️"
tags: ["オープンデータ", "つくば市"]
ducklake_url: "https://data.queria.io/tsukuba/ducklake.duckdb"

schemas:
  main:
    title: "メイン"
```

### 依存関係がある例（e_stat）

他のデータセットを参照する場合は `dependencies` を指定する:

```yaml
title: "E-Stat 統計データ"
description: "e-Stat API から取得した統計データ"
cover: "📊"
tags: ["e-stat", "統計"]
ducklake_url: "https://data.queria.io/e_stat/ducklake.duckdb"
dependencies:
  - alias: "dwh"
    ducklake_url: "https://data.oxon-data.work/fdl_experiment/dwh.ducklake"

schemas:
  main:
    title: "メイン"
  main_municipal:
    title: "市区町村統計"
  main_pref:
    title: "都道府県統計"
```

### フィールド一覧

| フィールド | 必須 | 説明 |
|---|---|---|
| title | Yes | データセットの表示名 |
| description | Yes | データセットの説明 |
| cover | No | カバー画像（絵文字） |
| tags | No | 分類タグの配列 |
| ducklake_url | Yes | R2 上の DuckLake 公開 URL |
| schemas | No | スキーマ名と表示名のマッピング |
| dependencies | No | 他データセットへの依存 |

ducklake_url は `https://data.queria.io/{データセット名}/ducklake.duckdb` の形式で命名する。

## 3. dbt モデルを実装する

データモデルは raw → stg → mart の3層構成。

### 3.1. raw: 外部データの取り込み

外部の CSV/JSON を DuckDB の `read_csv` / `read_json` で直接取り込む。

```sql
-- models/raw/raw_my_city_population_20240401.sql
{{
    config(
        materialized='table'
    )
}}

{{ read_population_csv(
    'https://example.com/data/population_20240401.csv'
) }}
```

取り込み用のマクロは `transform/macros/` に定義する。
Shift-JIS の CSV を読む場合の例:

```sql
-- macros/read_population_csv.sql
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
        '調査年月日': 'DATE'
    }
)
{% endmacro %}
```

同じ構造の CSV が複数期間ある場合、期間ごとにモデルを作成する:

```
models/raw/
├── raw_my_city_population_20240401.sql
├── raw_my_city_population_20240501.sql
└── raw_my_city_population_20241001.sql
```

raw 層のスキーマ定義（`models/raw/_schema.yml`）:

```yaml
version: 2

models:
  - name: raw_my_city_population_20240401
    description: 人口データ（2024年4月1日時点）の生データ
    columns: &raw_columns
      - name: '"全国地方公共団体コード"'
        description: 全国地方公共団体コード
      - name: '"調査年月日"'
        description: 調査年月日
  - name: raw_my_city_population_20240501
    description: 人口データ（2024年5月1日時点）の生データ
    columns: *raw_columns
```

YAML アンカー（`&raw_columns` / `*raw_columns`）で同じカラム定義を共有できる。

### 3.2. stg: データ変換・正規化

raw データを正規化する。日本語カラム名の英語化、UNPIVOT、型変換などを行う。

```sql
-- models/stg/stg_my_city_population_20240401.sql
{{ config(materialized='view') }}

{{ transform_population(ref('raw_my_city_population_20240401')) }}
```

変換用マクロで UNPIVOT や名前変換を実装する:

```sql
-- macros/transform_population.sql
{% macro transform_population(source_table) %}
WITH renamed AS (
    SELECT
        "全国地方公共団体コード" as lg_code,
        "調査年月日" as reference_date,
        "地域名" as area_name,
        ...
    FROM {{ source_table }}
)
SELECT * FROM renamed
{% endmacro %}
```

### 3.3. mart: 公開用ビューとメタ情報設定

mart 層は外部に公開するビュー。ここでのメタ情報設定がフロントエンドの表示に直結する。

SQL ファイル（複数の stg を UNION ALL で統合）:

```sql
-- models/mart/mart_my_city_population.sql
{{ config(materialized='view') }}

SELECT * FROM {{ ref('stg_my_city_population_20240401') }}
UNION ALL SELECT * FROM {{ ref('stg_my_city_population_20240501') }}
UNION ALL SELECT * FROM {{ ref('stg_my_city_population_20241001') }}
```

スキーマ定義ファイルでメタ情報を設定する:

```yaml
# models/mart/mart_my_city_population.yml
version: 2

models:
  - name: mart_my_city_population
    description: 全期間の人口データを統合したマートテーブル
    access: public
    config:
      contract:
        enforced: true
    meta:
      title: "人口統計"
      tags: ["人口", "統計"]
      license: "CC BY 4.0"
      license_url: "https://creativecommons.org/licenses/by/4.0/"
      source_url: "https://example.com/"
      public: true
    columns:
      - name: lg_code
        description: 全国地方公共団体コード
        data_type: VARCHAR
        constraints:
          - type: not_null
      - name: reference_date
        description: データの調査年月日
        data_type: DATE
        constraints:
          - type: not_null
      - name: area_name
        description: 地域の名称
        data_type: VARCHAR
        constraints:
          - type: not_null
```

### meta フィールドの説明

| フィールド | 必須 | 説明 |
|---|---|---|
| title | Yes | フロントエンドでの表示名 |
| tags | No | 分類タグの配列 |
| license | No | ライセンス名（例: "CC BY 4.0"） |
| license_url | No | ライセンスの URL |
| source_url | No | データの出典 URL |
| public | Yes | `true` にすると metadata.json に含まれ、フロントエンドに表示される |

`access: public` は dbt のモデルアクセス制御で、他の dbt プロジェクトから `ref()` できるようにする設定。
`meta.public: true` は queria 独自のフラグで、フロントエンドに公開するかどうかを制御する。

`config.contract.enforced: true` を設定すると、カラムの `data_type` と `constraints` がビルド時に検証される。mart 層では設定を推奨する。

## 4. ローカルでビルド・確認する

```bash
# 単一データセットのビルド
uv run queria run datasets/my_city

# 全データセットの一括ビルド
./scripts/dev-build.sh
```

ビルドすると `dist/` に以下が生成される:

```
datasets/my_city/dist/
├── ducklake.duckdb          # DuckLake カタログファイル
├── ducklake.duckdb.files/   # Parquet ファイル群
├── metadata.json            # メタデータ（フロントエンド用）
└── docs/                    # dbt ドキュメント
```

DuckDB CLI でデータを確認:

```bash
duckdb "ducklake:datasets/my_city/dist/ducklake.duckdb" \
    -c "SELECT * FROM mart_my_city_population LIMIT 10"
```

metadata.json の内容を確認:

```bash
cat datasets/my_city/dist/metadata.json | python -m json.tool
```

フロントエンド（queria-web）と合わせて確認する場合:

```bash
# パイプライン側
./scripts/dev-build.sh
npx serve .dev-serve --cors -l 4000

# フロントエンド側（別ターミナル）
cd /path/to/queria-web && pnpm dev
```

## 5. 本番デプロイ

環境変数を設定し（`.env.example` 参照）、本番ターゲットでビルド・デプロイする:

```bash
uv run queria run datasets/my_city --target prd
uv run queria freeze datasets/my_city
```

新しいデータセットを追加した場合、catalog の再ビルドも必要:

```bash
uv run queria run datasets/catalog --target prd
uv run queria freeze datasets/catalog
```

GitHub Actions で main ブランチに push すると、`scripts/prd-deploy.sh` が自動実行される。
