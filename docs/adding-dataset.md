# データセット追加ガイド

## 全体の流れ

```
スキャフォールド → モデル実装 → python pipeline.py → 動作確認 → queria push
```

1. データセットディレクトリを作成し、`dataset.yml` にメタデータを定義
2. `pyproject.toml` に `queria`, `dbt-core`, `dbt-duckdb` を依存に追加
3. `pipeline.py` でビルドエントリポイントを実装
4. dbt モデルを実装（raw → stg → mart）
5. `uv run python pipeline.py` + `uv run queria metadata` でローカルビルド・確認
6. `uv run queria pull` + `uv run python pipeline.py` + `uv run queria metadata` + `uv run queria push` で本番デプロイ

## 1. データセットを作成する

データセットディレクトリを作成し、以下のファイルを配置する:

```
datasets/my_city/
├── dataset.yml
├── pyproject.toml
├── pipeline.py
└── transform/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── raw/
        ├── stg/
        └── mart/
```

### pyproject.toml

```toml
[project]
name = "dataset-my-city"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = ["queria", "dbt-core>=1.11.2", "dbt-duckdb>=1.10.0"]

[tool.uv.sources]
queria = { path = "../../", editable = true }
```

### pipeline.py

dbt のみのデータセット（最も一般的なパターン）:

```python
"""dbt ビルド + メタデータ生成パイプライン。"""

import sys

from dbt.cli.main import dbtRunner


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "dev"

    dbt = dbtRunner()

    dbt_args = ["--project-dir", "transform"]
    target_args = ["--target", target]

    result = dbt.invoke(["deps", *dbt_args])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbt.invoke(["run", *dbt_args, *target_args])
    if not result.success:
        raise SystemExit("dbt run failed")

    result = dbt.invoke(["docs", "generate", *dbt_args, *target_args])
    if not result.success:
        raise SystemExit("dbt docs generate failed")
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

### 依存関係がある例（k_oxon）

他のデータセットを参照する場合は `dependencies` を指定する:

```yaml
title: "K-Oxon データ"
description: "K-Oxon が公開するオープンデータ"
cover: "🗾"
tags: ["GIS", "行政区域", "境界", "地図"]
ducklake_url: "https://data.queria.io/k_oxon/ducklake.duckdb"
dependencies:
  - alias: "dwh"
    ducklake_url: "https://data.oxon-data.work/fdl_experiment/dwh.ducklake"

schemas:
  japan_map_for_bi:
    title: "Japan map for BI"
  e_stat:
    title: "E-Stat 統計データ"
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
      published: true
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
| published | No | `true` にするとフロントエンドのデータセット一覧に表示される（UI表示制御） |

`access: public` は dbt のモデルアクセス制御で、他の dbt プロジェクトから `ref()` できるようにする設定。
`meta.published: true` は queria 独自のフラグで、フロントエンドのデータセット一覧に表示するかどうかを制御する。metadata.json にはすべてのモデル（raw/stg/mart）が含まれる。

`config.contract.enforced: true` を設定すると、カラムの `data_type` と `constraints` がビルド時に検証される。mart 層では設定を推奨する。

## 4. ローカルでビルド・確認する

```bash
cd datasets/my_city

# 単一データセットのビルド
uv run python pipeline.py
uv run queria metadata

# 全データセットの一括ビルド
../../scripts/build.sh dev
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
./scripts/build.sh dev
./scripts/dev-serve.sh

# フロントエンド側（別ターミナル）
cd /path/to/queria-web && pnpm dev
```

## 5. 本番デプロイ

環境変数を設定し（`.env.example` 参照）、本番ターゲットでビルド・デプロイする:

```bash
cd datasets/my_city
uv run queria pull
uv run python pipeline.py
uv run queria metadata
uv run queria push
```

新しいデータセットを追加した場合、catalog の再ビルドも必要:

```bash
cd datasets/catalog
uv run queria pull
uv run python pipeline.py
uv run queria metadata
uv run queria push
```

GitHub Actions で main ブランチに push すると、自動デプロイが実行される。

## パターン: dlt + DuckLake による API データ取得

外部 API からデータを取得し、dlt パイプライン経由で DuckLake に直接書き込むパターン。
dlt の state 管理により、2回目以降のロードで既取得データをスキップできる。

### アーキテクチャ

```
[外部 API]
    ↓  dlt resource (ページネーション + パース)
[dlt pipeline (DuckLake destination)]
    ↓  write_disposition="merge" で upsert
    ↓  _dlt_pipeline_state に state 保存
[dist/ducklake.sqlite]
    ↓  queria push 時に DuckDB へ自動変換
[dist/ducklake.duckdb]
    ↓  dbt (ビューのみ)
    ↓  metadata.json 生成
[R2]
```

### ディレクトリ構成

```
datasets/my_api/
├── dataset.yml
├── pyproject.toml
├── pipeline.py            # ingestion + dbt + metadata
├── tables.yml             # テーブル定義（API パラメータ、merge_keys）
└── transform/
    ├── dbt_project.yml
    ├── profiles.yml
    └── models/
        ├── raw/           # SELECT columns FROM source (view)
        └── mart/          # フィルタ・加工ビュー (view)
```

### 事前準備

dlt は SQLite 形式の DuckLake カタログに書き込む。事前に初期化が必要:

```bash
uv run queria init --sqlite
```

### pipeline.py

ingestion（dlt パイプライン）と dbt ビルドを `pipeline.py` にまとめる。
`_create_destination` で DuckLake destination を構築する:

```python
import os
from pathlib import Path

import dlt
from dlt.destinations import ducklake
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

DIST_DIR = Path("dist")

def _create_destination():
    bucket = os.environ.get("S3_BUCKET")
    dataset_name = Path.cwd().name
    if bucket:
        storage = f"s3://{bucket}/{dataset_name}/ducklake.duckdb.files/"
    else:
        storage = str(DIST_DIR / "ducklake.duckdb.files") + "/"

    return ducklake(
        credentials=DuckLakeCredentials(
            catalog=str(DIST_DIR / "ducklake.sqlite"),
            storage=storage,
        ),
    )

def _ingest():
    destination = _create_destination()

    pipeline = dlt.pipeline(
        pipeline_name="my_pipeline",
        destination=destination,
        dataset_name="_source",
    )

    # dlt resource からデータを取得して DuckLake に書き込み
    load_info = pipeline.run(my_resource, write_disposition="merge")
```

### state 管理による増分スキップ

dlt は `_dlt_pipeline_state` テーブルにロード状態を保存する。
`dlt.current.resource_state()` を使って、既に取得済みのデータをスキップできる:

```python
@dlt.resource(write_disposition="merge", primary_key=["id"])
def my_resource():
    state = dlt.current.resource_state()
    if state.get("loaded"):
        return  # 取得済み → API 呼び出しをスキップ
    yield fetch_data()
    state["loaded"] = True
```

### dbt モデル

`_ingest()` が `_source` スキーマにテーブルを作成し、dbt がビューを作成する。
dlt が追加する `_dlt_load_id`, `_dlt_id` カラムは raw ビューで除外する:

```sql
-- models/raw/raw_my_table.sql
SELECT col1, col2, col3
FROM {{ source('my_source', 'my_table') }}
```

全て `materialized: view` にすることで、データの再コピーが発生せず増分更新と相性が良い。

### prd 環境への切り替え

`_create_destination` は `S3_BUCKET` 環境変数の有無で dev/prd を自動判定する:

- dev (S3_BUCKET 未設定): ローカルパス
- prd (S3_BUCKET 設定済): `s3://{bucket}/{dataset}/ducklake.duckdb.files/`

実装例は `datasets/e_stat/` を参照。
