# queria 開発ガイド

dbt + DuckLake + Cloudflare R2 によるオープンデータ公開パイプライン。
マルチデータソース対応 (tsukuba, k_oxon, catalog, articles, zipcode, e_stat)。
各データセットが queria をフレームワークとして依存し、データセットディレクトリ内から CLI を実行する。

## セットアップ

```bash
# 前提: Python 3.13, uv
cd datasets/tsukuba   # 任意のデータセットディレクトリに移動
uv sync               # そのデータセットの依存をインストール
cp ../../.env.example ../../.env  # S3 認証情報を設定
```

## CLI コマンド

データセットディレクトリ内（または配下）で実行する。カレントディレクトリから上方向に `dataset.yml` を探索して自動検出する。

```bash
cd datasets/tsukuba
uv run queria init          # DuckLake カタログ初期化
uv run queria pull          # S3 から ducklake.duckdb + metadata.json をダウンロード
uv run queria run           # ビルド (DuckLake 初期化 → dbt run → メタデータ生成)
uv run queria push          # S3 またはローカルへデプロイ
```

`queria new <path>` のみ path 引数を取る（新規スキャフォールド用）。

ビルドスクリプト:

```bash
scripts/dev-build.sh    # 全データセットをローカルビルド (dev ターゲット)
scripts/prd-deploy.sh   # 全データセットをビルド + S3 アップロード (prd ターゲット)
```

## プロジェクト構造

```
datasets/
  {datasource}/
    dataset.yml              # メタデータ定義 (title, description, ducklake_url, schemas)
    pyproject.toml           # queria を dependency として指定
    ingestion/               # (オプション) データ取得スクリプト
      pipeline.py            # main() を実装
    transform/               # dbt プロジェクト
      dbt_project.yml
      profiles.yml           # dev (ローカル) / prd (S3) ターゲット
      models/
        raw/                 # 外部データ取り込み (materialized: table)
        stg/                 # 変換・正規化 (materialized: view)
        mart/                # 公開用ビュー (materialized: view)
      macros/
    dist/                    # ビルド成果物 (生成物)
      ducklake.duckdb
      ducklake.duckdb.files/ # Parquet データ
      metadata.json

src/queria/
  cli.py              # Typer CLI エントリポイント
  ingestion.py         # パイプラインスクリプト実行 + SQLite→DuckDB 自動変換
  dlt.py              # dlt + DuckLake destination ヘルパー (create_destination)
  ducklake.py          # DuckLake DuckDB カタログ初期化
  ducklake_patch.py    # dlt の DuckLake destination モンキーパッチ
  transform.py         # ビルドパイプライン (init_ducklake, dbt run, メタデータ生成)
  push.py              # S3 アップロード / ローカルコピー
  pull.py              # S3 ダウンロード
  init.py              # データセットスキャフォールド
  config_schema.py     # Pydantic DatasetConfig モデル
  metadata_schema.py   # Pydantic メタデータモデル
```

## ingestion 規約

データセット側の `ingestion/pipeline.py` は引数なしの `main()` 関数を実装する。
フレームワークがコンテキスト（dataset_dir 等）を管理するため、パイプライン作者はフレームワーク内部を意識する必要がない。

ingestion パターン:

1. dlt パイプライン: `queria.dlt.create_destination()` で DuckLake destination を取得し dlt 経由で書き込み。SQLite→DuckDB 変換はフレームワークが自動実行
2. ファイル取得: ローカルにファイル/DB を用意するだけ。dbt raw 層が参照

## dbt モデル構造

raw → stg → mart の 3 層構成:

- raw: `read_csv_auto()` で外部 CSV を取り込み (Shift-JIS 対応)。materialized は table
- stg: 日本語カラム名の英語化、UNPIVOT で正規化。materialized は view
- mart: 全期間の stg モデルを UNION ALL で統合。materialized は view

命名規則: `{layer}_{datasource}_{domain}_{period}` (例: `raw_tsukuba_population_20240401`)

公開フラグ: dbt モデルの YAML で `meta.published: true` を設定するとフロントエンドのデータセット一覧に表示される（UI表示制御のみ。metadata.json にはすべてのモデルが含まれる）。

profiles.yml の各データセットに dev/prd ターゲットがある:
- dev: `:memory:` パス、ローカルファイルアタッチ
- prd: `:memory:` パス、S3 設定 (環境変数から読み込み)

## DuckLake 設計

duckdb-wasm からは絶対 HTTP URL でしか Parquet ファイルを参照できない。
そのため DATA_PATH を空文字にしてはいけない。

フロー:
1. init_ducklake: dataset.yml の ducklake_url から公開 URL を導出し DATA_PATH に設定
   - 例: `https://data.queria.io/tsukuba/ducklake.duckdb.files/`
2. dbt run (prd target): OVERRIDE_DATA_PATH で S3 書き込み先に一時的に上書き
   - OVERRIDE_DATA_PATH はセッション限定でメタデータの data_path を変更しない

ducklake_url の正規ソースは各データセットの `dataset.yml` ファイル。

## メタデータ生成

`queria run` は dbt run の後に metadata.json を自動生成する。

生成フロー:
1. dataset.yml → DatasetConfig をロード
2. dbt の manifest.json / catalog.json からすべてのモデル情報を抽出
3. lineage (DAG) を構築
4. dist/metadata.json に出力

詳細仕様は `docs/metadata-spec.md` を参照。

## デプロイ

R2 バケット構造:

```
s3://{bucket}/
  {datasource}/
    ducklake.duckdb
    ducklake.duckdb.files/
    metadata.json
```

環境変数 (.env):
- S3_ENDPOINT: Cloudflare R2 エンドポイント
- S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY: R2 認証情報
- S3_BUCKET: バケット名 (dev: `queria-dev`, prd: `queria`)

CI/CD: GitHub Actions で main push 時に `scripts/prd-deploy.sh` を実行。
catalog データセットは他のデータセットの metadata.json に依存するため最後にビルドされる。

## コーディング規約

Python:
- 型ヒント必須 (Python 3.13+)
- Pydantic v2 でバリデーション
- snake_case (関数/変数)、PascalCase (クラス)

dbt:
- YAML アンカー (`*raw_columns`) でカラム定義を再利用
- Jinja2 マクロで共通ロジックを抽出
- モデル名は小文字 + アンダースコア

コミットメッセージ: 日本語で記述

## ドキュメント

開発者向けドキュメントを MkDocs Material で管理している（`docs/`, `mkdocs.yml`）。
機能追加や変更を行った場合、関連するドキュメントも更新すること。

対象ページ:
- `docs/getting-started.md` - セットアップと開発ワークフロー
- `docs/adding-dataset.md` - データセット追加ガイド
- `docs/cli.md` - CLI リファレンス
