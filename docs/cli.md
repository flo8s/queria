# CLI リファレンス

fdl CLI は DuckLake カタログ管理に特化したコマンドラインツール。
データセットディレクトリ内で `uv run fdl` で実行する。カレントディレクトリから上方向に `dataset.yml` を探索して自動検出する。

ビルド（dbt 実行）は各データセットの `pipeline` エントリポイントが担当する。

## fdl init

DuckLake カタログを初期化する。

```bash
uv run fdl init [--sqlite]
```

| オプション | 説明 |
|---|---|
| --sqlite | SQLite 形式でカタログを初期化する（dlt 連携用） |

`dist/ducklake.duckdb` を作成し、`dataset.yml` の `ducklake_url` から DATA_PATH を設定する。
`--sqlite` を指定した場合は `dist/ducklake.sqlite` を作成する（dlt が SQLite カタログに書き込み、push 時に DuckDB へ変換される）。

S3 上にカタログが存在しない場合、`fdl pull` が自動的に `init` を実行する。

## fdl pull

S3/R2 から ducklake.duckdb と metadata.json をダウンロードする。
S3 上にファイルが存在しない場合は `fdl init` で新規作成する。

```bash
uv run fdl pull
```

| オプション | 説明 |
|---|---|
| --bucket | S3 バケット名（環境変数 `S3_BUCKET` でも指定可） |

ダウンロードしたファイルは `dist/` に配置される。

環境変数 `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` が必要。

## fdl metadata

metadata.json を生成し、dbt ドキュメントを dist/ にコピーする。

```bash
uv run fdl metadata
```

dbt の成果物（manifest.json, catalog.json）と dataset.yml から metadata.json を構築する。
`uv run pipeline` の後に実行する。

生成されるファイル:

| ファイル | 説明 |
|---|---|
| dist/metadata.json | メタデータ（フロントエンド用） |
| dist/docs/ | dbt ドキュメント（index.html, manifest.json, catalog.json） |

## fdl push

ビルド成果物を S3/R2 にアップロード、またはローカルディレクトリにコピーする。
SQLite カタログ（dlt 使用時）がある場合は DuckDB 形式に自動変換してからアップロードする。

```bash
# S3 にアップロード
uv run fdl push

# ローカルにコピー
uv run fdl push --output-dir <dir>
```

| オプション | 説明 |
|---|---|
| --bucket | S3 バケット名（環境変数 `S3_BUCKET` でも指定可） |
| --output-dir | ローカル出力ディレクトリ |

`--bucket` と `--output-dir` のどちらか一方は必須。両方指定した場合は両方に出力する。

S3 アップロード時、ducklake.duckdb には `Cache-Control: no-cache` が設定される（クライアントが常に最新版を取得するため）。

## fdl gc

R2 上の孤立した Parquet ファイルを削除する。

```bash
uv run fdl gc --bucket <bucket> [--force] [--older-than-days <days>]
```

| オプション | デフォルト | 説明 |
|---|---|---|
| --bucket | | S3 バケット名（環境変数 `S3_BUCKET` でも指定可） |
| --force | false | 確認なしで削除する |
| --older-than-days | 7 | 指定日数以上経過したファイルのみ対象 |

## pipeline エントリポイント

各データセットに `pipeline.py` が配置されており、`uv run python pipeline.py` で dbt ビルドを実行する。

```bash
uv run python pipeline.py
```

典型的なワークフロー:

```bash
cd datasets/tsukuba
uv run fdl pull
uv run python pipeline.py
uv run fdl metadata
uv run fdl push
```
