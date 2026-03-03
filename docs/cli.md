# CLI リファレンス

queria CLI は Typer ベースのコマンドラインツール。
`uv run queria` で実行する。

## queria init

新規データセットのスカフォルディング。

```bash
uv run queria init <path>
```

| 引数 | 説明 |
|---|---|
| path | データセットディレクトリのパス（例: `datasets/my_city`） |

対話形式で Title, Description, Tags, DuckLake URL を入力する。
以下のファイルが生成される:

- `dataset.yml` - データセットメタデータ
- `transform/dbt_project.yml` - dbt プロジェクト設定
- `transform/profiles.yml` - dbt プロファイル（dev/prd）
- `transform/packages.yml` - dbt パッケージ依存（queria_common）
- `transform/models/raw/`, `stg/`, `mart/` - モデルディレクトリ

## queria pull

S3/R2 から ducklake.duckdb と metadata.json をダウンロードする。
CI 環境で既存データを取得する場合に使う。

```bash
uv run queria pull <path>
```

| 引数/オプション | 説明 |
|---|---|
| path | データセットディレクトリのパス |
| --bucket | S3 バケット名（環境変数 `S3_BUCKET` でも指定可） |

ダウンロードしたファイルは `dist/` に配置される。
S3 上にファイルが存在しない場合はスキップする。

環境変数 `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` が必要。

## queria run

データセットのビルドを実行する。

```bash
uv run queria run <path> [--target <target>] [--vars <json>]
```

| 引数/オプション | デフォルト | 説明 |
|---|---|---|
| path | | データセットディレクトリのパス |
| --target | dev | dbt ターゲット（profiles.yml で定義） |
| --vars | | dbt 変数（JSON 文字列） |

内部処理の流れ:

1. DuckLake 初期化（`dist/ducklake.duckdb` がなければ作成）
2. `dbt deps` - パッケージインストール
3. `dbt run` - モデル実行
4. `dbt docs generate` - ドキュメント生成
5. metadata.json 生成（dataset.yml + manifest.json + catalog.json から構築）

ビルド成果物は `dist/` に出力される:

| ファイル | 説明 |
|---|---|
| ducklake.duckdb | DuckLake カタログファイル |
| ducklake.duckdb.files/ | Parquet データファイル |
| metadata.json | メタデータ（フロントエンド用） |
| docs/ | dbt ドキュメント（index.html, manifest.json, catalog.json） |

### ターゲット

- dev: ローカルファイルシステムに Parquet を書き込む
- prd: S3/R2 パスに Parquet を書き込む（環境変数が必要）

## queria push

ビルド成果物を S3/R2 にアップロード、またはローカルディレクトリにコピーする。

```bash
# S3 にアップロード
uv run queria push <path> --bucket <bucket>

# ローカルにコピー
uv run queria push <path> --output-dir <dir>
```

| 引数/オプション | 説明 |
|---|---|
| path | データセットディレクトリのパス |
| --bucket | S3 バケット名（環境変数 `S3_BUCKET` でも指定可） |
| --output-dir | ローカル出力ディレクトリ |

`--bucket` と `--output-dir` のどちらか一方は必須。両方指定した場合は両方に出力する。

S3 アップロード時、ducklake.duckdb には `Cache-Control: no-cache` が設定される（クライアントが常に最新版を取得するため）。
