# queria

dbt + DuckLake + Cloudflare R2 を使ったオープンデータ公開基盤。

つくば市の人口データを取り込み、変換し、Frozen DuckLake としてR2に公開します。

## アーキテクチャ

```
オープンデータ (CSV)
    ↓
dbt-duckdb (ローカル)
    ↓
DuckLake (Parquet + メタデータ)
    ↓
Cloudflare R2 (公開)
    ↓
DuckDB WASM / CLI (クエリ)
```

## クイックスタート

### 公開データにアクセス

DuckDB CLIから:

```sql
ATTACH 'ducklake:https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev/queria.ducklake' AS queria;
SELECT * FROM queria.main.mart_tsukuba_population LIMIT 10;
```

または:

```bash
duckdb "ducklake:https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev/queria.ducklake" \
    -c "SELECT COUNT(*) FROM mart_tsukuba_population"
```

DuckDB WASMからも同様にアクセス可能です。

## 開発

### 前提条件

- Python 3.13+
- uv
- DuckDB CLI
- Node.js (wrangler用)

### セットアップ

```bash
# 依存関係インストール
uv sync
```

### プロジェクト構成

```
queria/
├── transform/                  # dbtプロジェクト
│   ├── models/
│   │   ├── raw/               # 外部CSVの取り込み
│   │   ├── stg/               # データ変換
│   │   └── mart/              # 公開用ビュー
│   ├── macros/                # 共通マクロ
│   ├── profiles.yml           # dbtプロファイル (dev/prod)
│   ├── queria.ducklake        # DuckLakeメタデータ
│   └── queria.ducklake.files/ # Parquetファイル (dev時)
├── scripts/
│   ├── dev.sh                 # ローカル開発用ビルド
│   ├── deploy.sh              # 本番デプロイ (R2書き込み + メタデータアップロード)
│   └── maintenance.sh         # DuckLakeメンテナンス (不要スナップショット・ファイル削除)
└── pyproject.toml
```

### dbtプロファイル

profiles.yml はリポジトリに含まれています (`transform/profiles.yml`)。

- dev target: ローカルにParquetを書き込み (`queria.ducklake.files/`)
- prod target: R2のS3パスに直接書き込み (環境変数 `R2_S3_DATA_PATH` が必要)

いずれの target も DuckLake の永続 data_path は公開HTTPS URLに固定されています。
OVERRIDE_DATA_PATH で書き込み先だけを切り替える構成です。

### ローカル開発

```bash
./scripts/dev.sh
```

DuckLake初期化 → dbt run (dev target) → カタログメタデータ生成を実行します。
Parquetは `transform/queria.ducklake.files/` にローカル生成されます。

### デプロイ

```bash
./scripts/deploy.sh
```

dbt run (prod target) で R2 に Parquet を直接書き込み、メタデータをアップロードします。

以下の環境変数が必要です (.env で設定):

| 変数名 | 説明 | 例 |
| --- | --- | --- |
| R2_ACCESS_KEY_ID | Cloudflare R2 アクセスキー | |
| R2_SECRET_ACCESS_KEY | Cloudflare R2 シークレットキー | |
| CLOUDFLARE_ACCOUNT_ID | Cloudflare アカウントID | |
| R2_S3_DATA_PATH | S3パス | s3://queria-dev/queria.ducklake.files/ |

R2認証は profiles.yml の settings で ACCOUNT_ID からエンドポイントを構築し、DuckDB に渡されます。

### R2 CORS設定

DuckDB WASMからアクセスする場合、R2バケットにCORS設定が必要です:

```json
[
  {
    "AllowedOrigins": ["http://localhost:3000"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["*"]
  }
]
```

## データソース

- つくば市オープンデータ: 町丁字別人口データ

## ライセンス

MIT
