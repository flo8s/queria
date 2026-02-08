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

# dbt実行
cd transform && uv run dbt run
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
│   ├── queria.ducklake        # DuckLakeメタデータ
│   └── queria.ducklake.files/ # Parquetファイル
├── scripts/
│   └── deploy.sh              # R2デプロイスクリプト
└── pyproject.toml
```

### dbtプロファイル設定

`~/.dbt/profiles.yml` に以下を追加:

```yaml
transform:
  outputs:
    dev:
      type: duckdb
      path: "ducklake:queria.ducklake"
      threads: 1
      extensions:
        - httpfs
        - ducklake
  target: dev
```

### デプロイ

```bash
# dbtでDuckLakeを構築
cd transform && uv run dbt run

# R2にアップロード
./scripts/deploy.sh
```

deploy.shは以下を行います:
1. ParquetファイルをR2にアップロード
2. メタデータのdata_pathをR2のURLに書き換え
3. メタデータをR2にアップロード

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
