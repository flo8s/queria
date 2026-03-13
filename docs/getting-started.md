# はじめに

## 前提条件

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## セットアップ

```bash
uv sync
```

本番デプロイや `pull` コマンドを使う場合は、`.env.example` を参考に環境変数を設定する:

| 変数 | 説明 | 例 |
|---|---|---|
| S3_ENDPOINT | S3互換エンドポイント | `<account_id>.r2.cloudflarestorage.com` |
| S3_ACCESS_KEY_ID | アクセスキー | |
| S3_SECRET_ACCESS_KEY | シークレットキー | |
| S3_BUCKET | バケット名 | dev: `queria-dev` / prd: `queria` |

## プロジェクト構造

```
queria/
├── datasets/              # 各データセットの定義
│   ├── tsukuba/           #   つくば市オープンデータ
│   │   ├── dataset.yml    #     メタデータ定義
│   │   ├── pipeline.py    #     ビルドエントリポイント
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   └── models/        #     dbt モデル
│   │       ├── raw/       #       外部データ取り込み
│   │       ├── stg/       #       変換・正規化
│   │       └── mart/      #       公開用ビュー
│   ├── zipcode/
│   ├── k_oxon/            #   K-Oxon データ (GIS + e-Stat)
│   └── catalog/           #   全データセットの統合カタログ
├── packages/
│   └── queria_common/     # 共有 dbt マクロ
├── src/
│   └── fdl/               # DuckLake カタログ管理 CLI
├── scripts/               # ビルド・デプロイスクリプト
└── pyproject.toml
```

各データセットは独立した dbt プロジェクトを持ち、`dataset.yml` でメタデータを定義する。

## 開発ワークフロー

### 単一データセットのビルド

```bash
cd datasets/tsukuba
uv run python pipeline.py
uv run fdl metadata
```

### 全データセットの一括ビルド

```bash
./scripts/build.sh dev
```

このスクリプトは全データセットをビルドし、`.dev-serve/` にまとめる。
ローカルサーバーで確認する:

```bash
./scripts/dev-serve.sh
```

### stg 環境へのデプロイ

R2 の `queria-dev` バケットにデプロイして動作確認する:

```bash
./scripts/build.sh stg
```

### 本番デプロイ

手動デプロイ:

```bash
cd datasets/tsukuba
uv run fdl pull
uv run python pipeline.py
uv run fdl metadata
uv run fdl push
```

catalog データセットは他データセットの metadata.json を参照するため、最後にビルドする:

```bash
# 1. 各データセットをビルド・デプロイ
cd datasets/tsukuba
uv run fdl pull && uv run python pipeline.py && uv run fdl metadata && uv run fdl push
cd ../k_oxon
uv run fdl pull && uv run python pipeline.py && uv run fdl metadata && uv run fdl push

# 2. 最後に catalog をビルド・デプロイ
cd ../catalog
uv run fdl pull && uv run python pipeline.py && uv run fdl metadata && uv run fdl push
```

### GitHub Actions

main ブランチへの push で自動デプロイが実行される（`.github/workflows/deploy.yml`）。
