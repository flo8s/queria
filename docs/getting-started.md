# はじめに

## 前提条件

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)

## セットアップ

```bash
uv sync
```

本番デプロイや `fetch` コマンドを使う場合は、`.env.example` を参考に環境変数を設定する:

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
│   │   └── transform/     #     dbt プロジェクト
│   │       ├── models/
│   │       │   ├── raw/   #       外部データ取り込み
│   │       │   ├── stg/   #       変換・正規化
│   │       │   └── mart/  #       公開用ビュー
│   │       └── profiles.yml
│   ├── zipcode/
│   ├── e_stat/
│   └── catalog/           #   全データセットの統合カタログ
├── packages/
│   └── queria_common/     # 共有 dbt マクロ
├── src/
│   └── queria/            # CLI ツール
├── scripts/               # ビルド・デプロイスクリプト
└── pyproject.toml
```

各データセットは独立した dbt プロジェクトを持ち、`dataset.yml` でメタデータを定義する。

## 開発ワークフロー

### 単一データセットのビルド

```bash
uv run queria run datasets/tsukuba
```

### 全データセットの一括ビルド

```bash
./scripts/dev-build.sh
```

このスクリプトは全データセットをビルドし、`.dev-serve/` にまとめる。
ローカルサーバーで確認する:

```bash
npx serve .dev-serve --cors -l 4000
```

### 本番デプロイ

手動デプロイ:

```bash
uv run queria run datasets/tsukuba --target prd
uv run queria freeze datasets/tsukuba --bucket queria
```

catalog データセットは他データセットの metadata.json を参照するため、最後にビルドする:

```bash
# 1. 各データセットをビルド・デプロイ
uv run queria run datasets/tsukuba --target prd
uv run queria freeze datasets/tsukuba
uv run queria run datasets/e_stat --target prd
uv run queria freeze datasets/e_stat

# 2. 最後に catalog をビルド・デプロイ
uv run queria run datasets/catalog --target prd
uv run queria freeze datasets/catalog
```

### GitHub Actions

main ブランチへの push で自動デプロイが実行される（`.github/workflows/deploy.yml`）。
`scripts/prd-deploy.sh` が全データセットのビルドとデプロイを行う。
