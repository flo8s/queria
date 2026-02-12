# マルチデータソース設計

複数のデータソースを扱うためのアーキテクチャ設計。

## 基本モデル

1データソース = 1 DuckLake。
1バケットに複数データソースをパスプレフィックスで格納する。

データソースの粒度は都市単位とは限らない。
複数都市を1データソースにまとめる判断もありうる。

## R2 バケット構造

バケット内でデータソースごとにパスプレフィックスで分離する。

```
s3://queria-dev/
├── tsukuba/
│   ├── ducklake.duckdb            メタデータ (DuckDB)
│   ├── ducklake.duckdb.files/     Parquet 実体 (DuckLake が自動管理)
│   └── catalog.json               カタログメタデータ
└── tsuchiura/
    ├── ducklake.duckdb
    ├── ducklake.duckdb.files/
    └── catalog.json
```

## dbt プロジェクト構成

単一プロジェクトにデータソースごとのサブディレクトリを置く。
共有マクロはプロジェクト直下の `macros/` に配置。

```
transform/
├── dbt_project.yml
├── profiles.yml
├── macros/
│   └── transform_population.sql
└── models/
    ├── tsukuba/
    │   ├── _catalog.yml
    │   ├── raw/
    │   ├── stg/
    │   └── mart/
    └── tsuchiura/
        ├── _catalog.yml
        ├── raw/
        ├── stg/
        └── mart/
```

各データソースのカタログ定義 (`_catalog.yml`) はそのサブディレクトリに置く。

## ターゲットとモデルの紐付け

dbt_project.yml でデータソースごとに `+database` を指定し、
対応する DuckLake に書き込む。

```yaml
models:
  transform:
    tsukuba:
      +database: tsukuba
    tsuchiura:
      +database: tsuchiura
```

profiles.yml の attach でデータソースごとの DuckLake を定義する。
`dbt run` 1回で全データソースをビルドできる。

## ビルドとデプロイ

`build.sh` は `DATASOURCES` 配列で全データソースを管理し、一括ビルドする。

```bash
./scripts/build.sh                # dev ビルド (全データソース)
./scripts/build.sh --target prd   # prd ビルド + R2 アップロード
```

## フロントエンド連携

データソースの一覧はフロントエンド (queria-web) のコード内で管理する。
各データソースの catalog.json URL を設定として保持し、fetch で取得する。
データソースの追加時はフロントエンドの設定変更 + 再デプロイで対応する。

集中管理用の index.json は設けない。
データソース数が増えて運用が困難になった時点で再検討する。

## 将来の分割パス

データソースごとに ingest 方法が異なる場合 (dbt 以外に dlt 等)、
データソースを独立したリポジトリに分割できる。

分割時の契約:
- 出力フォーマット: DuckLake + catalog.json (catalog-spec.md に準拠)
- デプロイ先: バケット内のパスプレフィックスで分離

共有マクロが必要な場合は dbt package として切り出す。
現時点では単一プロジェクトで十分なので、分割は必要になるまで行わない。
