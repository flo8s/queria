## 概要

Queria で公開している全データセットのメタデータを統合したカタログデータセットです。
各データセットの metadata.json を読み込み、テーブル定義・カラム定義・リネージュ情報を一元管理しています。

## 主要テーブル

- mart_datasets: データセット一覧（タイトル、説明、タグ、DuckLake URL）
- mart_schemas: スキーマ一覧
- mart_tables: テーブル・ビュー定義（カラム情報、SQL、ライセンス等）
- mart_columns: カラム定義（名前、型、説明）
- mart_dependencies: データセット間の依存関係
- mart_search_entries: 全文検索用エントリ
- mart_lineage_nodes / mart_lineage_edges: データリネージュ情報
