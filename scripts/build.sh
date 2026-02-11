#!/bin/bash
# デプロイ用アーティファクトをビルドする
# - dbt run: DuckLake (Parquet + メタデータ) を生成
# - build_catalog.py: カタログメタデータ (catalog_meta.json) を生成
set -e
cd "$(dirname "$0")/.."

echo "=== dbt run ==="
cd transform && uv run dbt run
cd ..

echo ""
echo "=== カタログメタデータを生成 ==="
uv run python scripts/build_catalog.py

echo ""
echo "=== ビルド完了 ==="
