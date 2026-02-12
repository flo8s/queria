#!/bin/bash
# ローカル開発用ビルド
# DuckLake初期化 + dbt run (dev target) + カタログメタデータ生成
set -e
cd "$(dirname "$0")/.."

R2_PUBLIC_URL="${R2_PUBLIC_URL:-https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev}"

echo "=== DuckLake 初期化チェック ==="
DUCKLAKE_FILE="transform/queria.ducklake"
if [ ! -f "${DUCKLAKE_FILE}" ]; then
    echo "DuckLakeを作成します (data_path: ${R2_PUBLIC_URL}/queria.ducklake.files/)"
    duckdb -c "
        ATTACH '${DUCKLAKE_FILE}' AS queria (
            TYPE ducklake,
            DATA_PATH '${R2_PUBLIC_URL}/queria.ducklake.files/'
        );
    "
fi

echo ""
echo "=== dbt run ==="
cd transform && uv run dbt run
cd ..

echo ""
echo "=== カタログメタデータを生成 ==="
uv run python scripts/build_catalog.py

echo ""
echo "=== ビルド完了 ==="
