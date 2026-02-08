#!/bin/bash
set -e

cd "$(dirname "$0")/.."

R2_BUCKET="${R2_BUCKET:-queria-dev}"
R2_PUBLIC_URL="${R2_PUBLIC_URL:-https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev}"

echo "=== Parquetファイルをアップロード ==="
find transform/queria.ducklake.files -name "*.parquet" | while read file; do
    # transform/からの相対パスを取得
    relative_path="${file#transform/}"
    echo "  ${relative_path}"
    npx wrangler r2 object put "${R2_BUCKET}/${relative_path}" --file="${file}" --remote
done

echo ""
echo "=== DuckLakeメタデータを準備 ==="
cp transform/queria.ducklake transform/queria.ducklake.public
duckdb transform/queria.ducklake.public -c "UPDATE ducklake_metadata SET value = '${R2_PUBLIC_URL}/queria.ducklake.files/' WHERE key = 'data_path';"

echo "=== DuckLakeメタデータをアップロード ==="
npx wrangler r2 object put "${R2_BUCKET}/queria.ducklake" --file="transform/queria.ducklake.public" --remote
rm transform/queria.ducklake.public

echo ""
echo "=== デプロイ完了 ==="
echo "アクセス: duckdb \"ducklake:${R2_PUBLIC_URL}/queria.ducklake\""
