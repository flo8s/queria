#!/bin/bash
# 本番デプロイ
# dbt run (prod target) で R2 に Parquet を直接書き込み、メタデータをアップロードする
#
# 以下の環境変数が必要 (.env で設定):
#   R2_ACCESS_KEY_ID       - Cloudflare R2 アクセスキー
#   R2_SECRET_ACCESS_KEY   - Cloudflare R2 シークレットキー
#   CLOUDFLARE_ACCOUNT_ID  - Cloudflare アカウントID
#   R2_S3_DATA_PATH        - S3パス (例: s3://queria-dev/queria.ducklake.files/)
#
# R2認証は profiles.yml の settings で DuckDB に渡される。
set -e
cd "$(dirname "$0")/.."

R2_BUCKET="${R2_BUCKET:-queria-dev}"
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
echo "=== dbt run (prod) ==="
cd transform && uv run dbt run --target prod
cd ..

echo ""
echo "=== カタログメタデータを生成 ==="
uv run python scripts/build_catalog.py

echo ""
echo "=== メタデータをアップロード ==="
npx wrangler r2 object put "${R2_BUCKET}/queria.ducklake" \
  --file="transform/queria.ducklake" --remote

npx wrangler r2 object put "${R2_BUCKET}/catalog.json" \
  --file="transform/target/catalog_meta.json" --remote

echo ""
echo "=== デプロイ完了 ==="
echo "アクセス: duckdb \"ducklake:${R2_PUBLIC_URL}/queria.ducklake\""
