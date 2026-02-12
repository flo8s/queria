#!/bin/bash
# DuckLake ビルドスクリプト
#
# 使い方:
#   ./scripts/build.sh                # ローカルビルド (dev)
#   ./scripts/build.sh --target prd   # R2に書き込み + メタデータアップロード
#
# prd の場合、以下の環境変数が必要 (.env で設定):
#   R2_ACCESS_KEY_ID       - Cloudflare R2 アクセスキー
#   R2_SECRET_ACCESS_KEY   - Cloudflare R2 シークレットキー
#   CLOUDFLARE_ACCOUNT_ID  - Cloudflare アカウントID
#   R2_S3_DATA_PATH        - S3パス (例: s3://queria-dev/queria.ducklake.files/)
set -e
cd "$(dirname "$0")/.."

# --target の値を取得 (デフォルト: dev)
TARGET="dev"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target) TARGET="$2"; shift 2 ;;
        *) echo "不明なオプション: $1" >&2; exit 1 ;;
    esac
done

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

echo "=== dbt run (${TARGET}) ==="
(cd transform && uv run dbt run --target "${TARGET}")

echo "=== カタログメタデータを生成 ==="
uv run python scripts/build_catalog.py

if [ "${TARGET}" = "prd" ]; then
  echo "=== メタデータをアップロード ==="
  npx wrangler r2 object put "${R2_BUCKET}/queria.ducklake" \
    --file="transform/queria.ducklake" --remote

  npx wrangler r2 object put "${R2_BUCKET}/catalog.json" \
    --file="transform/target/catalog_meta.json" --remote

  echo "=== デプロイ完了 ==="
  echo "アクセス: duckdb \"ducklake:${R2_PUBLIC_URL}/queria.ducklake\""
else
  echo "=== ビルド完了 ==="
fi
