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
#   R2_S3_BUCKET_PATH      - S3バケットパス (例: s3://queria-dev)
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

# models/ 配下の _catalog.yml を持つディレクトリをデータソースとして検出
DATASOURCES=()
for catalog_yml in transform/models/*/_catalog.yml; do
  DATASOURCES+=("$(basename "$(dirname "${catalog_yml}")")")
done
if [ ${#DATASOURCES[@]} -eq 0 ]; then
  echo "Error: データソースが見つかりません (transform/models/*/_catalog.yml)" >&2
  exit 1
fi

R2_BUCKET="${R2_BUCKET:-queria-dev}"
R2_PUBLIC_URL="${R2_PUBLIC_URL:-https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev}"

echo "=== DuckLake 初期化チェック ==="
for ds in "${DATASOURCES[@]}"; do
  DUCKLAKE_FILE="transform/${ds}.ducklake"
  if [ ! -f "${DUCKLAKE_FILE}" ]; then
    echo "DuckLake を作成します: ${ds} (data_path: ${R2_PUBLIC_URL}/${ds}/ducklake.duckdb.files/)"
    duckdb -c "
      ATTACH '${DUCKLAKE_FILE}' AS ${ds} (
        TYPE ducklake,
        DATA_PATH '${R2_PUBLIC_URL}/${ds}/ducklake.duckdb.files/'
      );
"
  fi
done

echo "=== dbt run (${TARGET}) ==="
(cd transform && uv run dbt run --target "${TARGET}")

echo "=== カタログメタデータを生成 ==="
uv run python scripts/build_catalog.py

if [ "${TARGET}" = "prd" ]; then
  echo "=== メタデータをアップロード ==="
  for ds in "${DATASOURCES[@]}"; do
    npx wrangler r2 object put "${R2_BUCKET}/${ds}/ducklake.duckdb" \
      --file="transform/${ds}.ducklake" --remote

    npx wrangler r2 object put "${R2_BUCKET}/${ds}/catalog.json" \
      --file="transform/target/${ds}_catalog_meta.json" --remote
  done

  echo "=== デプロイ完了 ==="
  for ds in "${DATASOURCES[@]}"; do
    echo "  ${ds}: duckdb \"ducklake:${R2_PUBLIC_URL}/${ds}/ducklake.duckdb\""
  done
else
  echo "=== ビルド完了 ==="
fi
