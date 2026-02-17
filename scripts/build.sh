#!/bin/bash
# DuckLake ビルドスクリプト
#
# 使い方:
#   ./scripts/build.sh                       # 全データソースをローカルビルド (dev)
#   ./scripts/build.sh tsukuba               # 指定データソースのみビルド
#   ./scripts/build.sh --target prd          # 全データソースを prd ビルド
#   ./scripts/build.sh tsukuba --target prd  # 指定データソースのみ prd ビルド
#
# prd の場合、以下の環境変数が必要 (.env で設定):
#   R2_ACCESS_KEY_ID       - Cloudflare R2 アクセスキー
#   R2_SECRET_ACCESS_KEY   - Cloudflare R2 シークレットキー
#   CLOUDFLARE_ACCOUNT_ID  - Cloudflare アカウントID
#   R2_S3_BUCKET_PATH      - S3バケットパス (例: s3://queria-dev)
set -e
cd "$(dirname "$0")/.."

# 引数をパース (第1引数: データソース名, --target: ビルドターゲット)
TARGET="dev"
DATASOURCE_FILTER=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target) TARGET="$2"; shift 2 ;;
        -*) echo "不明なオプション: $1" >&2; exit 1 ;;
        *) DATASOURCE_FILTER="$1"; shift ;;
    esac
done

# datasets/ 配下の _catalog.yml を持つディレクトリをデータソースとして検出
ALL_DATASOURCES=()
for catalog_yml in datasets/*/_catalog.yml; do
  ALL_DATASOURCES+=("$(basename "$(dirname "${catalog_yml}")")")
done
if [ ${#ALL_DATASOURCES[@]} -eq 0 ]; then
  echo "Error: データソースが見つかりません (datasets/*/_catalog.yml)" >&2
  exit 1
fi

# データソースのフィルタリング
if [ -n "${DATASOURCE_FILTER}" ]; then
  found=false
  for ds in "${ALL_DATASOURCES[@]}"; do
    if [ "${ds}" = "${DATASOURCE_FILTER}" ]; then
      found=true
      break
    fi
  done
  if ! $found; then
    echo "Error: データソース '${DATASOURCE_FILTER}' が見つかりません" >&2
    echo "利用可能: ${ALL_DATASOURCES[*]}" >&2
    exit 1
  fi
  DATASOURCES=("${DATASOURCE_FILTER}")
else
  DATASOURCES=("${ALL_DATASOURCES[@]}")
fi

R2_BUCKET="${R2_BUCKET:-queria-dev}"
R2_PUBLIC_URL="${R2_PUBLIC_URL:-https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev}"

echo "=== DuckLake 初期化チェック ==="
for ds in "${DATASOURCES[@]}"; do
  DUCKLAKE_FILE="datasets/${ds}/transform/${ds}.ducklake"
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
for ds in "${DATASOURCES[@]}"; do
  echo "--- ${ds} ---"
  (cd "datasets/${ds}/transform" && uv run dbt deps && uv run dbt run --target "${TARGET}")
done

echo "=== dbt docs generate ==="
for ds in "${DATASOURCES[@]}"; do
  echo "--- ${ds} ---"
  (cd "datasets/${ds}/transform" && uv run dbt docs generate --target "${TARGET}")
done

echo "=== カタログメタデータを生成 ==="
uv run python scripts/build_catalog.py

if [ "${TARGET}" = "prd" ]; then
  echo "=== メタデータをアップロード ==="
  for ds in "${DATASOURCES[@]}"; do
    npx wrangler r2 object put "${R2_BUCKET}/${ds}/ducklake.duckdb" \
      --file="datasets/${ds}/transform/${ds}.ducklake" --remote

    npx wrangler r2 object put "${R2_BUCKET}/${ds}/catalog.json" \
      --file="datasets/${ds}/transform/target/${ds}_catalog_meta.json" \
      --content-type "application/json; charset=utf-8" --remote
  done

  echo "=== デプロイ完了 ==="
  for ds in "${DATASOURCES[@]}"; do
    echo "  ${ds}: duckdb \"ducklake:${R2_PUBLIC_URL}/${ds}/ducklake.duckdb\""
  done
else
  echo "=== ビルド完了 ==="
fi
