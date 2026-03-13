#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-dev}"

if [ -f "$REPO_DIR/.env" ]; then
  set -a
  source "$REPO_DIR/.env"
  set +a
fi

case "$MODE" in
  dev)
    PULL_PUSH_DEST="$REPO_DIR/.dev-serve"
    ;;
  stg|prd)
    PULL_PUSH_DEST="s3://$S3_BUCKET"
    ;;
  *)
    echo "Usage: $0 [dev|stg|prd]" >&2
    exit 1
    ;;
esac

echo "=== Building datasets ($MODE) ==="

failed=()
for ds in "$REPO_DIR"/datasets/*/; do
  [ ! -f "$ds/dataset.yml" ] && continue
  [ "$(basename "$ds")" = "catalog" ] && continue

  name="$(basename "$ds")"
  echo ""
  echo "--- $name ---"
  # stg/prd: DUCKLAKE_STORAGE をストレージベースパスに設定
  if [[ "$MODE" == "stg" || "$MODE" == "prd" ]]; then
    export DUCKLAKE_STORAGE="s3://$S3_BUCKET/$name"
  else
    unset DUCKLAKE_STORAGE
  fi
  if (
    cd "$ds" && uv sync --quiet && \
    uv run fdl pull "$PULL_PUSH_DEST" && \
    uv run python pipeline.py && \
    uv run fdl metadata && \
    uv run fdl push "$PULL_PUSH_DEST"
  ); then
    :
  else
    echo "WARNING: $name failed, skipping"
    failed+=("$name")
  fi
done

echo ""
echo "=== Building catalog ==="
CATALOG_ENV=()
[[ "$MODE" == "dev" ]] && CATALOG_ENV=(STORAGE_BASE_URL="$PULL_PUSH_DEST")
# stg/prd: DUCKLAKE_STORAGE をストレージベースパスに設定
if [[ "$MODE" == "stg" || "$MODE" == "prd" ]]; then
  export DUCKLAKE_STORAGE="s3://$S3_BUCKET/catalog"
else
  unset DUCKLAKE_STORAGE
fi
(
  cd "$REPO_DIR/datasets/catalog" &&
  uv sync --quiet && \
  uv run fdl pull "$PULL_PUSH_DEST" && \
  env "${CATALOG_ENV[@]}" uv run python pipeline.py && \
  uv run fdl metadata && \
  uv run fdl push "$PULL_PUSH_DEST"
)

if [ ${#failed[@]} -gt 0 ]; then
  echo ""
  echo "WARNING: ${#failed[@]} dataset(s) failed: ${failed[*]}"
fi

echo ""
echo "Done."
