#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Required env vars check
for var in S3_BUCKET S3_ENDPOINT S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY; do
  if [ -z "${!var:-}" ]; then
    echo "Error: $var is not set" >&2
    exit 1
  fi
done

# Clean build artifacts
echo "=== Cleaning dist/ ==="
rm -rf "$REPO_DIR"/datasets/*/dist

# Collect non-catalog datasets
datasets=()
for ds in "$REPO_DIR"/datasets/*/; do
  name="$(basename "$ds")"
  # dataset.yml を持たないディレクトリはデータセットではないのでスキップ
  [ ! -f "$ds/dataset.yml" ] && continue
  # TODO: e_stat は dwh 依存先の Cloudflare 問題が解決するまでスキップ (CI のみ)
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    # CI では、catalog と e_stat は R2 にアップロードされたメタデータを参照するためスキップ
    [ "$name" = "e_stat" ] && continue
  fi
  datasets+=("$ds")
done

# Fetch
echo "=== Fetching datasets ==="
for ds in "${datasets[@]}"; do
  echo "--- $(basename "$ds") ---"
  uv run queria fetch "$ds"
done

# Build and freeze non-catalog datasets
echo "=== Building datasets ==="
for ds in "${datasets[@]}"; do

  echo "--- $(basename "$ds") ---"
  uv run queria run "$ds" --target prd
  uv run queria freeze "$ds"
done

# Build and freeze catalog (depends on other datasets' metadata on R2)
echo "=== Building catalog ==="
uv run python "$REPO_DIR/datasets/catalog/generate_sources.py"
uv run queria fetch "$REPO_DIR/datasets/catalog"
uv run queria run "$REPO_DIR/datasets/catalog" --target prd
uv run queria freeze "$REPO_DIR/datasets/catalog"

echo "Done."
