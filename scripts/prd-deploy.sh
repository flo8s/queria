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
  if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
    :
  fi
  datasets+=("$ds")
done

# Build and push non-catalog datasets
echo "=== Building datasets ==="
for ds in "${datasets[@]}"; do
  echo "--- $(basename "$ds") ---"
  (
    cd "$ds" && uv sync --quiet && \
    uv run queria pull && \
    uv run queria run --target prd && \
    uv run queria push
  )
done

# Build and push catalog (depends on other datasets' metadata on R2)
echo "=== Building catalog ==="
uv run python "$REPO_DIR/datasets/catalog/generate_sources.py"
(
  cd "$REPO_DIR/datasets/catalog" &&
  uv sync --quiet && \
  uv run queria pull && \
  uv run queria run --target prd && \
  uv run queria push
)

echo "Done."
