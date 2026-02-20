#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="$REPO_DIR/.dev-serve"

echo "=== Building datasets ==="

for ds in "$REPO_DIR"/datasets/*/; do
  [ ! -f "$ds/dataset.yml" ] && continue
  [ "$(basename "$ds")" = "catalog" ] && continue

  echo ""
  echo "--- $(basename "$ds") ---"
  uv run queria run "$ds" --target dev
  uv run queria freeze "$ds" --output-dir "$OUTPUT_DIR"
done

echo ""
echo "=== Building catalog ==="
uv run python "$REPO_DIR/scripts/generate_catalog_sources.py"
uv run queria run "$REPO_DIR/datasets/catalog" --target dev \
  --vars "{\"storage_base_url\": \"$OUTPUT_DIR\"}"
uv run queria freeze "$REPO_DIR/datasets/catalog" --output-dir "$OUTPUT_DIR"

echo ""
echo "Done. Serve with:"
echo "  npx serve $OUTPUT_DIR --cors -l 4000"
