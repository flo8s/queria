#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="$REPO_DIR/.dev-serve"

if [ -f "$REPO_DIR/.env" ]; then
  set -a
  source "$REPO_DIR/.env"
  set +a
fi
unset S3_BUCKET  # dev ビルドはローカルストレージを使用

echo "=== Building datasets ==="

failed=()
for ds in "$REPO_DIR"/datasets/*/; do
  [ ! -f "$ds/dataset.yml" ] && continue
  [ "$(basename "$ds")" = "catalog" ] && continue

  name="$(basename "$ds")"
  echo ""
  echo "--- $name ---"
  if uv run queria run "$ds" --target dev && \
     uv run queria freeze "$ds" --output-dir "$OUTPUT_DIR"; then
    :
  else
    echo "WARNING: $name failed, skipping"
    failed+=("$name")
  fi
done

echo ""
echo "=== Building catalog ==="
uv run python "$REPO_DIR/datasets/catalog/generate_sources.py"
uv run queria run "$REPO_DIR/datasets/catalog" --target dev \
  --vars "{\"storage_base_url\": \"$OUTPUT_DIR\"}"
uv run queria freeze "$REPO_DIR/datasets/catalog" --output-dir "$OUTPUT_DIR"

if [ ${#failed[@]} -gt 0 ]; then
  echo ""
  echo "WARNING: ${#failed[@]} dataset(s) failed: ${failed[*]}"
fi

echo ""
echo "Done. Serve with:"
echo "  ./scripts/dev-serve.sh"
