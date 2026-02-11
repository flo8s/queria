"""manifest.json と _catalog.yml を統合して catalog_meta.json を生成する。

使い方:
    cd /path/to/queria
    uv run dbt docs generate  # manifest.json を生成
    uv run python scripts/build_catalog.py
"""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = ROOT / "transform" / "target" / "manifest.json"
CATALOG_YML_PATH = ROOT / "transform" / "models" / "_catalog.yml"
OUTPUT_PATH = ROOT / "transform" / "target" / "catalog_meta.json"


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        print(f"Error: {MANIFEST_PATH} が見つかりません。先に dbt docs generate を実行してください。", file=sys.stderr)
        sys.exit(1)
    with open(MANIFEST_PATH) as f:
        return json.load(f)


def load_catalog_yml() -> dict:
    if not CATALOG_YML_PATH.exists():
        print(f"Error: {CATALOG_YML_PATH} が見つかりません。", file=sys.stderr)
        sys.exit(1)
    with open(CATALOG_YML_PATH) as f:
        return yaml.safe_load(f)


def extract_models(manifest: dict) -> list[dict]:
    """manifest.json から public なモデルの情報を抽出する。"""
    models = []
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        meta = node.get("meta", {})
        if not meta.get("public", False):
            continue

        columns = []
        for col_name, col_info in node.get("columns", {}).items():
            columns.append({
                "name": col_name,
                "description": col_info.get("description", ""),
            })

        models.append({
            "name": node.get("name"),
            "description": node.get("description", ""),
            "schema": node.get("schema", "main"),
            "meta": meta,
            "columns": columns,
        })
    return models


def build_catalog(catalog_yml: dict, models: list[dict]) -> dict:
    """_catalog.yml の構造にモデル情報をマージしてカタログを生成する。"""
    catalog = {"databases": {}}

    for db_name, db_info in catalog_yml.get("databases", {}).items():
        schemas = {}
        for schema_name, schema_info in db_info.get("schemas", {}).items():
            schema_models = [m for m in models if m["schema"] == schema_name]
            tables = []
            for model in schema_models:
                tables.append({
                    "name": model["name"],
                    "description": model["description"],
                    "meta": model["meta"],
                    "columns": model["columns"],
                })
            schemas[schema_name] = {
                **schema_info,
                "tables": tables,
            }

        catalog["databases"][db_name] = {
            "display_name": db_info.get("display_name", {}),
            "description": db_info.get("description", {}),
            "tags": db_info.get("tags", []),
            "schemas": schemas,
        }

    return catalog


def main() -> None:
    manifest = load_manifest()
    catalog_yml = load_catalog_yml()
    models = extract_models(manifest)
    catalog = build_catalog(catalog_yml, models)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    print(f"カタログを生成しました: {OUTPUT_PATH}")
    print(f"  データベース数: {len(catalog['databases'])}")
    total_tables = sum(
        len(s.get("tables", []))
        for db in catalog["databases"].values()
        for s in db["schemas"].values()
    )
    print(f"  公開テーブル数: {total_tables}")


if __name__ == "__main__":
    main()
