"""manifest.json と _catalog.yml を統合して catalog_meta.json を生成する。

models/ 配下の _catalog.yml を自動検出し、データソースごとに
{datasource}_catalog_meta.json を出力する。

使い方:
    cd /path/to/queria
    uv run dbt run           # manifest.json を生成
    uv run python scripts/build_catalog.py
"""

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = ROOT / "transform" / "target" / "manifest.json"
CATALOG_PATH = ROOT / "transform" / "target" / "catalog.json"
MODELS_DIR = ROOT / "transform" / "models"


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        print(f"Error: {MANIFEST_PATH} が見つかりません。先に dbt run を実行してください。", file=sys.stderr)
        sys.exit(1)
    with open(MANIFEST_PATH) as f:
        return json.load(f)


def load_dbt_catalog() -> dict:
    if not CATALOG_PATH.exists():
        return {}
    with open(CATALOG_PATH) as f:
        return json.load(f)


def discover_catalog_ymls() -> dict[str, tuple[Path, dict]]:
    """models/ 配下の _catalog.yml を検出し、データソース名とともに返す。

    データソース名は _catalog.yml の親ディレクトリ名から取得する。
    """
    result = {}
    for yml_path in MODELS_DIR.rglob("_catalog.yml"):
        datasource = yml_path.parent.name
        with open(yml_path) as f:
            data = yaml.safe_load(f)
        result[datasource] = (yml_path, data)
    return result


def extract_models(manifest: dict, dbt_catalog: dict, datasource: str) -> list[dict]:
    """manifest.json から指定データソースの public なモデルの情報を抽出する。"""
    models = []
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue

        # fqn でデータソースをフィルタリング (例: ["transform", "tsukuba", "mart", "model_name"])
        fqn = node.get("fqn", [])
        if len(fqn) < 2 or fqn[1] != datasource:
            continue

        meta = node.get("meta", {})
        if not meta.get("public", False):
            continue

        # dbt catalog からカラム型を取得
        catalog_node = dbt_catalog.get("nodes", {}).get(node_id, {})
        catalog_columns = catalog_node.get("columns", {})

        columns = []
        for col_name, col_info in node.get("columns", {}).items():
            cat_col = catalog_columns.get(col_name, {})
            columns.append({
                "name": col_name,
                "description": col_info.get("description", ""),
                "data_type": cat_col.get("type", ""),
            })

        model = {
            "name": node.get("name"),
            "description": node.get("description", ""),
            "schema": node.get("schema", "main"),
            "title": meta.get("title", ""),
            "tags": meta.get("tags", []),
            "license": meta.get("license", ""),
            "source_url": meta.get("source_url", ""),
            "columns": columns,
        }
        license_url = meta.get("license_url", "")
        if license_url:
            model["license_url"] = license_url

        models.append(model)
    return models


def build_catalog(catalog_yml: dict, models: list[dict]) -> dict:
    """_catalog.yml の構造にモデル情報をマージしてカタログを生成する。"""
    schemas = {}
    for schema_name, schema_info in catalog_yml.get("schemas", {}).items():
        schema_models = [m for m in models if m["schema"] == schema_name]
        tables = []
        for model in schema_models:
            table = {
                "name": model["name"],
                "title": model["title"],
                "description": model["description"],
                "tags": model["tags"],
                "license": model["license"],
                "source_url": model["source_url"],
                "columns": model["columns"],
            }
            if "license_url" in model:
                table["license_url"] = model["license_url"]
            tables.append(table)
        schemas[schema_name] = {
            "title": schema_info.get("title", ""),
            "tables": tables,
        }

    catalog = {
        "title": catalog_yml.get("title", ""),
        "description": catalog_yml.get("description", ""),
        "tags": catalog_yml.get("tags", []),
        "ducklake_url": catalog_yml.get("ducklake_url", ""),
        "schemas": schemas,
    }

    dependencies = catalog_yml.get("dependencies")
    if dependencies:
        catalog["dependencies"] = dependencies

    return catalog


def main() -> None:
    manifest = load_manifest()
    dbt_catalog = load_dbt_catalog()
    catalog_ymls = discover_catalog_ymls()

    if not catalog_ymls:
        print("Error: _catalog.yml が見つかりません。", file=sys.stderr)
        sys.exit(1)

    output_dir = MANIFEST_PATH.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    for datasource, (yml_path, catalog_yml) in sorted(catalog_ymls.items()):
        models = extract_models(manifest, dbt_catalog, datasource)
        catalog = build_catalog(catalog_yml, models)

        output_path = output_dir / f"{datasource}_catalog_meta.json"
        with open(output_path, "w") as f:
            json.dump(catalog, f, ensure_ascii=False, indent=2)

        total_tables = sum(
            len(s.get("tables", []))
            for s in catalog["schemas"].values()
        )
        print(f"カタログを生成しました: {output_path}")
        print(f"  データソース: {datasource} / 公開テーブル数: {total_tables}")


if __name__ == "__main__":
    main()
