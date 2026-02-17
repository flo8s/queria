"""manifest.json と _catalog.yml を統合して catalog_meta.json を生成する。

datasets/ 配下の _catalog.yml を自動検出し、データソースごとに
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
DATASETS_DIR = ROOT / "datasets"


def load_json(path: Path) -> dict:
    if not path.exists():
        print(f"Error: {path} が見つかりません。先に dbt run を実行してください。", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        return json.load(f)


def discover_catalog_ymls() -> dict[str, tuple[Path, dict]]:
    """datasets/ 配下の _catalog.yml を検出し、データソース名とともに返す。

    データソース名は _catalog.yml の親ディレクトリ名から取得する。
    """
    result = {}
    for yml_path in sorted(DATASETS_DIR.glob("*/_catalog.yml")):
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

        # fqn でデータソースをフィルタリング (例: ["tsukuba", "mart", "model_name"])
        fqn = node.get("fqn", [])
        if len(fqn) < 1 or fqn[0] != datasource:
            continue

        meta = node.get("meta", {})
        if not meta.get("public", False):
            continue

        materialized = node.get("config", {}).get("materialized", "table")
        model_type = "view" if materialized == "view" else "table"
        compiled_code = node.get("compiled_code", "").strip()

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
            "type": model_type,
        }
        license_url = meta.get("license_url", "")
        if license_url:
            model["license_url"] = license_url
        if compiled_code:
            model["sql"] = compiled_code

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
                "type": model["type"],
            }
            if "license_url" in model:
                table["license_url"] = model["license_url"]
            if "sql" in model:
                table["sql"] = model["sql"]
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
    catalog_ymls = discover_catalog_ymls()

    if not catalog_ymls:
        print("Error: _catalog.yml が見つかりません。", file=sys.stderr)
        sys.exit(1)

    for datasource, (yml_path, catalog_yml) in sorted(catalog_ymls.items()):
        manifest_path = DATASETS_DIR / datasource / "transform" / "target" / "manifest.json"
        catalog_path = DATASETS_DIR / datasource / "transform" / "target" / "catalog.json"

        manifest = load_json(manifest_path)
        dbt_catalog = {}
        if catalog_path.exists():
            with open(catalog_path) as f:
                dbt_catalog = json.load(f)

        models = extract_models(manifest, dbt_catalog, datasource)
        catalog = build_catalog(catalog_yml, models)

        output_dir = DATASETS_DIR / datasource / "transform" / "target"
        output_dir.mkdir(parents=True, exist_ok=True)
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
