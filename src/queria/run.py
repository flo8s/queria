"""Build pipeline: DuckLake initialization + dbt execution + metadata generation."""

import json
import os
import shutil
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

import duckdb
import yaml
from dbt.artifacts.resources.v1.model import Model
from dbt.artifacts.schemas.catalog import CatalogArtifact
from dbt.artifacts.schemas.manifest import WritableManifest
from dbt.cli.main import dbtRunner

from queria import DATASET_YML, DIST_DIR, DUCKLAKE_FILE, METADATA_JSON, TRANSFORM_DIR
from queria.config_schema import DatasetConfig
from queria.metadata_schema import (
    ColumnInfo,
    DatasetMetadata,
    LineageInfo,
    ModelInfo,
    NodeConfig,
    NodeInfo,
    SchemaInfo,
)


@contextmanager
def working_directory(path: Path):
    """Context manager to temporarily change the current working directory.

    dbt must run with transform/ as the working directory because
    profiles.yml uses relative paths (ducklake:ducklake.duckdb).
    """
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def load_dataset_config(dataset_dir: Path) -> DatasetConfig:
    """Load and validate dataset.yml as a DatasetConfig."""
    path = dataset_dir / DATASET_YML
    if not path.exists():
        raise FileNotFoundError(f"{path} not found.")
    with open(path) as f:
        config = DatasetConfig.model_validate(yaml.safe_load(f))
    if not config.name:
        config.name = dataset_dir.name
    return config


def init_ducklake(dataset_dir: Path) -> None:
    """Initialize DuckLake only if the file does not exist yet."""
    dist_dir = dataset_dir / DIST_DIR
    ducklake_file = dist_dir / DUCKLAKE_FILE
    if ducklake_file.exists():
        return

    config = load_dataset_config(dataset_dir)
    datasource = config.name
    data_path = f"{config.ducklake_url}.files/"
    print(f"Creating DuckLake: {datasource} (DATA_PATH: {data_path})")

    dist_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        ATTACH '{ducklake_file}' AS {datasource} (
            TYPE ducklake,
            DATA_PATH '{data_path}'
        )
    """)
    conn.close()


def run_dbt(transform_dir: Path, args: list[str]) -> None:
    """Run a dbt command inside the transform/ directory."""
    with working_directory(transform_dir):
        result = dbtRunner().invoke(args)
        if not result.success:
            raise RuntimeError(f"dbt {' '.join(args)} failed")


def _is_datasource_model(node, datasource: str) -> bool:
    if not isinstance(node, Model):
        return False
    if not node.fqn or node.fqn[0] != datasource:
        return False
    return True


def _resolve_column_type(
    col_name: str,
    col_info_data_type: str | None,
    catalog_columns: dict,
) -> str:
    """catalog (DB introspection) > manifest (YAML-declared) > empty"""
    if col_name in catalog_columns:
        catalog_type = catalog_columns[col_name].type
        if catalog_type:
            return catalog_type
    return col_info_data_type or ""


def _build_columns(node: Model, catalog_columns: dict) -> list[ColumnInfo]:
    """manifest のカラム定義と catalog の型情報を統合して ColumnInfo リストを構築する。"""
    return [
        ColumnInfo(
            name=col_name,
            title=col_info.meta.get("title", ""),
            description=col_info.description,
            data_type=_resolve_column_type(col_name, col_info.data_type, catalog_columns),
            nullable=not any(c.type.value == "not_null" for c in col_info.constraints),
        )
        for col_name, col_info in node.columns.items()
    ]


def _build_model_info(node: Model, catalog_columns: dict) -> ModelInfo:
    """manifest ノードから公開用の ModelInfo を構築する。"""
    meta = node.meta
    return ModelInfo(
        name=node.name,
        title=meta.get("title", ""),
        description=node.description,
        tags=meta.get("tags", []),
        license=meta.get("license", ""),
        license_url=meta.get("license_url", ""),
        source_url=meta.get("source_url", ""),
        published=meta.get("published", False),
        columns=_build_columns(node, catalog_columns),
        materialized=node.config.materialized,
        sql=(node.compiled_code or "").strip() or None,
    )


def extract_models(
    manifest: WritableManifest, catalog: CatalogArtifact | None, datasource: str
) -> dict[str, list[ModelInfo]]:
    """Extract models for the given datasource from manifest.json and group by schema."""
    tables_by_schema: dict[str, list[ModelInfo]] = defaultdict(list)
    for node_id, node in manifest.nodes.items():
        if not _is_datasource_model(node, datasource):
            continue
        catalog_node = catalog.nodes.get(node_id) if catalog else None
        catalog_columns = catalog_node.columns if catalog_node else {}
        tables_by_schema[node.schema].append(
            _build_model_info(node, catalog_columns)
        )
    return dict(tables_by_schema)


def extract_lineage(manifest: WritableManifest, datasource: str) -> LineageInfo:
    """Extract lineage (DAG) information from manifest.json."""
    prefix = f"model.{datasource}."
    parent_map_raw = manifest.parent_map or {}

    parent_map: dict[str, list[str]] = {}
    node_keys: set[str] = set()

    for full_key, parents in parent_map_raw.items():
        if not full_key.startswith(prefix):
            continue
        short_key = full_key[len(prefix) :]
        short_parents = [p[len(prefix) :] for p in parents if p.startswith(prefix)]
        parent_map[short_key] = short_parents
        node_keys.add(short_key)
        node_keys.update(short_parents)

    nodes: dict[str, NodeInfo] = {}
    for key in node_keys:
        full_key = prefix + key
        node = manifest.nodes.get(full_key)
        if node:
            nodes[key] = NodeInfo(
                fqn=node.fqn,
                resource_type=node.resource_type,
                config=NodeConfig(materialized=node.config.materialized),
                meta=node.meta,
            )
        else:
            nodes[key] = NodeInfo(
                fqn=[],
                resource_type="model",
                config=NodeConfig(materialized="view"),
                meta={},
            )

    return LineageInfo(parent_map=parent_map, nodes=nodes)


def build_metadata(
    dataset_config: DatasetConfig,
    manifest: WritableManifest,
    catalog: CatalogArtifact | None,
    datasource: str,
) -> DatasetMetadata:
    """Pure function: DatasetConfig → DatasetMetadata"""
    tables_by_schema = extract_models(manifest, catalog, datasource)
    lineage = extract_lineage(manifest, datasource)

    schemas: dict[str, SchemaInfo] = {}
    for name, info in dataset_config.schemas.items():
        schemas[name] = SchemaInfo(
            title=info.title,
            tables=tables_by_schema.get(name, []),
        )
    for name, tables in tables_by_schema.items():
        if name not in schemas:
            schemas[name] = SchemaInfo(title="", tables=tables)

    return DatasetMetadata(
        title=dataset_config.title,
        description=dataset_config.description,
        cover=dataset_config.cover,
        tags=dataset_config.tags,
        ducklake_url=dataset_config.ducklake_url,
        schemas=schemas,
        lineage=lineage,
        dependencies=dataset_config.dependencies,
    )


def load_manifest(dataset_dir: Path) -> WritableManifest:
    """Load dbt manifest.json from the target directory."""
    path = dataset_dir / TRANSFORM_DIR / "target" / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Run dbt run first.")
    return WritableManifest.read_and_check_versions(str(path))


def load_catalog(dataset_dir: Path) -> CatalogArtifact | None:
    """Load dbt catalog.json if it exists."""
    path = dataset_dir / TRANSFORM_DIR / "target" / "catalog.json"
    if not path.exists():
        return None
    return CatalogArtifact.read_and_check_versions(str(path))


def generate_metadata(dataset_dir: Path) -> None:
    """I/O wrapper: ファイル読み書き + ログ出力"""
    dataset_config = load_dataset_config(dataset_dir)
    datasource = dataset_config.name
    manifest = load_manifest(dataset_dir)
    catalog = load_catalog(dataset_dir)

    output = build_metadata(dataset_config, manifest, catalog, datasource)

    dist_dir = dataset_dir / DIST_DIR
    dist_dir.mkdir(parents=True, exist_ok=True)
    output_path = dist_dir / METADATA_JSON
    with open(output_path, "w") as f:
        json.dump(output.model_dump(exclude_none=True), f, ensure_ascii=False, indent=2)

    total_tables = sum(len(s.tables) for s in output.schemas.values())
    print(f"Generated metadata: {output_path}")
    print(f"  Datasource: {datasource} / Public tables: {total_tables}")


def _copy_docs_to_dist(dataset_dir: Path) -> None:
    """Copy dbt docs files from transform/target/ to dist/docs/."""
    target_dir = dataset_dir / TRANSFORM_DIR / "target"
    docs_dir = dataset_dir / DIST_DIR / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    for name in ("index.html", "manifest.json", "catalog.json"):
        src = target_dir / name
        if src.exists():
            shutil.copy2(src, docs_dir / name)


def build_datasource(dataset_dir: Path, target: str, *, dbt_vars: str | None = None) -> None:
    """Build pipeline for a single datasource.

    init_ducklake -> dbt deps -> dbt run -> dbt docs generate -> generate_metadata
    """
    datasource = load_dataset_config(dataset_dir).name
    transform_dir = dataset_dir / TRANSFORM_DIR

    init_ducklake(dataset_dir)

    extra_args = ["--vars", dbt_vars] if dbt_vars else []

    print(f"--- dbt deps + run ({datasource}, {target}) ---")
    run_dbt(transform_dir, ["deps"])
    run_dbt(transform_dir, ["run", "--target", target, *extra_args])

    print(f"--- dbt docs generate ({datasource}) ---")
    run_dbt(transform_dir, ["docs", "generate", "--target", target, *extra_args])

    print(f"--- Copying docs to dist/ ({datasource}) ---")
    _copy_docs_to_dist(dataset_dir)

    print(f"--- Generating metadata ({datasource}) ---")
    generate_metadata(dataset_dir)
