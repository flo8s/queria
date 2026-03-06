"""Build pipeline: DuckLake initialization + dbt execution + metadata generation."""

from pathlib import Path

from queria import TRANSFORM_DIR
from queria.config_schema import load_dataset_config
from queria.dbt import run_dbt
from queria.ducklake import init_ducklake
from queria.metadata import _copy_docs_to_dist, generate_metadata


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
