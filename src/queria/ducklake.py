"""DuckLake initialization."""

from pathlib import Path

import duckdb

from queria import DIST_DIR, DUCKLAKE_FILE
from queria.config_schema import load_dataset_config


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
