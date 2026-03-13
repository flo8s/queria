from pathlib import Path

DATASET_YML = "dataset.yml"
METADATA_JSON = "metadata.json"
TRANSFORM_DIR = "transform"
DIST_DIR = Path("dist")
DUCKLAKE_FILE = "ducklake.duckdb"
DUCKLAKE_SQLITE = "ducklake.sqlite"


def ducklake_data_path(catalog_url: str) -> str:
    """Derive DuckLake DATA_PATH from a catalog URL or path."""
    return f"{catalog_url}.files/"
