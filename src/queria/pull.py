"""Pull: download ducklake.duckdb and metadata.json from S3."""

from pathlib import Path

from botocore.exceptions import ClientError

from queria import DIST_DIR, DUCKLAKE_FILE, METADATA_JSON
from queria.s3 import create_s3_client
from queria.config_schema import load_dataset_config


def _download_file(client, bucket: str, key: str, dest: Path) -> None:
    try:
        print(f"  {key}")
        client.download_file(bucket, key, str(dest))
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"  {key} not found in S3, skipping")
            return
        raise


def pull_from_s3(client, bucket: str, dataset_dir: Path, datasource: str) -> None:
    """Download ducklake.duckdb and metadata.json from S3."""
    dist_dir = dataset_dir / DIST_DIR
    dist_dir.mkdir(parents=True, exist_ok=True)

    _download_file(client, bucket, f"{datasource}/ducklake.duckdb", dist_dir / DUCKLAKE_FILE)
    _download_file(client, bucket, f"{datasource}/metadata.json", dist_dir / METADATA_JSON)


def pull_datasource(dataset_dir: Path, *, bucket: str) -> None:
    """Pull ducklake.duckdb from S3."""
    config = load_dataset_config(dataset_dir)
    datasource = config.name

    print(f"--- pull: {datasource} ---")

    client = create_s3_client()
    pull_from_s3(client, bucket, dataset_dir, datasource)
