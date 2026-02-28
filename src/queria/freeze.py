"""Freeze: S3 upload / local copy."""

import shutil
from pathlib import Path

from queria import DIST_DIR, DUCKLAKE_FILE, METADATA_JSON
from queria.config_schema import load_dataset_config
from queria.s3 import create_s3_client


def _upload(
    client,
    bucket: str,
    key: str,
    file_path: Path,
    content_type: str | None = None,
    cache_control: str | None = None,
) -> None:
    """Upload a single file to S3."""
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if cache_control:
        extra_args["CacheControl"] = cache_control

    print(f"  {key}")
    client.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args or None)


def freeze_to_s3(client, bucket: str, dataset_dir: Path, datasource: str) -> None:
    """Upload artifacts to S3."""
    dist_dir = dataset_dir / DIST_DIR

    _upload(
        client, bucket,
        f"{datasource}/ducklake.duckdb",
        dist_dir / DUCKLAKE_FILE,
        cache_control="no-cache",
    )

    _upload(
        client, bucket,
        f"{datasource}/{METADATA_JSON}",
        dist_dir / METADATA_JSON,
        content_type="application/json; charset=utf-8",
    )

    docs_dir = dist_dir / "docs"
    _upload(
        client, bucket,
        f"{datasource}/docs/index.html",
        docs_dir / "index.html",
        content_type="text/html; charset=utf-8",
    )
    _upload(
        client, bucket,
        f"{datasource}/docs/manifest.json",
        docs_dir / "manifest.json",
        content_type="application/json; charset=utf-8",
    )
    _upload(
        client, bucket,
        f"{datasource}/docs/catalog.json",
        docs_dir / "catalog.json",
        content_type="application/json; charset=utf-8",
    )


def freeze_to_local(output_dir: Path, dataset_dir: Path, datasource: str) -> None:
    """Copy artifacts to a local directory."""
    dist_dir = dataset_dir / DIST_DIR
    dest = output_dir / datasource
    dest.mkdir(parents=True, exist_ok=True)

    shutil.copy2(dist_dir / DUCKLAKE_FILE, dest / DUCKLAKE_FILE)

    # Data files (ducklake.duckdb.files/)
    ducklake_data_dir = f"{DUCKLAKE_FILE}.files"
    data_dir = dist_dir / ducklake_data_dir
    if data_dir.exists():
        dest_data = dest / ducklake_data_dir
        if dest_data.exists():
            shutil.rmtree(dest_data)
        shutil.copytree(data_dir, dest_data)

    shutil.copy2(dist_dir / METADATA_JSON, dest / METADATA_JSON)

    docs_dir = dest / "docs"
    docs_dir.mkdir(exist_ok=True)
    src_docs_dir = dist_dir / "docs"
    for name in ("index.html", "manifest.json", "catalog.json"):
        src = src_docs_dir / name
        if src.exists():
            shutil.copy2(src, docs_dir / name)


def freeze_datasource(
    dataset_dir: Path,
    *,
    bucket: str | None = None,
    output_dir: Path | None = None,
) -> None:
    """Freeze datasource artifacts."""
    datasource = load_dataset_config(dataset_dir).name

    print(f"--- freeze: {datasource} ---")

    if bucket:
        client = create_s3_client()
        freeze_to_s3(client, bucket, dataset_dir, datasource)

    if output_dir:
        freeze_to_local(output_dir, dataset_dir, datasource)
