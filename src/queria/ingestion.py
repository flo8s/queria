"""Ingestion helpers: DuckLake SQLite lifecycle management."""

from pathlib import Path

import duckdb

from queria import DIST_DIR, DUCKLAKE_FILE, DUCKLAKE_SQLITE
from queria.config_schema import load_dataset_config
from queria.s3 import create_s3_client


def init_sqlite(dataset_dir: Path) -> None:
    """SQLite 形式の DuckLake カタログを初期化（既に存在すればスキップ）。"""
    dist_dir = dataset_dir / DIST_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    if sqlite_file.exists():
        return

    config = load_dataset_config(dataset_dir)
    datasource = config.name
    data_path = f"{config.ducklake_url}.files/"
    print(f"Creating DuckLake (SQLite): {datasource} (DATA_PATH: {data_path})")

    dist_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        ATTACH 'ducklake:{sqlite_file}' AS {datasource} (
            DATA_PATH '{data_path}',
            META_TYPE 'sqlite'
        )
    """)
    conn.close()


def convert_to_duckdb(dataset_dir: Path) -> None:
    """SQLite カタログを DuckDB 形式に変換して ducklake.duckdb を上書き。"""
    dist_dir = dataset_dir / DIST_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    duckdb_file = dist_dir / DUCKLAKE_FILE
    if not sqlite_file.exists():
        return

    config = load_dataset_config(dataset_dir)
    data_path = f"{config.ducklake_url}.files/"

    print("Converting DuckLake: SQLite -> DuckDB")
    SRC = "src"
    DST = "dst"
    tmp_file = duckdb_file.with_suffix(".duckdb.tmp")
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")

        conn.execute(f"""
            ATTACH 'ducklake:{tmp_file}' AS {DST} (DATA_PATH '{data_path}')
        """)

        conn.execute(f"DETACH {DST}")
        conn.execute(f"ATTACH '{sqlite_file}' AS {SRC} (TYPE sqlite)")
        conn.execute(f"ATTACH '{tmp_file}' AS {DST}")

        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog='{SRC}'"
        ).fetchall()
        for (table_name,) in tables:
            conn.execute(f"DELETE FROM {DST}.main.{table_name}")
            conn.execute(
                f"INSERT INTO {DST}.main.{table_name} "
                f"SELECT * FROM {SRC}.main.{table_name}"
            )

        conn.execute(f"CHECKPOINT {DST}")
        conn.close()

        if duckdb_file.exists():
            duckdb_file.unlink()
        tmp_file.rename(duckdb_file)
    finally:
        for f in [tmp_file, tmp_file.with_suffix(".duckdb.tmp.wal")]:
            if f.exists():
                f.unlink()


def fetch_sqlite(dataset_dir: Path, bucket: str) -> None:
    """S3 から ducklake.sqlite をダウンロード（なければ新規開始）。"""
    from botocore.exceptions import ClientError

    config = load_dataset_config(dataset_dir)
    datasource = config.name

    dist_dir = dataset_dir / DIST_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE

    dist_dir.mkdir(parents=True, exist_ok=True)
    try:
        create_s3_client().download_file(
            bucket, f"{datasource}/{DUCKLAKE_SQLITE}", str(sqlite_file)
        )
        print("Fetched ducklake.sqlite from S3")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("ducklake.sqlite not found in S3, starting fresh")
        else:
            raise


def freeze_sqlite(dataset_dir: Path, bucket: str) -> None:
    """ducklake.sqlite を S3 にアップロード。"""
    config = load_dataset_config(dataset_dir)
    datasource = config.name

    dist_dir = dataset_dir / DIST_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE

    if not sqlite_file.exists():
        return

    create_s3_client().upload_file(
        str(sqlite_file),
        bucket,
        f"{datasource}/{DUCKLAKE_SQLITE}",
        ExtraArgs={"CacheControl": "no-cache"},
    )
    print("Froze ducklake.sqlite to S3")
