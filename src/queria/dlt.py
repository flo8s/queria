"""dlt + DuckLake destination helpers."""

import os
from pathlib import Path

import duckdb

from queria import DIST_DIR, DUCKLAKE_FILE, DUCKLAKE_SQLITE
from queria.config_schema import DatasetConfig, load_dataset_config
from queria.ingestion import get_dataset_dir


def create_destination():
    """dlt 用の DuckLake destination を作成する。

    内部で SQLite カタログの初期化も行う。
    pipeline 実行後の DuckDB 変換は ingest_datasource が自動で行う。
    """
    dataset_dir = get_dataset_dir()
    config = load_dataset_config(dataset_dir)
    dist_dir = dataset_dir / DIST_DIR
    _init_sqlite(dataset_dir)
    return _create_ducklake_destination(dist_dir, config)


def _create_ducklake_destination(dist_dir: Path, config: DatasetConfig):
    """dlt 用の DuckLake destination を作成する。

    S3_BUCKET 環境変数の有無でローカル/S3 ストレージを自動判定。
    """
    import dlt
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

    datasource = config.name
    sqlite_file = (dist_dir / DUCKLAKE_SQLITE).resolve()
    s3_bucket = os.environ.get("S3_BUCKET")
    base_path = f"s3://{s3_bucket}/{datasource}/" if s3_bucket else str(dist_dir) + "/"
    storage_path = f"{base_path}{DUCKLAKE_FILE}.files/"

    if storage_path.startswith("s3://"):
        from dlt.common.configuration.specs.aws_credentials import AwsCredentials
        from dlt.common.storages.configuration import FilesystemConfiguration

        storage = FilesystemConfiguration(
            bucket_url=storage_path,
            credentials=AwsCredentials(
                aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                endpoint_url=os.environ.get("S3_ENDPOINT"),
                region_name="auto",
            ),
        )
    else:
        storage = storage_path

    return dlt.destinations.ducklake(
        credentials=DuckLakeCredentials(
            ducklake_name=datasource,
            catalog=f"duckdb:///{sqlite_file}",
            storage=storage,
        ),
        override_data_path=True,
    )


def _init_sqlite(dataset_dir: Path) -> None:
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


def _convert_to_duckdb(dataset_dir: Path) -> None:
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
