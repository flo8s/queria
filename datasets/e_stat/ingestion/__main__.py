"""e-Stat API からデータを取得し dlt 経由で DuckLake に書き込む。"""

import argparse
import gc
import os
from pathlib import Path

import duckdb
import yaml

INGESTION_DIR = Path(__file__).parent
DATASET_DIR = INGESTION_DIR.parent
DIST_DIR = DATASET_DIR / "dist"
DUCKLAKE_SQLITE = DIST_DIR / "ducklake.sqlite"
DUCKLAKE_DUCKDB = DIST_DIR / "ducklake.duckdb"
DUCKLAKE_FILES_DIR = "ducklake.duckdb.files"
DATASOURCE = "e_stat"
SOURCE_SCHEMA = "_source"


def _load_data_path() -> str:
    with open(DATASET_DIR / "dataset.yml") as f:
        config = yaml.safe_load(f)
    return f"{config['ducklake_url']}.files/"


DATA_PATH = _load_data_path()


def _init_sqlite() -> None:
    """SQLite 形式の DuckLake カタログを初期化（dlt 用）。"""
    if DUCKLAKE_SQLITE.exists():
        return
    print(f"Creating DuckLake (SQLite): {DATASOURCE}")
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")
    conn.execute(f"""
        ATTACH 'ducklake:{DUCKLAKE_SQLITE}' AS {DATASOURCE} (
            DATA_PATH '{DATA_PATH}',
            META_TYPE 'sqlite'
        )
    """)
    conn.close()


def _convert_to_duckdb() -> None:
    """SQLite カタログを DuckDB 形式に変換して ducklake.duckdb を上書き。

    dlt は META_TYPE 'sqlite' でしか書き込めないが、
    duckdb-wasm は DuckDB 形式カタログでしか HTTP ATTACH できない。
    さらに DuckDB 形式には NOT NULL 制約があり、これがないと
    duckdb-wasm がデータを 0 行として返す。

    そのため以下の手順で変換する:
    1. DuckDB 形式で空の DuckLake を作成（NOT NULL 制約付きスキーマを得る）
    2. DuckLake ビューを切断し、両カタログを通常の DB として開く
    3. SQLite のメタデータを DuckDB にコピー（全量入れ替え）
    """
    if not DUCKLAKE_SQLITE.exists():
        return
    print("Converting DuckLake: SQLite -> DuckDB")
    SRC = "src"
    DST = "dst"
    tmp_file = DUCKLAKE_DUCKDB.with_suffix(".duckdb.tmp")
    try:
        conn = duckdb.connect(":memory:")
        conn.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")

        # DuckDB 形式の空カタログを DuckLake として作成
        # → ducklake_table, ducklake_column 等の内部テーブルが NOT NULL 制約付きで生成される
        conn.execute(f"""
            ATTACH 'ducklake:{tmp_file}' AS {DST} (DATA_PATH '{DATA_PATH}')
        """)

        # DuckLake ビューを切断し、以降は通常の DB として内部テーブルを直接操作する
        conn.execute(f"DETACH {DST}")
        conn.execute(f"ATTACH '{DUCKLAKE_SQLITE}' AS {SRC} (TYPE sqlite)")
        conn.execute(f"ATTACH '{tmp_file}' AS {DST}")

        # DuckLake の内部メタデータテーブル (ducklake_table, ducklake_column 等) を列挙し、
        # SQLite 側の内容を DuckDB 側にコピーする
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_catalog='{SRC}'"
        ).fetchall()
        for (table_name,) in tables:
            # 空カタログ作成時の初期行をクリアし、SQLite の全メタデータで置き換え
            conn.execute(f"DELETE FROM {DST}.main.{table_name}")
            conn.execute(
                f"INSERT INTO {DST}.main.{table_name} "
                f"SELECT * FROM {SRC}.main.{table_name}"
            )

        conn.execute(f"CHECKPOINT {DST}")
        conn.close()

        # 変換成功: ducklake.duckdb を置き換え
        if DUCKLAKE_DUCKDB.exists():
            DUCKLAKE_DUCKDB.unlink()
        tmp_file.rename(DUCKLAKE_DUCKDB)
    finally:
        # 異常終了時に tmp ファイルを残さない
        for f in [tmp_file, tmp_file.with_suffix(".duckdb.tmp.wal")]:
            if f.exists():
                f.unlink()


def _parse_s3_url(url: str) -> tuple[str, str]:
    """s3://bucket/prefix/ → (bucket, prefix)."""
    path = url.removeprefix("s3://")
    bucket, _, prefix = path.partition("/")
    return bucket, prefix


def _create_s3_client():
    """S3 クライアントを作成。"""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['S3_ENDPOINT']}",
        aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def _fetch_sqlite(base_path: str) -> None:
    """base_path から ducklake.sqlite を取得。ローカルなら何もしない。"""
    if not base_path.startswith("s3://"):
        return
    from botocore.exceptions import ClientError

    bucket, prefix = _parse_s3_url(base_path)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _create_s3_client().download_file(
            bucket, f"{prefix}ducklake.sqlite", str(DUCKLAKE_SQLITE)
        )
        print("  Fetched ducklake.sqlite from S3")
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("  ducklake.sqlite not found in S3, starting fresh")
        else:
            raise


def _freeze_sqlite(base_path: str) -> None:
    """ducklake.sqlite を base_path に保存。ローカルなら何もしない。"""
    if not DUCKLAKE_SQLITE.exists():
        return
    if not base_path.startswith("s3://"):
        return
    bucket, prefix = _parse_s3_url(base_path)
    _create_s3_client().upload_file(
        str(DUCKLAKE_SQLITE),
        bucket,
        f"{prefix}ducklake.sqlite",
        ExtraArgs={"CacheControl": "no-cache"},
    )
    print("  Froze ducklake.sqlite to S3")


def _load_single_table(
    name: str,
    stats_id: str,
    merge_keys: list[str],
    app_id: str,
    data_path: str,
) -> None:
    """1 テーブル分のデータを取得して DuckLake に書き込む。"""
    import dlt
    import ducklake_patch
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
    from estat_api_dlt_helper import (
        DestinationConfig,
        EstatDltConfig,
        SourceConfig,
        create_estat_pipeline,
        create_estat_resource,
    )

    ducklake_patch.apply()

    if data_path.startswith("s3://"):
        from dlt.common.configuration.specs.aws_credentials import AwsCredentials
        from dlt.common.storages.configuration import FilesystemConfiguration

        storage = FilesystemConfiguration(
            bucket_url=data_path,
            credentials=AwsCredentials(
                aws_access_key_id=os.environ["S3_ACCESS_KEY_ID"],
                aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
                endpoint_url=os.environ.get("S3_ENDPOINT"),
                region_name="auto",
            ),
        )
    else:
        storage = data_path

    destination = dlt.destinations.ducklake(
        credentials=DuckLakeCredentials(
            ducklake_name=DATASOURCE,
            catalog=f"duckdb:///{DUCKLAKE_SQLITE}",
            storage=storage,
        )
    )

    estat_config = EstatDltConfig(
        source=SourceConfig(app_id=app_id, statsDataId=stats_id),
        destination=DestinationConfig(
            destination=destination,
            dataset_name=SOURCE_SCHEMA,
            table_name=name,
            write_disposition="merge" if merge_keys else "replace",
            primary_key=merge_keys or None,
            pipeline_name=f"estat_{name}",
        ),
    )

    resource = create_estat_resource(estat_config)
    pipeline = create_estat_pipeline(estat_config)
    info = pipeline.run(resource)
    print(f"    -> {info}")


def _table_exists(table_name: str) -> bool:
    """DuckLake カタログにテーブルが存在するか確認。"""
    if not DUCKLAKE_SQLITE.exists():
        return False
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
        conn.execute(f"""
            ATTACH 'ducklake:{DUCKLAKE_SQLITE}' AS {DATASOURCE} (
                DATA_PATH '{DATA_PATH}', META_TYPE 'sqlite'
            )
        """)
        result = conn.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_catalog=? AND table_schema=? AND table_name=?",
            [DATASOURCE, SOURCE_SCHEMA, table_name],
        ).fetchone()
        return result is not None
    except Exception:
        return False
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="全データを再取得（洗い替え）")
    args = parser.parse_args()

    app_id = os.environ["ESTAT_API_KEY"]
    s3_bucket = os.environ.get("S3_BUCKET")
    base_path = f"s3://{s3_bucket}/{DATASOURCE}/" if s3_bucket else str(DIST_DIR) + "/"
    storage_path = f"{base_path}{DUCKLAKE_FILES_DIR}/"

    _fetch_sqlite(base_path)
    _init_sqlite()
    os.environ["DLT_DUCKLAKE_OVERRIDE_DATA_PATH"] = "true"

    with open(INGESTION_DIR / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    loaded = 0
    for table_def in tables_config["tables"]:
        name = table_def["name"]
        stats_id = table_def["statsDataId"]
        merge_keys = table_def.get("merge_keys", [])

        if not args.refresh and _table_exists(name):
            print(f"  Skipping {name} ({stats_id}, already loaded)")
            continue

        print(f"  Loading {name} (statsDataId: {stats_id})")
        _load_single_table(name, stats_id, merge_keys, app_id, storage_path)
        loaded += 1
        _freeze_sqlite(base_path)
        gc.collect()

    if loaded > 0:
        _convert_to_duckdb()
    else:
        print("  No tables loaded, skipping DuckDB conversion")


main()
