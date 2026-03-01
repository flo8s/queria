"""e-Stat API からデータを取得し DuckLake に直接書き込む。"""

import json
import os
import urllib.request
from pathlib import Path

import duckdb
import pyarrow as pa
import yaml
from estat_api_dlt_helper import parse_response

INGESTION_DIR = Path(__file__).parent
DIST_DIR = INGESTION_DIR.parent / "dist"
DUCKLAKE_FILE = DIST_DIR / "ducklake.duckdb"
DEFAULT_DATA_PATH = str(DIST_DIR / "ducklake.duckdb.files") + "/"
SOURCE_SCHEMA = "_source"


def fetch_pages(
    app_id: str, stats_data_id: str, limit: int = 100000
) -> list[pa.Table]:
    """e-Stat API からページネーションしながら Arrow Tables を取得する。"""
    pages: list[pa.Table] = []
    offset = 1
    while True:
        url = (
            f"https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
            f"?appId={app_id}&statsDataId={stats_data_id}"
            f"&limit={limit}&startPosition={offset}"
        )
        with urllib.request.urlopen(url) as resp:
            data = json.loads(resp.read())
        table = parse_response(data)
        if table is None or len(table) == 0:
            break
        pages.append(table)
        offset += len(table)
        if len(table) < limit:
            break
    return pages


def load_table(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    stats_data_id: str,
    merge_keys: list[str],
    app_id: str,
) -> int:
    """1テーブル分のデータを取得し DuckLake に書き込む。"""
    fqn = f"estat.{SOURCE_SCHEMA}.{name}"

    pages = fetch_pages(app_id, stats_data_id)
    if not pages:
        print(f"    No data returned")
        return 0
    combined = pa.concat_tables(pages)
    total_rows = len(combined)

    exists = conn.execute(
        f"SELECT count(*) FROM information_schema.tables "
        f"WHERE table_catalog='estat' AND table_schema='{SOURCE_SCHEMA}' "
        f"AND table_name='{name}'"
    ).fetchone()[0]

    if not exists:
        conn.register("_new_data", combined)
        conn.execute(f"CREATE TABLE {fqn} AS SELECT * FROM _new_data")
        conn.unregister("_new_data")
    else:
        conn.register("_new_data", combined)
        if merge_keys:
            keys_condition = " AND ".join(
                f"{fqn}.{k} = _new_data.{k}" for k in merge_keys
            )
            conn.execute(
                f"DELETE FROM {fqn} WHERE EXISTS "
                f"(SELECT 1 FROM _new_data WHERE {keys_condition})"
            )
            conn.execute(f"INSERT INTO {fqn} SELECT * FROM _new_data")
        else:
            conn.execute(f"DELETE FROM {fqn}")
            conn.execute(f"INSERT INTO {fqn} SELECT * FROM _new_data")
        conn.unregister("_new_data")

    return total_rows


def main() -> None:
    app_id = os.environ["ESTAT_API_KEY"]
    data_path = os.environ.get("ESTAT_DATA_PATH", DEFAULT_DATA_PATH)

    with open(INGESTION_DIR / "tables.yml") as f:
        config = yaml.safe_load(f)

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL ducklake; LOAD ducklake;")

    if os.environ.get("S3_ENDPOINT"):
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"""
            SET s3_url_style = 'path';
            SET s3_access_key_id = '{os.environ["S3_ACCESS_KEY_ID"]}';
            SET s3_secret_access_key = '{os.environ["S3_SECRET_ACCESS_KEY"]}';
            SET s3_endpoint = '{os.environ["S3_ENDPOINT"]}';
            SET s3_region = 'auto';
        """)

    conn.execute(f"""
        ATTACH 'ducklake:{DUCKLAKE_FILE}' AS estat (
            DATA_PATH '{data_path}',
            OVERRIDE_DATA_PATH true
        )
    """)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS estat.{SOURCE_SCHEMA}")

    for table_def in config["tables"]:
        name = table_def["name"]
        stats_id = table_def["statsDataId"]
        merge_keys = table_def.get("merge_keys", [])
        print(f"  Loading {name} (statsDataId: {stats_id})")
        rows = load_table(conn, name, stats_id, merge_keys, app_id)
        print(f"    -> {rows} rows")

    conn.close()


main()
