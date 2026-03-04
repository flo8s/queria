"""BODIK ODCS（CKAN）からデータを取得し dlt 経由で DuckLake に書き込む。"""

import argparse
import gc
import logging
import os
from pathlib import Path
from typing import Any, Generator

import yaml
from dlt.sources.helpers.requests import Client

logging.getLogger("dlt.extract.extractors").setLevel(logging.ERROR)

INGESTION_DIR = Path(__file__).parent
DATASET_DIR = INGESTION_DIR.parent
DIST_DIR = DATASET_DIR / "dist"
DUCKLAKE_SQLITE = DIST_DIR / "ducklake.sqlite"
DUCKLAKE_FILES_DIR = "ducklake.duckdb.files"
DATASOURCE = "bodik"
SOURCE_SCHEMA = "_source"

DATASTORE_SEARCH_LIMIT = 1000


def _fetch_resource_last_modified(base_url: str, resource_id: str) -> str | None:
    """CKAN resource_show API からリソースの last_modified を取得。"""
    try:
        client = Client(request_timeout=30)
        resp = client.get(
            f"{base_url}/api/3/action/resource_show",
            params={"id": resource_id},
        )
        data = resp.json()
        if data.get("success"):
            result = data["result"]
            return result.get("last_modified") or result.get("metadata_modified")
    except Exception:
        return None


def _fetch_datastore_records(
    base_url: str,
    resource_id: str,
) -> Generator[list[dict[str, Any]], None, None]:
    """CKAN datastore_search API からレコードをページネーション付きで取得。"""
    client = Client()
    offset = 0
    while True:
        resp = client.get(
            f"{base_url}/api/3/action/datastore_search",
            params={
                "resource_id": resource_id,
                "limit": DATASTORE_SEARCH_LIMIT,
                "offset": offset,
            },
        )
        data = resp.json()

        if not data.get("success"):
            raise RuntimeError(
                f"CKAN API error: {data.get('error', 'Unknown error')}"
            )

        result = data["result"]
        records = result["records"]
        if not records:
            break

        # _id フィールドを除去（CKAN 内部の行 ID）
        for record in records:
            record.pop("_id", None)

        yield records

        total = result.get("total", 0)
        offset += len(records)
        if offset >= total:
            break


def _load_single_resource(
    name: str,
    resource_id: str,
    base_url: str,
    data_path: str,
    last_modified: str | None = None,
) -> None:
    """1 リソース分のデータを CKAN API から取得して DuckLake に書き込む。"""
    import sys

    # リポジトリ root の ducklake_patch.py を import するためパスを追加
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    import dlt
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

    import ducklake_patch

    ducklake_patch.apply()

    os.environ["SCHEMA__NAMING"] = "direct"

    destination = dlt.destinations.ducklake(
        credentials=DuckLakeCredentials(
            ducklake_name=DATASOURCE,
            catalog=f"duckdb:///{DUCKLAKE_SQLITE}",
            storage=data_path,
        )
    )

    @dlt.resource(name=name, write_disposition="replace")
    def ckan_resource():
        for batch in _fetch_datastore_records(base_url, resource_id):
            yield batch
        if last_modified:
            dlt.current.source_state()[name] = last_modified

    pipeline = dlt.pipeline(
        pipeline_name=f"bodik_{name}",
        destination=destination,
        dataset_name=SOURCE_SCHEMA,
    )

    info = pipeline.run(ckan_resource())
    print(f"    -> {info}")


def _get_stored_last_modified(name: str, data_path: str) -> str | None:
    """dlt pipeline の source state から前回の last_modified を取得する。"""
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

    import dlt
    from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

    import ducklake_patch

    ducklake_patch.apply()

    try:
        destination = dlt.destinations.ducklake(
            credentials=DuckLakeCredentials(
                ducklake_name=DATASOURCE,
                catalog=f"duckdb:///{DUCKLAKE_SQLITE}",
                storage=data_path,
            )
        )
        pipeline = dlt.pipeline(
            pipeline_name=f"bodik_{name}",
            destination=destination,
            dataset_name=SOURCE_SCHEMA,
        )
        for source_state in pipeline.state.get("sources", {}).values():
            if name in source_state:
                return source_state[name]
    except Exception:
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-if-not-modified",
        action="store_true",
        help="CKAN の last_modified を見て、更新があったリソースのみ再取得",
    )
    args = parser.parse_args()

    storage_path = str(DIST_DIR) + f"/{DUCKLAKE_FILES_DIR}/"
    os.environ["DLT_DUCKLAKE_OVERRIDE_DATA_PATH"] = "true"

    with open(INGESTION_DIR / "resources.yml") as f:
        config = yaml.safe_load(f)

    base_url = config["base_url"]
    loaded = 0

    for res_def in config["resources"]:
        name = res_def["name"]
        resource_id = res_def["resource_id"]

        last_modified = _fetch_resource_last_modified(base_url, resource_id)

        if args.skip_if_not_modified:
            stored = _get_stored_last_modified(name, storage_path)
            if stored is not None and stored == last_modified:
                print(f"  Skipping {name} ({resource_id}, up to date)")
                continue

        print(f"  Loading {name} (resource_id: {resource_id})")
        _load_single_resource(name, resource_id, base_url, storage_path, last_modified)
        loaded += 1
        gc.collect()

    if loaded == 0:
        print("  No resources loaded")


main()
