"""e-Stat API からデータを取得し dlt 経由で DuckLake に書き込む。"""

import os
from pathlib import Path

import dlt
import yaml
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials
from estat_api_dlt_helper import (
    DestinationConfig,
    EstatDltConfig,
    SourceConfig,
    create_estat_pipeline,
    create_estat_resource,
)

INGESTION_DIR = Path(__file__).parent
DIST_DIR = INGESTION_DIR.parent / "dist"
DUCKLAKE_FILE = DIST_DIR / "ducklake.duckdb"
DEFAULT_DATA_PATH = str(DIST_DIR / "ducklake.duckdb.files") + "/"
SOURCE_SCHEMA = "_source"


def _build_destination(data_path: str):
    """DuckLake destination を構築（dev: ローカル, prd: S3）。"""
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

    return dlt.destinations.ducklake(
        credentials=DuckLakeCredentials(
            ducklake_name="estat",
            catalog=f"duckdb:///{DUCKLAKE_FILE}",
            storage=storage,
        )
    )


def main() -> None:
    app_id = os.environ["ESTAT_API_KEY"]
    data_path = os.environ.get("ESTAT_DATA_PATH", DEFAULT_DATA_PATH)

    with open(INGESTION_DIR / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    destination = _build_destination(data_path)

    for table_def in tables_config["tables"]:
        name = table_def["name"]
        stats_id = table_def["statsDataId"]
        merge_keys = table_def.get("merge_keys", [])

        print(f"  Loading {name} (statsDataId: {stats_id})")

        estat_config = EstatDltConfig(
            source=SourceConfig(
                app_id=app_id,
                statsDataId=stats_id,
            ),
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


main()
