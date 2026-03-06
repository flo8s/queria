"""e-Stat API からデータを取得し dlt 経由で DuckLake に書き込む。"""

import logging
import os
from pathlib import Path

import dlt
import yaml
from estat_api_dlt_helper import (
    DestinationConfig,
    EstatDltConfig,
    SourceConfig,
    create_estat_resource,
)

from queria.dlt import create_destination

# dlt の ArrowExtractor が merge 時に出す column hints 差異の WARNING を抑制
logging.getLogger("dlt.extract.extractors").setLevel(logging.ERROR)

SOURCE_SCHEMA = "_source"


def main() -> None:
    app_id = os.environ["ESTAT_API_KEY"]

    with open(Path(__file__).parent / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    destination = create_destination()
    pipeline = dlt.pipeline(
        pipeline_name="estat",
        destination=destination,
        dataset_name=SOURCE_SCHEMA,
    )
    info = pipeline.run(
        [
            create_estat_resource(
                EstatDltConfig(
                    source=SourceConfig(
                        app_id=app_id, statsDataId=table_def["statsDataId"]
                    ),
                    destination=DestinationConfig(
                        destination=destination,
                        dataset_name=SOURCE_SCHEMA,
                        table_name=table_def["name"],
                        write_disposition="merge"
                        if table_def.get("merge_keys")
                        else "replace",
                        primary_key=table_def.get("merge_keys") or None,
                    ),
                )
            )
            for table_def in tables_config["tables"]
        ]
    )
    print(info)
