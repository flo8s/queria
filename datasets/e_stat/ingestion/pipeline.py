"""e-Stat API からデータを取得し dlt 経由で DuckLake に書き込む。"""

import logging
from pathlib import Path

import dlt
import yaml
from estat_api_dlt_helper import estat_source, estat_table

from queria.dlt import create_destination

# dlt の ArrowExtractor が merge 時に出す column hints 差異の WARNING を抑制
logging.getLogger("dlt.extract.extractors").setLevel(logging.ERROR)

SOURCE_SCHEMA = "_source"


def main() -> None:
    with open(Path(__file__).parent / "tables.yml") as f:
        tables_config = yaml.safe_load(f)

    destination = create_destination()
    pipeline = dlt.pipeline(
        pipeline_name="estat",
        destination=destination,
        dataset_name=SOURCE_SCHEMA,
    )
    source = estat_source(
        tables=[
            estat_table(
                stats_data_id=t["statsDataId"],
                table_name=t["name"],
                write_disposition="merge" if t.get("merge_keys") else "replace",
                primary_key=t.get("merge_keys"),
            )
            for t in tables_config["tables"]
        ],
    )
    info = pipeline.run(source)
    print(info)
