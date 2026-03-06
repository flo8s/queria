"""Pydantic model definitions for dataset configuration (dataset.yml)."""

from pathlib import Path

import yaml
from pydantic import BaseModel

from queria import DATASET_YML


class DependencyInfo(BaseModel):
    alias: str
    ducklake_url: str


class DatasetSchemaConfig(BaseModel):
    title: str = ""


class DatasetConfig(BaseModel):
    name: str = ""
    title: str = ""
    description: str = ""
    tags: list[str] = []
    cover: str = ""
    ducklake_url: str
    schemas: dict[str, DatasetSchemaConfig] = {}
    dependencies: list[DependencyInfo] | None = None


def load_dataset_config(dataset_dir: Path) -> DatasetConfig:
    """Load and validate dataset.yml as a DatasetConfig."""
    path = dataset_dir / DATASET_YML
    if not path.exists():
        raise FileNotFoundError(f"{path} not found.")
    with open(path) as f:
        config = DatasetConfig.model_validate(yaml.safe_load(f))
    if not config.name:
        config.name = dataset_dir.resolve().name
    return config
