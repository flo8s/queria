"""Pydantic model definitions for dataset configuration (dataset.yml)."""

from pydantic import BaseModel


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
