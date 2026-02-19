"""Pydantic model definitions for dataset metadata output."""

from typing import Literal

from pydantic import BaseModel

Materialization = Literal["table", "view", "incremental"]


class ColumnInfo(BaseModel):
    name: str
    description: str
    data_type: str


class ModelInfo(BaseModel):
    name: str
    title: str
    description: str
    tags: list[str]
    license: str
    license_url: str
    source_url: str
    materialized: Materialization
    columns: list[ColumnInfo]
    sql: str | None = None


class SchemaInfo(BaseModel):
    title: str
    tables: list[ModelInfo]


class NodeConfig(BaseModel):
    materialized: str


class NodeInfo(BaseModel):
    fqn: list[str]
    resource_type: str
    config: NodeConfig
    meta: dict


class LineageInfo(BaseModel):
    parent_map: dict[str, list[str]]
    nodes: dict[str, NodeInfo]


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
    ducklake_url: str
    schemas: dict[str, DatasetSchemaConfig] = {}
    dependencies: list[DependencyInfo] | None = None


class DatasetMetadata(BaseModel):
    title: str
    description: str
    tags: list[str]
    ducklake_url: str
    schemas: dict[str, SchemaInfo]
    lineage: LineageInfo
    dependencies: list[DependencyInfo] | None = None
