"""Ingestion: dataset pipeline script execution."""

import importlib.util
from pathlib import Path

from queria import DIST_DIR, DUCKLAKE_SQLITE, INGESTION_SCRIPT

_current_dataset_dir: Path | None = None


def get_dataset_dir() -> Path:
    """現在実行中のパイプラインのデータセットディレクトリを返す。"""
    assert _current_dataset_dir is not None, "Not inside an ingestion pipeline"
    return _current_dataset_dir


def _ingestion_script(dataset_dir: Path) -> Path:
    return dataset_dir / "ingestion" / INGESTION_SCRIPT


def has_ingestion_script(dataset_dir: Path) -> bool:
    """インジェスションスクリプトが存在するかチェックする。"""
    return _ingestion_script(dataset_dir).exists()


def ingest_datasource(dataset_dir: Path) -> None:
    """データセット固有のインジェスションスクリプトを実行する。"""
    global _current_dataset_dir

    if not has_ingestion_script(dataset_dir):
        raise FileNotFoundError(
            f"Ingestion script not found: {_ingestion_script(dataset_dir)}"
        )

    script = _ingestion_script(dataset_dir)

    spec = importlib.util.spec_from_file_location("ingestion.pipeline", script)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)

    _current_dataset_dir = dataset_dir
    try:
        spec.loader.exec_module(mod)
        mod.main()
    finally:
        _current_dataset_dir = None

    # dlt パイプラインが ducklake.sqlite を作成した場合、自動で DuckDB に変換
    dist_dir = dataset_dir / DIST_DIR
    sqlite_file = dist_dir / DUCKLAKE_SQLITE
    if sqlite_file.exists():
        from queria.dlt import _convert_to_duckdb

        _convert_to_duckdb(dataset_dir)
