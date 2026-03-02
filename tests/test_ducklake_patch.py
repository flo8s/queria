"""ducklake_patch の動作確認テスト。"""

import os

import pytest
from dlt.common.configuration.specs import ConnectionStringCredentials

import ducklake_patch


@pytest.fixture(autouse=True)
def _apply_patch():
    """各テストの前にパッチを適用し、後に元に戻す。"""
    ducklake_patch.apply()
    yield
    from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

    DuckLakeSqlClient.build_attach_statement = staticmethod(  # type: ignore[assignment]
        ducklake_patch._original_build_attach_statement
    )


def _build(env_value: str | None = None) -> str:
    from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

    old = os.environ.pop("DLT_DUCKLAKE_OVERRIDE_DATA_PATH", None)
    try:
        if env_value is not None:
            os.environ["DLT_DUCKLAKE_OVERRIDE_DATA_PATH"] = env_value
        return DuckLakeSqlClient.build_attach_statement(
            ducklake_name="test_lake",
            catalog=ConnectionStringCredentials(
                {"drivername": "sqlite", "database": "test.sqlite"}
            ),
            storage_url="/tmp/data",
        )
    finally:
        if old is not None:
            os.environ["DLT_DUCKLAKE_OVERRIDE_DATA_PATH"] = old
        else:
            os.environ.pop("DLT_DUCKLAKE_OVERRIDE_DATA_PATH", None)


class TestOverrideDataPath:
    def test_env_true(self):
        sql = _build("true")
        assert "OVERRIDE_DATA_PATH true" in sql
        assert sql.endswith(")")

    def test_env_1(self):
        sql = _build("1")
        assert "OVERRIDE_DATA_PATH true" in sql

    def test_env_yes(self):
        sql = _build("yes")
        assert "OVERRIDE_DATA_PATH true" in sql

    def test_env_unset(self):
        sql = _build(None)
        assert "OVERRIDE_DATA_PATH" not in sql

    def test_env_empty(self):
        sql = _build("")
        assert "OVERRIDE_DATA_PATH" not in sql

    def test_env_false(self):
        sql = _build("false")
        assert "OVERRIDE_DATA_PATH" not in sql

    def test_sql_structure(self):
        sql = _build("true")
        assert sql.startswith("ATTACH IF NOT EXISTS 'ducklake:test.sqlite'")
        assert "AS test_lake" in sql
        assert "DATA_PATH '/tmp/data'" in sql
