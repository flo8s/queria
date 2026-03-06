"""ducklake_patch の動作確認テスト。"""

import pytest
from dlt.common.configuration.specs import ConnectionStringCredentials
from queria import ducklake_patch


@pytest.fixture(autouse=True)
def _restore_original():
    """テスト後にオリジナルの build_attach_statement を復元する。"""
    yield
    from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

    DuckLakeSqlClient.build_attach_statement = staticmethod(
        ducklake_patch._original_build_attach_statement
    )


def _build(*, override_data_path: bool = False) -> str:
    from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

    ducklake_patch.apply(override_data_path=override_data_path)
    return DuckLakeSqlClient.build_attach_statement(
        ducklake_name="test_lake",
        catalog=ConnectionStringCredentials(
            {"drivername": "sqlite", "database": "test.sqlite"}
        ),
        storage_url="/tmp/data",
    )


class TestSqliteOptionPreserved:
    def test_sqlite_options_preserved(self):
        sql = _build()
        assert "META_TYPE 'sqlite'" in sql
        assert "META_JOURNAL_MODE 'WAL'" in sql
        assert "META_BUSY_TIMEOUT 1000" in sql

    def test_still_valid_sql(self):
        sql = _build()
        assert sql.startswith("ATTACH IF NOT EXISTS 'ducklake:test.sqlite'")
        assert "AS test_lake" in sql
        assert "DATA_PATH '/tmp/data'" in sql
        assert sql.endswith(")")


class TestOverrideDataPath:
    def test_enabled(self):
        sql = _build(override_data_path=True)
        assert "OVERRIDE_DATA_PATH true" in sql
        assert sql.endswith(")")

    def test_disabled(self):
        sql = _build(override_data_path=False)
        assert "OVERRIDE_DATA_PATH" not in sql

    def test_default(self):
        sql = _build()
        assert "OVERRIDE_DATA_PATH" not in sql

    def test_sql_structure(self):
        sql = _build(override_data_path=True)
        assert sql.startswith("ATTACH IF NOT EXISTS 'ducklake:test.sqlite'")
        assert "AS test_lake" in sql
        assert "DATA_PATH '/tmp/data'" in sql
