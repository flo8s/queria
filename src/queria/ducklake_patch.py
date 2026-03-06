"""dlt DuckLake destination のモンキーパッチ。

build_attach_statement に override_data_path 引数を追加する。
dlt 本体に同等の機能が入るまでの暫定対応。
(ref: https://github.com/dlt-hub/dlt/pull/3709)

使い方:
    from queria import ducklake_patch
    ducklake_patch.apply(override_data_path=True)
"""

import functools

from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

_original_build_attach_statement = DuckLakeSqlClient.build_attach_statement


def _patched_build_attach_statement(
    *,
    ducklake_name: str,
    catalog: object,
    storage_url: str,
    override_data_path: bool = False,
) -> str:
    result = _original_build_attach_statement(
        ducklake_name=ducklake_name,
        catalog=catalog,
        storage_url=storage_url,
    )
    if override_data_path:
        result = result[:-1] + ", OVERRIDE_DATA_PATH true)"
    return result


def apply(*, override_data_path: bool = False) -> None:
    """モンキーパッチを適用する。"""
    patched = _patched_build_attach_statement
    if override_data_path:
        patched = functools.partial(patched, override_data_path=True)
    DuckLakeSqlClient.build_attach_statement = staticmethod(patched)
