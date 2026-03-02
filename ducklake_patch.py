"""dlt DuckLake destination に OVERRIDE_DATA_PATH サポートを追加するモンキーパッチ。

DuckLake の ATTACH 文で OVERRIDE_DATA_PATH true を指定すると、
カタログに保存された DATA_PATH を変更せずに、現在の接続でのみ別のストレージパスを使える。

環境変数 DLT_DUCKLAKE_OVERRIDE_DATA_PATH が truthy のとき有効になる。

使い方:
    import ducklake_patch
    ducklake_patch.apply()
"""

import os

from dlt.destinations.impl.ducklake.sql_client import DuckLakeSqlClient

_original_build_attach_statement = DuckLakeSqlClient.build_attach_statement


def _patched_build_attach_statement(
    *,
    ducklake_name: str,
    catalog: object,
    storage_url: str,
) -> str:
    result = _original_build_attach_statement(
        ducklake_name=ducklake_name,
        catalog=catalog,
        storage_url=storage_url,
    )
    if os.environ.get("DLT_DUCKLAKE_OVERRIDE_DATA_PATH", "").lower() in (
        "1",
        "true",
        "yes",
    ):
        # 末尾の ")" の手前に OVERRIDE_DATA_PATH true を挿入
        result = result[:-1] + ", OVERRIDE_DATA_PATH true)"
    return result


def apply() -> None:
    """モンキーパッチを適用する。"""
    DuckLakeSqlClient.build_attach_statement = staticmethod(  # type: ignore[assignment]
        _patched_build_attach_statement
    )
