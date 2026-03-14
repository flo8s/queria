"""不動産情報ライブラリ API 取得 + dbt ビルドパイプライン。"""

import os
from datetime import date
from itertools import product

import duckdb
import pyarrow as pa
from dbt.cli.main import dbtRunner
from reinfolib import ReinfolibClient

from fdl.ducklake import connect

type Year = int
type Quarter = int
type YearQuarter = tuple[Year, Quarter]

TABLE = "reinfolib._source.trade_prices"
PRICE_CLASSIFICATION = "01"
START: YearQuarter = (2005, 3)


def main():
    ingest()

    result = dbtRunner().invoke(["deps"])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbtRunner().invoke(["run"])
    if not result.success:
        raise SystemExit("dbt run failed")

    result = dbtRunner().invoke(["docs", "generate"])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


def ingest() -> None:
    """API からデータを取得し DuckLake に直接書き込む。"""
    api_key = os.environ["REINFOLIB_API_KEY"]
    areas = [f"{a:02d}" for a in range(1, 48)]
    all_quarters = _generate_quarters(START)

    with connect() as conn, ReinfolibClient(api_key) as client:
        conn.execute("CREATE SCHEMA IF NOT EXISTS reinfolib._source")
        quarters = _pending_quarters(conn, all_quarters, area_count=len(areas))
        if not quarters:
            print("  新しいデータなし")
            return
        print(f"  取得対象: {len(quarters)} 四半期")
        ingest_trade_prices(conn, client, areas=areas, quarters=quarters)


def ingest_trade_prices(
    conn: duckdb.DuckDBPyConnection,
    client: ReinfolibClient,
    *,
    areas: list[str],
    quarters: list[YearQuarter],
) -> None:
    """XIT001: 取引価格・成約価格を取得。"""
    current = quarters[-1]
    completed = _completed_pairs(conn)

    for area, (year, quarter) in product(areas, quarters):
        if (area, year, quarter) in completed and (year, quarter) != current:
            continue
        rows = client.get_real_estate_prices(
            year=year,
            quarter=quarter,
            area=area,
            price_classification=PRICE_CLASSIFICATION,
        )
        if not rows:
            continue

        # DELETE-INSERT の冪等性キーとしてリクエストパラメータを付与
        for row in rows:
            row["_area_code"] = area
            row["_year"] = year
            row["_quarter"] = quarter

        conn.register("_batch", pa.Table.from_pylist(rows))
        # Arrow スキーマからテーブルを初回作成（データは入れない）
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {TABLE} AS SELECT * FROM _batch WITH NO DATA"
        )
        # 同一パーティションの既存データを削除して再挿入（冪等性を保証）
        # トランザクションで囲み、中断時にDELETEだけ残らないようにする
        conn.execute("BEGIN")
        conn.execute(
            f"DELETE FROM {TABLE} WHERE _area_code = ? AND _year = ? AND _quarter = ?",
            [area, year, quarter],
        )
        conn.execute(f"INSERT INTO {TABLE} SELECT * FROM _batch")
        conn.execute("COMMIT")
        conn.unregister("_batch")

        print(f"  XIT001 area={area} {year}Q{quarter}: {len(rows)} rows")


def _completed_pairs(
    conn: duckdb.DuckDBPyConnection,
) -> set[tuple[str, int, int]]:
    """取得済みの (area_code, year, quarter) ペアを返す。"""
    try:
        return {
            (row[0], row[1], row[2])
            for row in conn.execute(
                f"SELECT DISTINCT _area_code, _year, _quarter FROM {TABLE}"
            ).fetchall()
        }
    except duckdb.CatalogException:
        return set()


def _pending_quarters(
    conn: duckdb.DuckDBPyConnection,
    quarters: list[YearQuarter],
    *,
    area_count: int,
) -> list[YearQuarter]:
    """未取得・不完全な四半期を返す。最新四半期は常に再取得対象。"""
    exists = conn.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = 'reinfolib' AND table_schema = '_source' "
        "AND table_name = 'trade_prices'"
    ).fetchone()[0] > 0

    if not exists:
        return quarters

    # 全 area が揃っている四半期のみ「完了」とみなす
    complete = {
        (row[0], row[1])
        for row in conn.execute(
            f"SELECT _year, _quarter FROM {TABLE} "
            f"GROUP BY _year, _quarter "
            f"HAVING COUNT(DISTINCT _area_code) >= ?",
            [area_count],
        ).fetchall()
    }

    current = quarters[-1]
    return [q for q in quarters if q not in complete or q == current]


def _generate_quarters(
    start: YearQuarter,
    end: YearQuarter | None = None,
) -> list[YearQuarter]:
    """取得対象の四半期 (year, quarter) リストを生成する。"""
    if end is None:
        today = date.today()
        end = (today.year, (today.month - 1) // 3 + 1)
    return [
        (y, q)
        for y in range(start[0], end[0] + 1)
        for q in range(1, 5)
        if start <= (y, q) <= end
    ]


if __name__ == "__main__":
    main()
