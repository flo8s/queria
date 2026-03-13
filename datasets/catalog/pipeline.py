"""generate_sources + dbt ビルド + メタデータ生成パイプライン。"""

from dbt.cli.main import dbtRunner


def main():
    # ソース定義を自動生成
    from generate_sources import main as gen

    gen()

    # dbt ビルド
    dbt = dbtRunner()

    result = dbt.invoke(["deps"])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbt.invoke(["run"])
    if not result.success:
        raise SystemExit("dbt run failed")

    result = dbt.invoke(["docs", "generate"])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


if __name__ == "__main__":
    main()
