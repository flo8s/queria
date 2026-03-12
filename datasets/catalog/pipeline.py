"""generate_sources + dbt ビルド + メタデータ生成パイプライン。"""

from dbt.cli.main import dbtRunner


def main():
    # ソース定義を自動生成
    from generate_sources import main as gen

    gen()

    # dbt ビルド
    dbt = dbtRunner()

    dbt_args = ["--project-dir", "transform", "--profiles-dir", "transform"]

    result = dbt.invoke(["deps", *dbt_args])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbt.invoke(["run", *dbt_args])
    if not result.success:
        raise SystemExit("dbt run failed")

    result = dbt.invoke(["docs", "generate", *dbt_args])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


if __name__ == "__main__":
    main()
