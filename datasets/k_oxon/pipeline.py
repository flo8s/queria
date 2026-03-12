"""dbt ビルド + メタデータ生成パイプライン。"""

from dbt.cli.main import dbtRunner


def main():
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
