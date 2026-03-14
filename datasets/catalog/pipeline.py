"""generate_sources + dbt ビルド + メタデータ生成パイプライン。"""

from dbt.cli.main import dbtRunner


def main():
    # ソース定義を自動生成
    from generate_sources import main as gen

    gen()

    # dbt ビルド (invoke ごとに新しいインスタンスを使い、deps 後のマクロ解決を確実にする)
    result = dbtRunner().invoke(["deps"])
    if not result.success:
        raise SystemExit("dbt deps failed")

    result = dbtRunner().invoke(["run"])
    if not result.success:
        raise SystemExit("dbt run failed")

    result = dbtRunner().invoke(["docs", "generate"])
    if not result.success:
        raise SystemExit("dbt docs generate failed")


if __name__ == "__main__":
    main()
