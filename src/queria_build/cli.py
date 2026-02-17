"""queria-build CLI エントリーポイント。"""

import argparse
import sys
from pathlib import Path

from queria_build.catalog import generate_catalog


def discover_datasources(datasets_dir: Path) -> list[str]:
    """datasets/ 配下の _catalog.yml を持つディレクトリ名を返す。"""
    return sorted(
        p.parent.name
        for p in datasets_dir.glob("*/_catalog.yml")
    )


def cmd_catalog(args: argparse.Namespace) -> None:
    datasets_dir = Path.cwd() / "datasets"
    if not datasets_dir.is_dir():
        print("Error: datasets/ ディレクトリが見つかりません。プロジェクトルートで実行してください。", file=sys.stderr)
        sys.exit(1)

    if args.datasource:
        datasources = [args.datasource]
    else:
        datasources = discover_datasources(datasets_dir)
        if not datasources:
            print("Error: _catalog.yml が見つかりません。", file=sys.stderr)
            sys.exit(1)

    for datasource in datasources:
        generate_catalog(datasets_dir, datasource)


def main() -> None:
    parser = argparse.ArgumentParser(prog="queria-build")
    subparsers = parser.add_subparsers(dest="command", required=True)

    catalog_parser = subparsers.add_parser("catalog", help="カタログメタデータを生成")
    catalog_parser.add_argument("--datasource", help="対象データソース名 (省略時は全データセット)")

    args = parser.parse_args()
    if args.command == "catalog":
        cmd_catalog(args)


if __name__ == "__main__":
    main()
