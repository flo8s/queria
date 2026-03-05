"""e-Stat 国勢調査2020 小地域（町丁・字等）境界データのダウンロード・変換スクリプト.

47都道府県分の Shapefile を e-Stat からダウンロードし、GeoParquet に変換して保存する。

Usage:
    python -m ingestion.boundary [--refresh] [--output-dir OUTPUT_DIR]
"""

from __future__ import annotations

import argparse
import io
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import requests

BASE_URL = (
    "https://www.e-stat.go.jp/gis/statmap-search/data"
    "?dlserveyId=A002005212020&code={code}&coordSys=1"
    "&format=shape&downloadType=5&datum=2011"
)

PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

DEFAULT_OUTPUT_DIR = Path(__file__).parent / "output"

USER_AGENT = (
    "Mozilla/5.0 (compatible; queria-boundary-downloader/1.0; "
    "+https://github.com/flo8s/queria)"
)

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds


def download_shapefile(pref_code: str, *, session: requests.Session) -> gpd.GeoDataFrame:
    """e-Stat から指定都道府県の Shapefile ZIP をダウンロードし GeoDataFrame として返す."""
    url = BASE_URL.format(code=pref_code)

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            break
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                print(f"  リトライ ({attempt + 1}/{MAX_RETRIES}): {e}, {wait}秒後に再試行")
                time.sleep(wait)
            else:
                raise

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(tmpdir)

        shp_files = list(Path(tmpdir).rglob("*.shp"))
        if not shp_files:
            msg = f"都道府県 {pref_code}: ZIP 内に .shp ファイルが見つかりません"
            raise FileNotFoundError(msg)

        gdf = gpd.read_file(shp_files[0], encoding="cp932")

    return gdf


def main() -> None:
    parser = argparse.ArgumentParser(description="e-Stat 小地域境界データのダウンロード")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="既存のファイルを上書きして再ダウンロード",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"出力ディレクトリ (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--pref",
        type=str,
        nargs="*",
        help="特定の都道府県コードのみ処理 (例: 08 13)",
    )
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    pref_codes = args.pref if args.pref else PREF_CODES

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    success_count = 0
    skip_count = 0
    error_count = 0

    for pref_code in pref_codes:
        output_path = output_dir / f"boundary_{pref_code}.parquet"

        if output_path.exists() and not args.refresh:
            print(f"[{pref_code}] スキップ (既に存在): {output_path}")
            skip_count += 1
            continue

        print(f"[{pref_code}] ダウンロード中...")
        try:
            gdf = download_shapefile(pref_code, session=session)
            gdf.to_parquet(output_path)
            print(f"[{pref_code}] 保存完了: {output_path} ({len(gdf)} ポリゴン)")
            success_count += 1
        except Exception as e:
            print(f"[{pref_code}] エラー: {e}", file=sys.stderr)
            error_count += 1

    print(f"\n完了: 成功={success_count}, スキップ={skip_count}, エラー={error_count}")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
