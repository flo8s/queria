"""e-Stat 国勢調査2020 小地域（町丁・字等）境界データのダウンロード・変換スクリプト.

47都道府県分の Shapefile を e-Stat からダウンロードし、
DuckDB spatial 拡張で GeoParquet に変換して保存する。

Usage:
    python -m ingestion.boundary
"""

from __future__ import annotations

import io
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import duckdb
import requests

BASE_URL = (
    "https://www.e-stat.go.jp/gis/statmap-search/data"
    "?dlserveyId=A002005212020&code={code}&coordSys=1"
    "&format=shape&downloadType=5&datum=2011"
)

PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

OUTPUT_DIR = Path(__file__).parent / "output"

USER_AGENT = (
    "Mozilla/5.0 (compatible; queria-boundary-downloader/1.0; "
    "+https://github.com/flo8s/queria)"
)

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds


def download_zip(pref_code: str, *, session: requests.Session) -> bytes:
    """e-Stat から指定都道府県の Shapefile ZIP をダウンロードする."""
    url = BASE_URL.format(code=pref_code)

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                print(f"  リトライ ({attempt + 1}/{MAX_RETRIES}): {e}, {wait}秒後に再試行")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("unreachable")


def convert_shapefile_to_parquet(shp_path: Path, output_path: Path) -> int:
    """DuckDB spatial で Shapefile を GeoParquet に変換する."""
    conn = duckdb.connect(":memory:")
    conn.install_extension("spatial")
    conn.load_extension("spatial")

    count = conn.execute(
        f"""
        COPY (
            SELECT * FROM ST_Read('{shp_path}', open_options=['ENCODING=CP932'])
        ) TO '{output_path}' (FORMAT PARQUET)
        """
    ).fetchone()[0]

    conn.close()
    return count


def process_prefecture(
    pref_code: str, output_path: Path, *, session: requests.Session
) -> int:
    """1都道府県分のダウンロード→変換を実行し、ポリゴン数を返す."""
    zip_content = download_zip(pref_code, session=session)

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            zf.extractall(tmpdir)

        shp_files = list(Path(tmpdir).rglob("*.shp"))
        if not shp_files:
            msg = f"都道府県 {pref_code}: ZIP 内に .shp ファイルが見つかりません"
            raise FileNotFoundError(msg)

        return convert_shapefile_to_parquet(shp_files[0], output_path)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    success_count = 0
    error_count = 0

    for pref_code in PREF_CODES:
        output_path = OUTPUT_DIR / f"boundary_{pref_code}.parquet"

        print(f"[{pref_code}] ダウンロード中...")
        try:
            count = process_prefecture(pref_code, output_path, session=session)
            print(f"[{pref_code}] 保存完了: {output_path} ({count} ポリゴン)")
            success_count += 1
        except Exception as e:
            print(f"[{pref_code}] エラー: {e}", file=sys.stderr)
            error_count += 1

    print(f"\n完了: 成功={success_count}, エラー={error_count}")

    if error_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
