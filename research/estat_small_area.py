"""e-Stat API 小地域（町丁・字等）統計データの調査スクリプト。

getStatsList API (searchKind=2) を呼び出し、利用可能な小地域統計表を網羅的に収集する。
結果を JSON ファイルに出力し、後続の YAML 設定ファイル作成に使う。

実行方法:
    ESTAT_API_KEY=your_key python docs/research/estat_small_area.py
"""

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

BASE_URL = "https://api.e-stat.go.jp/rest/3.0/app/json/"
OUTPUT_DIR = Path(__file__).parent
REQUEST_INTERVAL = 1  # seconds


def fetch_stats_list(
    app_id: str, search_kind: int = 2, limit: int = 100
) -> list[dict[str, Any]]:
    """getStatsList API で統計表一覧を全件取得する（ページネーション対応）。"""
    all_tables: list[dict[str, Any]] = []
    start_position = 1

    while True:
        params = {
            "appId": app_id,
            "searchKind": search_kind,
            "startPosition": start_position,
            "limit": limit,
            "lang": "J",
        }
        url = f"{BASE_URL}getStatsList?{urlencode(params)}"
        print(f"  GET {BASE_URL}getStatsList (startPosition={start_position})")

        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        result = data.get("GET_STATS_LIST", {})
        datalist_inf = result.get("DATALIST_INF", {})
        number = int(datalist_inf.get("NUMBER", 0))
        result_inf = datalist_inf.get("RESULT_INF", {})

        table_inf = datalist_inf.get("TABLE_INF", [])
        if isinstance(table_inf, dict):
            table_inf = [table_inf]
        all_tables.extend(table_inf)

        from_number = int(result_inf.get("FROM_NUMBER", 0))
        to_number = int(result_inf.get("TO_NUMBER", 0))
        print(f"    -> {from_number}-{to_number} / {number}")

        if to_number >= number or not table_inf:
            break

        start_position = to_number + 1
        time.sleep(REQUEST_INTERVAL)

    return all_tables


def extract_title(table: dict[str, Any]) -> str:
    """TABLE_INF から TITLE テキストを取得する。"""
    title = table.get("TITLE", "")
    if isinstance(title, dict):
        return title.get("$", str(title))
    return str(title)


def extract_stat_name(table: dict[str, Any]) -> str:
    """TABLE_INF から政府統計名を取得する。"""
    stat_name = table.get("STAT_NAME", "")
    if isinstance(stat_name, dict):
        return stat_name.get("$", str(stat_name))
    return str(stat_name)


def extract_stat_code(table: dict[str, Any]) -> str:
    """TABLE_INF から政府統計コードを取得する。"""
    stat_name = table.get("STAT_NAME", "")
    if isinstance(stat_name, dict):
        return stat_name.get("@code", "")
    return ""


def classify_table(table: dict[str, Any]) -> str:
    """テーブルを small_area / mesh / unknown に分類する。"""
    title = extract_title(table)
    stat_name = extract_stat_name(table)
    statistics_name = str(table.get("STATISTICS_NAME", ""))

    all_text = f"{title} {stat_name} {statistics_name}"

    if "メッシュ" in all_text:
        return "mesh"

    small_area_keywords = ["小地域", "町丁", "字等"]
    if any(kw in all_text for kw in small_area_keywords):
        return "small_area"

    return "unknown"


def fetch_stats_data_sample(
    app_id: str, stats_data_id: str, limit: int = 5
) -> dict[str, Any]:
    """getStatsData API でサンプルデータと CLASS_INF メタデータを取得する。"""
    params = {
        "appId": app_id,
        "statsDataId": stats_data_id,
        "limit": limit,
        "metaGetFlg": "Y",
        "cntGetFlg": "N",
        "lang": "J",
    }
    url = f"{BASE_URL}getStatsData?{urlencode(params)}"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def extract_class_inf(response: dict[str, Any]) -> list[dict[str, Any]]:
    """レスポンスから CLASS_INF の要約を抽出する。"""
    stats_data = response.get("GET_STATS_DATA", {})
    statistical_data = stats_data.get("STATISTICAL_DATA", {})
    class_inf = statistical_data.get("CLASS_INF", {})
    class_objs = class_inf.get("CLASS_OBJ", [])
    if isinstance(class_objs, dict):
        class_objs = [class_objs]

    axes = []
    for obj in class_objs:
        classes = obj.get("CLASS", [])
        if isinstance(classes, dict):
            classes = [classes]
        axes.append(
            {
                "id": obj.get("@id", ""),
                "name": obj.get("@name", ""),
                "num_classes": len(classes),
                "sample_codes": [c.get("@code", "") for c in classes[:5]],
                "sample_names": [c.get("@name", "") for c in classes[:5]],
            }
        )
    return axes


def main() -> None:
    app_id = os.environ.get("ESTAT_API_KEY")
    if not app_id:
        print("ERROR: ESTAT_API_KEY 環境変数を設定してください", file=sys.stderr)
        sys.exit(1)

    # Step 1: 全テーブル一覧を取得
    print("=" * 60)
    print("Step 1: getStatsList (searchKind=2) で全テーブル一覧を取得")
    print("=" * 60)
    all_tables = fetch_stats_list(app_id, search_kind=2)
    print(f"\n合計: {len(all_tables)} テーブル\n")

    # Step 2: 小地域 / 地域メッシュ / 不明 に分類
    print("=" * 60)
    print("Step 2: 小地域 / 地域メッシュ に分類")
    print("=" * 60)
    classified: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table in all_tables:
        category = classify_table(table)
        classified[category].append(table)

    for cat, tables in sorted(classified.items()):
        print(f"  {cat}: {len(tables)} テーブル")

    small_area_tables = classified["small_area"]
    if not small_area_tables:
        print("\n小地域テーブルが見つかりませんでした。")
        print("unknown に分類されたテーブルのタイトルを確認してください:")
        for t in classified["unknown"][:10]:
            print(f"  - {t.get('@id')}: {extract_title(t)}")
        # unknown を含めて結果を保存
        small_area_tables = classified["unknown"]

    # Step 3: 政府統計名でグループ化
    print(f"\n{'=' * 60}")
    print("Step 3: 政府統計名でグループ化")
    print("=" * 60)
    by_survey: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table in small_area_tables:
        survey_name = extract_stat_name(table)
        by_survey[survey_name].append(table)

    for survey, tables in sorted(by_survey.items(), key=lambda x: -len(x[1])):
        dates = sorted(set(str(t.get("SURVEY_DATE", "")) for t in tables))
        print(f"  {survey} ({extract_stat_code(tables[0])})")
        print(f"    テーブル数: {len(tables)}")
        print(f"    調査年: {', '.join(dates[-5:])}")  # 直近5件

    # Step 4: 各調査の最新テーブルから CLASS_INF サンプルを取得
    print(f"\n{'=' * 60}")
    print("Step 4: 各調査の最新テーブルから CLASS_INF を取得")
    print("=" * 60)
    class_inf_samples: dict[str, Any] = {}

    for survey, tables in sorted(by_survey.items()):
        # 調査年の最新を取得
        latest = max(tables, key=lambda t: str(t.get("SURVEY_DATE", "0")))
        stats_id = latest["@id"]
        title = extract_title(latest)
        print(f"\n  [{survey}] {title}")
        print(f"    statsDataId: {stats_id}")

        try:
            resp = fetch_stats_data_sample(app_id, stats_id, limit=5)
            axes = extract_class_inf(resp)
            class_inf_samples[stats_id] = {
                "survey": survey,
                "title": title,
                "survey_date": str(latest.get("SURVEY_DATE", "")),
                "axes": axes,
            }
            for ax in axes:
                print(f"    {ax['id']} ({ax['name']}): {ax['num_classes']} classes")
                if ax["sample_names"]:
                    print(f"      例: {', '.join(ax['sample_names'][:3])}")
        except Exception as e:
            print(f"    ERROR: {e}")
            class_inf_samples[stats_id] = {"error": str(e)}

        time.sleep(REQUEST_INTERVAL)

    # Step 5: 全結果を JSON に保存
    print(f"\n{'=' * 60}")
    print("Step 5: 結果を JSON に保存")
    print("=" * 60)

    # テーブルの簡易情報に変換
    def simplify_table(t: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": t.get("@id", ""),
            "stat_name": extract_stat_name(t),
            "stat_code": extract_stat_code(t),
            "title": extract_title(t),
            "survey_date": str(t.get("SURVEY_DATE", "")),
            "statistics_name": str(t.get("STATISTICS_NAME", "")),
            "small_area": str(t.get("SMALL_AREA", "")),
            "collect_area": str(t.get("COLLECT_AREA", "")),
        }

    output = {
        "meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "search_kind": 2,
            "total_tables": len(all_tables),
            "small_area_count": len(classified["small_area"]),
            "mesh_count": len(classified["mesh"]),
            "unknown_count": len(classified["unknown"]),
        },
        "small_area_tables": [simplify_table(t) for t in classified["small_area"]],
        "mesh_tables": [simplify_table(t) for t in classified["mesh"]],
        "unknown_tables": [simplify_table(t) for t in classified["unknown"]],
        "by_survey": {
            survey: {
                "count": len(tables),
                "stat_code": extract_stat_code(tables[0]),
                "survey_dates": sorted(
                    set(str(t.get("SURVEY_DATE", "")) for t in tables)
                ),
                "tables": [simplify_table(t) for t in tables],
            }
            for survey, tables in sorted(by_survey.items())
        },
        "class_inf_samples": class_inf_samples,
    }

    output_file = OUTPUT_DIR / "estat_small_area_raw.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  -> {output_file}")
    print(f"\n完了: 小地域 {len(classified['small_area'])} 件, "
          f"メッシュ {len(classified['mesh'])} 件, "
          f"不明 {len(classified['unknown'])} 件")


if __name__ == "__main__":
    main()
