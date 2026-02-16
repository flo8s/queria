"""行政区域ポリゴンの簡略化 (Visvalingam-Whyatt)。

国土数値情報の行政区域ポリゴンを topojson ライブラリで簡略化し、
Web 表示に適したサイズに削減する。

簡略化パラメータ (mapshaper 互換):
  - visvalingam weighting=0.7
  - 小面積ポリゴン (< 200,000 m²): percentage=0.04
  - 大面積ポリゴン (>= 200,000 m²): percentage=0.005
  - 小島除去: 50,000 m² 未満を除外
"""

import pandas as pd
import geopandas as gpd
import topojson as tp


SMALL_ISLAND_THRESHOLD_M2 = 50_000
AREA_SPLIT_THRESHOLD_M2 = 200_000
SIMPLIFY_PCT_SMALL = 0.04
SIMPLIFY_PCT_LARGE = 0.005


def _to_geodataframe(df):
    """DataFrame を GeoDataFrame に変換する。"""
    return gpd.GeoDataFrame(df, geometry="geom", crs="EPSG:4326")


def _compute_area_m2(gdf):
    """EPSG:3857 に投影して面積 (m²) を計算し、元の CRS に戻す。"""
    projected = gdf.to_crs(epsg=3857)
    gdf = gdf.copy()
    gdf["area_m2"] = projected.geometry.area
    return gdf


def _remove_small_islands(gdf, threshold_m2):
    """面積が閾値未満のポリゴンを除外する。"""
    return gdf[gdf["area_m2"] >= threshold_m2].copy()


def _simplify_group(gdf, toposimplify):
    """Visvalingam-Whyatt で簡略化する。"""
    if len(gdf) == 0:
        return gdf
    topo = tp.Topology(gdf, toposimplify=toposimplify, topoquantize=False)
    return topo.to_gdf()


def _simplify_by_area(gdf, split_threshold, pct_small, pct_large):
    """面積に応じて簡略化率を分けて適用する。"""
    small = gdf[gdf["area_m2"] < split_threshold]
    large = gdf[gdf["area_m2"] >= split_threshold]

    results = []
    for subset, pct in [(small, pct_small), (large, pct_large)]:
        simplified = _simplify_group(subset, pct)
        if len(simplified) > 0:
            results.append(simplified)

    if not results:
        return gdf.iloc[:0]

    return gpd.GeoDataFrame(pd.concat(results, ignore_index=True), crs="EPSG:4326")


def model(dbt, session):
    dbt.config(materialized="table")

    raw = dbt.ref("raw_mlit_boundary")
    df = raw.df()

    gdf = _to_geodataframe(df)
    gdf = _compute_area_m2(gdf)
    gdf = _remove_small_islands(gdf, SMALL_ISLAND_THRESHOLD_M2)
    gdf = _simplify_by_area(
        gdf, AREA_SPLIT_THRESHOLD_M2, SIMPLIFY_PCT_SMALL, SIMPLIFY_PCT_LARGE
    )

    gdf = gdf.drop(columns=["area_m2"], errors="ignore")
    return gdf
