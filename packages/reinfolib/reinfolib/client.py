"""不動産情報ライブラリ API クライアント。"""

from typing import Any, Self

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from reinfolib.types import Municipality, RealEstatePrice

BASE_URL = "https://www.reinfolib.mlit.go.jp/ex-api/external"


class ReinfolibClient:
    """不動産情報ライブラリ API の HTTP クライアント。

    リトライ (指数バックオフ) を内部で処理する。
    """

    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._session.headers["Ocp-Apim-Subscription-Key"] = api_key
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    def close(self) -> None:
        """セッションを閉じる。"""
        self._session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # -- XIT001: 不動産価格情報 --

    def get_real_estate_prices(
        self,
        *,
        year: int,
        quarter: int | None = None,
        area: str | None = None,
        city: str | None = None,
        station: str | None = None,
        price_classification: str | None = None,
        language: str | None = None,
    ) -> list[RealEstatePrice]:
        """XIT001: 不動産価格 (取引価格・成約価格) 情報を取得する。"""
        params: dict[str, str] = {"year": str(year)}
        if quarter is not None:
            params["quarter"] = str(quarter)
        if area is not None:
            params["area"] = area
        if city is not None:
            params["city"] = city
        if station is not None:
            params["station"] = station
        if price_classification is not None:
            params["priceClassification"] = price_classification
        if language is not None:
            params["language"] = language
        return self._get("/XIT001", params).get("data", [])

    # -- XIT002: 市区町村一覧 --

    def get_municipalities(
        self,
        area: str,
        *,
        language: str | None = None,
    ) -> list[Municipality]:
        """XIT002: 都道府県内の市区町村一覧を取得する。"""
        params: dict[str, str] = {"area": area}
        if language is not None:
            params["language"] = language
        return self._get("/XIT002", params).get("data", [])

    # -- XCT001: 鑑定評価書情報 --

    def get_appraisal_reports(
        self,
        *,
        year: int,
        area: str,
        division: str,
    ) -> list[dict[str, Any]]:
        """XCT001: 地価公示の鑑定評価書情報を取得する。"""
        params: dict[str, str] = {
            "year": str(year),
            "area": area,
            "division": division,
        }
        return self._get("/XCT001", params).get("data", [])

    # -- XPT001: 不動産価格ポイント --

    def get_real_estate_prices_point(
        self,
        *,
        z: int,
        x: int,
        y: int,
        period_from: int,
        period_to: int,
        price_classification: str | None = None,
        land_type_code: list[str] | None = None,
    ) -> dict[str, Any]:
        """XPT001: 不動産価格のポイントデータを GeoJSON で取得する。"""
        params: dict[str, str] = {
            "z": str(z),
            "x": str(x),
            "y": str(y),
            "from": str(period_from),
            "to": str(period_to),
        }
        if price_classification is not None:
            params["priceClassification"] = price_classification
        if land_type_code is not None:
            params["landTypeCode"] = ",".join(land_type_code)
        return self._get("/XPT001", params)

    # -- XPT002: 地価公示・地価調査ポイント --

    def get_land_prices_point(
        self,
        *,
        z: int,
        x: int,
        y: int,
        year: int,
        price_classification: str | None = None,
        use_category_code: list[str] | None = None,
    ) -> dict[str, Any]:
        """XPT002: 地価公示・地価調査のポイントデータを GeoJSON で取得する。"""
        params: dict[str, str] = {
            "z": str(z),
            "x": str(x),
            "y": str(y),
            "year": str(year),
        }
        if price_classification is not None:
            params["priceClassification"] = price_classification
        if use_category_code is not None:
            params["useCategoryCode"] = ",".join(use_category_code)
        return self._get("/XPT002", params)

    # -- internal --

    def _get(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        """HTTP GET を実行する。リトライは urllib3.Retry に委譲。"""
        url = f"{BASE_URL}{endpoint}"
        resp = self._session.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            return {"data": []}
        resp.raise_for_status()
        return resp.json()
