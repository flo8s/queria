"""不動産情報ライブラリ API の型定義。"""

from enum import StrEnum, unique
from typing import TypedDict


@unique
class PriceClassification(StrEnum):
    """XIT001 / XPT001 の価格分類。"""

    TRADE_PRICE = "01"
    CONTRACT_PRICE = "02"


@unique
class UseDivision(StrEnum):
    """XCT001 / XPT002 の用途区分。"""

    RESIDENTIAL_LAND = "00"
    BUILDING_SITE_WITH_INTERIM_USE = "03"
    COMMERCIAL_LAND = "05"
    QUASI_INDUSTRIAL_LAND = "07"
    INDUSTRIAL_LAND = "09"
    URBANIZATION_CONTROL_AREA = "10"
    CURRENT_FOREST_LAND = "13"
    FOREST_LAND = "20"


@unique
class LandTypeCode(StrEnum):
    """XPT001 の土地種別。"""

    LAND = "01"
    LAND_AND_BUILDING = "02"
    PRE_OWNED_CONDOMINIUMS = "07"
    AGRICULTURAL_LAND = "10"
    FOREST_LAND = "11"


# -- レスポンス型 --


class Municipality(TypedDict):
    """XIT002 市区町村。"""

    id: str
    name: str


class RealEstatePrice(TypedDict, total=False):
    """XIT001 不動産価格情報。

    取引種別によって含まれるフィールドが異なるため total=False。
    """

    Type: str
    Region: str
    MunicipalityCode: str
    Prefecture: str
    Municipality: str
    DistrictName: str
    TradePrice: str
    PricePerUnit: str
    FloorPlan: str
    Area: str
    UnitPrice: str
    LandShape: str
    Frontage: str
    TotalFloorArea: str
    BuildingYear: str
    Structure: str
    Use: str
    Purpose: str
    Direction: str
    Classification: str
    Breadth: str
    CityPlanning: str
    CoverageRatio: str
    FloorAreaRatio: str
    Period: str
    Renovation: str
    Remarks: str
    PriceCategory: str
    DistrictCode: str
