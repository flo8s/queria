-- 簡略化精度の検証: 簡略化後のポリゴン面積が元データと大きく乖離していないことを確認
--
-- 市区町村単位で ST_Union した面積を raw (簡略化前) と mart (簡略化後) で比較し、
-- 面積誤差率が閾値を超えるレコードがあればテスト失敗とする。
-- 返却行が 0 であればテスト成功。

WITH raw_areas AS (
    SELECT
        N03_007 AS lg_code_5,
        SUM(ST_Area_Spheroid(ST_MakeValid(geom))) AS raw_area
    FROM {{ ref('raw_mlit_boundary') }}
    WHERE N03_004 IS NOT NULL
      AND N03_004 != '所属未定地'
      AND N03_007 IS NOT NULL
      AND N03_007 != ''
    GROUP BY N03_007
),

simplified_areas AS (
    SELECT
        lg_code AS lg_code_5,
        ST_Area_Spheroid(geom) AS simplified_area
    FROM {{ ref('mart_municipality') }}
)

SELECT
    r.lg_code_5,
    r.raw_area,
    s.simplified_area,
    ABS(r.raw_area - s.simplified_area) / NULLIF(r.raw_area, 0) AS error_ratio
FROM raw_areas r
INNER JOIN simplified_areas s ON r.lg_code_5 = s.lg_code_5
WHERE ABS(r.raw_area - s.simplified_area) / NULLIF(r.raw_area, 0) > 0.01
