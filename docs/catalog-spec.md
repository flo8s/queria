# catalog.json スキーマ仕様

catalog.json はデータカタログのメタデータファイル。
`_catalog.yml` と dbt の `manifest.json` から `build_catalog.py` で生成される。

## 構造

```
catalog.json
├── title          カタログ名
├── description    カタログの説明
├── tags[]         分類タグ
├── ducklake_url   DuckLake エンドポイント URL
└── schemas
    └── {schema_name}
        ├── title      スキーマ名
        └── tables[]
            ├── name        テーブル識別子 (dbt model 名)
            ├── title       テーブル表示名
            ├── description テーブルの説明
            ├── tags[]      分類タグ
            ├── license     ライセンス
            ├── license_url ライセンスの URL
            ├── source_url  データソースの URL
            └── columns[]
                ├── name        カラム名
                └── description カラムの説明
```

## フィールド定義

### トップレベル

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| title | string | Yes | カタログの表示名 |
| description | string | Yes | カタログの説明文 |
| tags | string[] | No | 分類用タグ |
| ducklake_url | string | Yes | DuckLake エンドポイント URL |
| schemas | object | Yes | スキーマ名をキーとしたオブジェクト |

### schemas.{name}

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| title | string | Yes | スキーマの表示名 |
| tables | object[] | Yes | テーブルの配列 |

### schemas.{name}.tables[]

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| name | string | Yes | テーブル識別子 (dbt model 名と一致) |
| title | string | Yes | テーブルの表示名 |
| description | string | Yes | テーブルの説明文 |
| tags | string[] | No | 分類用タグ |
| license | string | No | ライセンス (例: "CC BY 4.0") |
| license_url | string | No | ライセンスの URL |
| source_url | string | No | データソースの URL |
| columns | object[] | Yes | カラムの配列 |

### columns[]

| フィールド | 型 | 必須 | 説明 |
|---|---|---|---|
| name | string | Yes | カラム名 |
| description | string | Yes | カラムの説明文 |

## データソース

- トップレベル / スキーマ: `transform/models/_catalog.yml` から取得
- テーブル / カラム: dbt の `manifest.json` (各 model の `meta` と `columns`) から取得
- `meta.public: true` のモデルのみカタログに含まれる

## 設計判断

1データソース = 1 DuckLake の前提 (詳細は multi-datasource.md):
- 1 catalog.json = 1データソース → `databases` ラッパーは不要、トップレベルに直接配置
- 多言語対応は後回し → `{ja: ..., en: ...}` ではなくプレーンな文字列
- `display_name` → DCAT の `dct:title` にマッピングしやすい `title` に統一
- `meta` のネストを廃止 → テーブルごとに `title`, `tags`, `license` 等をフラットに配置
- 単一ファイル構成 → データ量が少ないうちは分割不要

## 将来の拡張パス

- 多言語対応: `title` を `title: {ja: "...", en: "..."}` に拡張
- DCAT 変換: catalog.json → DCAT (JSON-LD) への変換スクリプトを追加
- ファイル分割: テーブル数が増えた場合、スキーマ単位で分割
- CMS 化: catalog.json を CMS (Headless CMS 等) から生成する構成

## フィールド追加手順

1. `_catalog.yml` または dbt model の `meta` に新フィールドを追加
2. `build_catalog.py` の `extract_models()` / `build_catalog()` でフィールドを処理
3. このドキュメントのフィールド定義テーブルを更新
4. `uv run python scripts/build_catalog.py` で生成を確認
