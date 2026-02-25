# queria

dbt + DuckLake + Cloudflare R2 によるオープンデータ公開パイプライン。

オープンデータを取り込み、dbt で変換し、Frozen DuckLake として R2 に公開する。
公開されたデータは DuckDB CLI や DuckDB WASM からSQLで直接クエリできる。

## アーキテクチャ

```
Open Data (CSV/JSON)
    ↓
dbt-duckdb (ローカル)
    ↓
DuckLake (Parquet + メタデータ)
    ↓
Cloudflare R2 (公開)
    ↓
DuckDB WASM / CLI (クエリ)
```

## 公開データへのアクセス

DuckDB CLI から直接クエリできる:

```sql
ATTACH 'ducklake:https://data.queria.io/tsukuba/ducklake.duckdb' AS tsukuba;
SELECT * FROM tsukuba.main.mart_tsukuba_population LIMIT 10;
```

```bash
duckdb "ducklake:https://data.queria.io/tsukuba/ducklake.duckdb" \
    -c "SELECT COUNT(*) FROM mart_tsukuba_population"
```

DuckDB WASM でも同じ URL でアクセスできる。

## 公開中のデータセット

| データセット | 説明 |
|---|---|
| tsukuba | つくば市オープンデータ（人口統計） |
| zipcode | 日本郵便 郵便番号データ |
| e_stat | e-Stat（政府統計） |
| articles | 記事メタデータ |
| catalog | 全データセットの統合カタログ |
