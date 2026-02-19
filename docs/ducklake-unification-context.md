# DuckLake Unification: Deprecating JSON Deploy + DuckDB-WASM Catalog Delivery

## Context

In the preceding refactoring (completed), generate_metadata() was changed to directly output
the DatasetMetadata format for the frontend, and the DuckLake→JSON conversion pipeline
(generate_catalog_from_ducklake, etc., 200+ lines) was removed.

Current pipeline:
```
manifest.json → generate_metadata() → datasets/{ds}/metadata.json → R2
                                              ↓
                                     catalog dbt → catalog.ducklake → R2
Frontend ← JSON (R2)
```

Issue: both metadata.json and catalog.ducklake are being deployed,
and the frontend reads JSON, so DuckLake is not being utilized.

## Approach

Make DuckLake the sole deployment target, with the frontend querying directly via DuckDB-WASM.
metadata.json remains as an intermediate file in the build pipeline but is not deployed to R2.

Pipeline after changes:
```
manifest.json → generate_metadata() → datasets/{ds}/metadata.json (intermediate, not deployed)
                                              ↓
                                     catalog dbt → catalog.ducklake → R2

SSG build:  catalog.ducklake → DuckDB (Node.js) → static page generation
Browser:    catalog.ducklake → DuckDB-WASM → search, filtering, etc.
```

## Current Architecture (Implemented)

### Build Pipeline (src/queria/)

- cli.py: `queria build` command. Phase 1 (datasource build) → Phase 2 (catalog dbt) → Phase 3 (R2 upload)
- build.py:
  - `build_datasource()`: dbt run → dbt docs generate → generate_metadata()
  - `build_catalog()`: catalog dbt's dbt run (generates catalog.ducklake)
- catalog.py:
  - `generate_metadata()`: manifest.json + dataset.yml → DatasetMetadata format metadata.json
  - `extract_models()`: extracts public models from manifest, groups TableInfo by schema
  - `extract_lineage()`: extracts DAG information from manifest
- models.py: Pydantic models (DatasetMetadata, SchemaInfo, TableInfo, ColumnInfo, etc.)
- upload.py:
  - `upload_metadata()`: for Phase 2. Uploads metadata.json to R2 (catalog dbt reads it via HTTP)
  - `upload_artifacts()`: for Phase 3. Uploads ducklake + metadata.json + dbt docs to R2

### catalog dbt Project (datasets/catalog/transform/)

raw layer: raw_tsukuba.sql, raw_e_stat.sql (reads metadata.json from R2 via read_json)
stg layer:
  - stg_catalog.sql: UNION ALL of raw tables
  - stg_models.sql: expands schemas.*.tables
  - stg_columns.sql: expands schemas.*.tables.*.columns
  - stg_datasets.sql, stg_dataset_schemas.sql, stg_dataset_dependencies.sql
  - stg_lineage_edges.sql, stg_lineage_nodes.sql
mart layer: mart_tables, mart_columns, mart_datasets, mart_schemas, mart_dependencies, mart_lineage_*

### DatasetMetadata Format (metadata.json)

```json
{
  "title": "つくば市オープンデータ",
  "description": "...",
  "tags": ["オープンデータ", "つくば市"],
  "ducklake_url": "https://...",
  "schemas": {
    "main": {
      "title": "メイン",
      "tables": [
        {
          "name": "mart_tsukuba_population",
          "title": "つくば市人口統計",
          "description": "...",
          "tags": [],
          "license": "CC BY 4.0",
          "license_url": "...",
          "source_url": "...",
          "type": "view",
          "columns": [{"name": "...", "description": "...", "data_type": "..."}],
          "sql": "SELECT ..."
        }
      ]
    }
  },
  "lineage": {
    "parent_map": {"model_a": ["model_b"]},
    "nodes": {"model_a": {"fqn": [...], "resource_type": "model", "config": {"materialized": "view"}, "meta": {...}}}
  },
  "dependencies": [{"alias": "...", "ducklake_url": "..."}]
}
```

### mart Tables (catalog DuckLake)

- mart_datasets: datasource, title, description, ducklake_url, tags_json
- mart_schemas: datasource, schema_name, title
- mart_tables: datasource, node_id, node_index, name, schema_name, description, type, title, license, license_url, source_url, is_public, tags_json, sql
- mart_columns: datasource, node_id, table_name, column_name, column_index, description, data_type
- mart_dependencies: datasource, alias, ducklake_url
- mart_lineage_edges: datasource, child, parent
- mart_lineage_nodes: datasource, name, resource_type, materialized, fqn_json, meta_json

## Changes (Backend)

### 1. upload.py: Deprecate metadata.json deployment

Remove metadata.json upload from upload_artifacts().
Only deploy catalog.ducklake (DuckLake file).

Before:
```python
upload_file(client, bucket, f"{ds}/metadata.json",
    datasets_dir / ds / "metadata.json", ...)
```
→ Remove this line

### 2. upload.py: Handling upload_metadata()

The Phase 2 (prd) process that temporarily uploads metadata.json to R2 needs to remain.
The catalog dbt raw layer reads metadata.json from R2 URLs via read_json.
However, it should be excluded from the final deployment in Phase 3.

### 3. Frontend Changes (Separate Session)

- Introduce DuckDB-WASM
- ATTACH catalog.ducklake (R2) and query mart tables
- During SSG build, execute the same queries using DuckDB Node.js bindings
- Replace existing JSON fetches with DuckDB queries

## Verification

1. `uv run queria build` completes successfully
2. Artifacts deployed to R2:
   - {ds}/ducklake.duckdb (DuckLake file)
   - {ds}/docs/* (dbt docs)
   - metadata.json is NOT deployed (upload_metadata is only for temporary use during build)
3. mart tables in catalog.ducklake contain data
