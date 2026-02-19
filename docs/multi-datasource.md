# Multi-Datasource Design

Architecture design for handling multiple datasources.

## Basic Model

1 datasource = 1 DuckLake.
Multiple datasources are stored in a single bucket using path prefixes.

The granularity of a datasource is not necessarily per city.
It may be appropriate to combine multiple cities into one datasource.

## R2 Bucket Structure

Datasources are separated within the bucket using path prefixes.

```
s3://queria-dev/
├── tsukuba/
│   ├── ducklake.duckdb            Metadata (DuckDB)
│   ├── ducklake.duckdb.files/     Parquet data (managed by DuckLake)
│   └── metadata.json               Catalog metadata
└── tsuchiura/
    ├── ducklake.duckdb
    ├── ducklake.duckdb.files/
    └── metadata.json
```

## dbt Project Structure

A single project with subdirectories for each datasource.
Shared macros are placed in the project root's `macros/` directory.

```
transform/
├── dbt_project.yml
├── profiles.yml
├── macros/
│   └── transform_population.sql
└── models/
    ├── tsukuba/
    │   ├── dataset.yml
    │   ├── raw/
    │   ├── stg/
    │   └── mart/
    └── tsuchiura/
        ├── dataset.yml
        ├── raw/
        ├── stg/
        └── mart/
```

Each datasource's catalog definition (`dataset.yml`) is placed in its subdirectory.

## Target-Model Binding

Specify `+database` per datasource in dbt_project.yml
to write to the corresponding DuckLake.

```yaml
models:
  transform:
    tsukuba:
      +database: tsukuba
    tsuchiura:
      +database: tsuchiura
```

Define per-datasource DuckLake in profiles.yml's attach section.
A single `dbt run` builds all datasources.

## Build and Deploy

`build.sh` manages all datasources in the `DATASOURCES` array and builds them in batch.

```bash
./scripts/build.sh                # dev build (all datasources)
./scripts/build.sh --target prd   # prd build + R2 upload
```

## Frontend Integration

The list of datasources is managed in the frontend (queria-web) code.
Each datasource's metadata.json URL is stored as a configuration and fetched.
When adding a datasource, update the frontend configuration and redeploy.

No centralized index.json is used.
This will be reconsidered when the number of datasources grows to the point where it becomes difficult to manage.

## Future Splitting Path

If datasources require different ingestion methods (e.g., dlt instead of dbt),
they can be split into independent repositories.

Contract for splitting:
- Output format: DuckLake + metadata.json (conforming to metadata-spec.md)
- Deployment target: separated by path prefix within the bucket

If shared macros are needed, extract them as a dbt package.
Currently, a single project is sufficient, so splitting is deferred until necessary.
