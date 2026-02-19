# queria

An open data publishing platform using dbt + DuckLake + Cloudflare R2.

Ingests open data, transforms it, and publishes it as Frozen DuckLake on R2.

## Architecture

```
Open Data (CSV)
    ↓
dbt-duckdb (local)
    ↓
DuckLake (Parquet + metadata)
    ↓
Cloudflare R2 (public)
    ↓
DuckDB WASM / CLI (query)
```

## Quick Start

### Access Published Data

From DuckDB CLI:

```sql
ATTACH 'ducklake:https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev/tsukuba/ducklake.duckdb' AS tsukuba;
SELECT * FROM tsukuba.main.mart_tsukuba_population LIMIT 10;
```

Or:

```bash
duckdb "ducklake:https://pub-0292714ad4094bd0aaf8d36835b0972a.r2.dev/tsukuba/ducklake.duckdb" \
    -c "SELECT COUNT(*) FROM mart_tsukuba_population"
```

DuckDB WASM can access the data in the same way.

## Development

### Prerequisites

- Python 3.13+
- uv

### Setup

```bash
# Install dependencies
uv sync
```

### Project Structure

```
queria/
├── datasets/
│   ├── tsukuba/               # Tsukuba city datasource
│   │   ├── dataset.yml        # Dataset metadata definition
│   │   └── transform/         # dbt project
│   │       ├── models/
│   │       │   ├── raw/       # External CSV ingestion
│   │       │   ├── stg/       # Data transformation
│   │       │   └── mart/      # Public views
│   │       └── profiles.yml   # dbt profiles (dev/prd)
│   └── e_stat/                # e-Stat datasource
│       ├── dataset.yml
│       └── transform/
├── src/
│   └── queria/                # Build & deploy CLI tool
│       ├── cli.py             # CLI entry point
│       ├── run.py             # DuckLake init + dbt execution + metadata generation
│       ├── freeze.py          # R2 upload / local copy
│       └── models.py          # Pydantic model definitions for catalog output
└── pyproject.toml
```

### dbt Profiles

profiles.yml is included in each datasource's transform/ directory.

- dev target: writes Parquet locally
- prd target: writes directly to R2 S3 path (requires environment variables)

### Pipeline Execution

```bash
# Build a specific datasource locally
uv run queria run datasets/tsukuba

# Production build + deploy
uv run queria run datasets/tsukuba --target prd
uv run queria freeze datasets/tsukuba --bucket queria-dev

# Freeze to local directory
uv run queria freeze datasets/tsukuba --output-dir ./out
```

The pipeline is split into two commands: `run` and `freeze`:
- `queria run <path>`: DuckLake init -> dbt deps/run/docs -> metadata generation. Does not touch R2
- `queria freeze <path>`: Uploads to R2 or copies locally

The catalog dataset reads metadata from other datasources on R2, so run it last:

```bash
uv run queria run datasets/tsukuba --target prd
uv run queria run datasets/e_stat --target prd
uv run queria freeze datasets/tsukuba
uv run queria freeze datasets/e_stat
uv run queria run datasets/catalog --target prd
uv run queria freeze datasets/catalog
```

The `freeze` command and prd target require the following environment variables:

| Variable | Description | Example |
| --- | --- | --- |
| S3_ENDPOINT | S3-compatible endpoint | `<account_id>.r2.cloudflarestorage.com` |
| S3_ACCESS_KEY_ID | Access key | |
| S3_SECRET_ACCESS_KEY | Secret key | |
| S3_BUCKET | Bucket name | queria-dev |

### R2 CORS Configuration

When accessing from DuckDB WASM, the R2 bucket requires CORS configuration:

```json
[
  {
    "AllowedOrigins": ["http://localhost:3000"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["*"]
  }
]
```

## Datasources

- tsukuba: Tsukuba city open data (population by area)
- e_stat: e-Stat (Japanese government statistics)

## License

MIT
