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
ATTACH 'ducklake:https://data.queria.io/tsukuba/ducklake.duckdb' AS tsukuba;
SELECT * FROM tsukuba.main.mart_tsukuba_population LIMIT 10;
```

Or:

```bash
duckdb "ducklake:https://data.queria.io/tsukuba/ducklake.duckdb" \
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
│   │   ├── pipeline.py        # Build entry point (dbt execution)
│   │   └── transform/         # dbt project
│   │       ├── models/
│   │       │   ├── raw/       # External CSV ingestion
│   │       │   ├── stg/       # Data transformation
│   │       │   └── mart/      # Public views
│   │       └── profiles.yml   # dbt profiles (dev/prd)
│   └── k_oxon/                # K-Oxon datasource (GIS + e-Stat)
│       ├── dataset.yml
│       ├── pipeline.py
│       └── transform/
├── src/
│   └── queria/                # DuckLake catalog management CLI
│       ├── cli.py             # CLI entry point (init, pull, push, metadata, gc)
│       ├── ducklake.py        # DuckLake catalog initialization
│       ├── pull.py            # S3 download
│       ├── push.py            # S3 upload / local copy
│       ├── metadata.py        # metadata.json generation
│       └── gc.py              # Orphaned Parquet file cleanup
└── pyproject.toml
```

### dbt Profiles

profiles.yml is included in each datasource's transform/ directory.

- dev target: writes Parquet locally
- prd target: writes directly to R2 S3 path (requires environment variables)

### Pipeline Execution

```bash
cd datasets/tsukuba

# Build a specific datasource locally
uv run python pipeline.py

# Production build + deploy
uv run queria pull
uv run python pipeline.py
uv run queria metadata
uv run queria push
```

Each dataset has a `pipeline.py` entry point that handles dbt execution.
The `queria` CLI manages DuckLake catalog operations (init, pull, push, metadata, gc).

The catalog dataset reads metadata from other datasources on R2, so run it last:

```bash
# 1. Build and push each dataset
cd datasets/tsukuba
uv run queria pull && uv run python pipeline.py && uv run queria metadata && uv run queria push

# 2. Build and push catalog last
cd datasets/catalog
uv run queria pull && uv run python pipeline.py && uv run queria metadata && uv run queria push
```

The `push` command and prd target require the following environment variables:

| Variable | Description | Example |
| --- | --- | --- |
| S3_ENDPOINT | S3-compatible endpoint | `<account_id>.r2.cloudflarestorage.com` |
| S3_ACCESS_KEY_ID | Access key | |
| S3_SECRET_ACCESS_KEY | Secret key | |
| S3_BUCKET | Bucket name | dev: `queria-dev` / prd: `queria` |

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
- k_oxon: K-Oxon data (GIS boundaries + e-Stat statistics)

## License

MIT
