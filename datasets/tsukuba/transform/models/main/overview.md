{% docs __overview__ %}

# Tsukuba City Open Data

A dataset of statistical data published by Tsukuba city, transformed and organized using dbt.

## Data Source

- Provider: [Tsukuba City](https://www.city.tsukuba.lg.jp/)
- Data format: [Municipal Standard Open Data Set "Population by Area/Age"](https://www.digital.go.jp/resources/open_data/municipal-standard-data-set-test)
- License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- Coverage period: April 2024 – October 2025 (6 time points)

## Model Structure

### raw: Raw Data Ingestion

Ingests CSV files (Shift_JIS) published by Tsukuba city for each time point.
Column names are preserved in Japanese as-is.

| Model | Time Point |
|---|---|
| `raw_tsukuba_population_20240401` | April 1, 2024 |
| `raw_tsukuba_population_20240501` | May 1, 2024 |
| `raw_tsukuba_population_20241001` | October 1, 2024 |
| `raw_tsukuba_population_20250401` | April 1, 2025 |
| `raw_tsukuba_population_20250501` | May 1, 2025 |
| `raw_tsukuba_population_20251001` | October 1, 2025 |

### stg: Normalization

UNPIVOTs raw data from each time point into a long format with one row per sex and age group.
Column names are renamed to English.

Output columns: `lg_code`, `area_code`, `lg_name`, `reference_date`, `area_name`, `sex`, `age_group`, `population`

### mart: Unified Table

A view that combines all time points from stg models using UNION ALL.

| Model | Description |
|---|---|
| `mart_tsukuba_population` | Mart table unifying Tsukuba city population data across all periods |

## Lineage

Click the blue icon in the bottom right to view the lineage graph.

{% enddocs %}
