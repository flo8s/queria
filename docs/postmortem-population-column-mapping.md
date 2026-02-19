# Postmortem: Column Mapping Inconsistency in Population by Area/Age Data

Date: 2026-02-09

## Summary

In the pipeline for ingesting Tsukuba city's "Population by Area/Age" open data, a column ordering error in the CSV definition caused values in four columns to be shifted. The same issue was reproduced across all 6 time points (April 2024 – October 2025) and all 303 areas.

## Impact

The following data was in an incorrect state:

- Population of females aged 85+ was entirely missing (1,818 rows)
- Population of males aged 85+ contained the household count values
- Household count contained the population of males aged 85+
- Total population was overstated by approximately 110,000 (372,325 vs. actual 261,771)

Column shift pattern:

| Column | Stored Value | Expected Value |
|---|---|---|
| Households | Males aged 85+ population | Households |
| Remarks | Females aged 85+ population | Remarks |
| Males aged 85+ | Households | Males aged 85+ population |
| Females aged 85+ | NULL (outside read range) | Females aged 85+ population |

## Root Cause

The issue was caused by the behavior of the `columns` parameter in DuckDB's `read_csv` function.

The `columns` parameter ignores the CSV header row and maps columns positionally in the specified order. The pipeline implementation placed `Households` and `Remarks` before the age-group columns (positions 9-10), but in the actual CSV they were placed after the age-group columns (positions 45-46).

Actual column order in the CSV:
```
..., Females 80-84, Males 85+, Females 85+, Households, Remarks
```

Column order assumed by the pipeline:
```
..., Females, Households, Remarks, Males 0-4, ..., Males 85+, Females 85+
```

As a result, all columns from position 9 onward were shifted by two, with the inconsistency surfacing at the last two columns (females aged 85+, households/remarks swap).

## Fix

Changed the CSV reading approach in the `read_population_csv` macro.

Before: explicitly specified names and types for all 46 columns positionally using the `columns` parameter
After: used the `dtypes` parameter to specify types only for columns with unstable type inference, with column names auto-detected from the CSV header

Columns with explicitly specified types:
- Local government code: VARCHAR (to preserve leading zeros)
- Area code: VARCHAR (same reason)
- Survey date: DATE
- Remarks: VARCHAR (unstable type inference for empty columns)

## Verification Results

After the fix, all of the following checks passed:

1. Rows for females aged 85+: 1,818 rows (for all area × date combinations)
2. Rows per area × date: 36 rows (18 age groups × 2 sexes) × 1,818 groups
3. Sum of age-group populations matches total population: exact match across all 6 time points
4. Average population per household: 2.25 (within a reasonable range)

## Prevention Measures

By switching from the `columns` parameter (position-based) to the `dtypes` parameter (header-based), CSV reading no longer depends on column order. Even if the CSV specification changes in the future, misalignment will not occur as long as column names are maintained.

## References

- Municipal Standard Open Data Set "Population by Area/Age": https://www.digital.go.jp/resources/open_data/municipal-standard-data-set-test
- Tsukuba City Open Data: https://www.city.tsukuba.lg.jp/soshikikarasagasu/shiminbuicthokensuishinka/gyomuannai/8/4/index.html
