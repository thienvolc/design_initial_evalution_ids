# Phase 1: Offline Data Foundation

## Goal

Build a trustworthy offline raw-to-silver ingestion phase so later phases can rely on one cleaned parquet source of truth.

## Status

- completed

## Tasks

### Task 1: Raw Data Ingestion and Cleaning

- Status: done
- Goal: convert raw source files into a consistent parquet-based dataset
- What was done:
  - loaded raw CSV files from `data/raw/`
  - normalized `Timestamp` / `Label` columns to standard names
  - parsed timestamps and dropped corrupt / out-of-range timestamps
  - dropped null or empty labels
  - normalized labels using `configs/schema/label_mapping.yaml`
  - derived `label_norm` and binary target `label_binary`
  - replaced `Inf` / `-Inf` in feature columns with `NaN`
  - removed duplicate rows
  - attached `source_file` for traceability
- Outputs:
  - one cleaned silver parquet dataset:
    - `data/silver/ids2018_cleaned.parquet`
  - offline ingest log:
    - `logs/offline/phase01_ingest.log`
- Main code:
  - `src/ids_platform/offline/data_loading.py`
  - `src/ids_platform/offline/cleaning.py`

### Task 2: Stable Dataset Layout

- Status: done
- Goal: create a single stable silver dataset instead of re-reading raw CSVs in later phases
- What was done:
  - wrote one temp parquet per raw CSV, then merged them into one silver parquet
  - used memory-safe ingestion so phase 1 does not require one full in-memory concat
- Outputs:
  - merged silver dataset at `data/silver/ids2018_cleaned.parquet`
- Main code:
  - `src/ids_platform/offline/data_loading.py`

### Task 3: Canonical Phase Boundary

- Status: done
- Goal: make later phases consume silver parquet instead of raw files
- What was done:
  - Phase 1 ended at silver parquet creation
  - temporal train/valid/test split was left to Phase 2
- Outputs:
  - clean handoff boundary between:
    - Phase 1: raw -> silver
    - Phase 2: silver -> train/valid/test
- Main code:
  - `src/ids_platform/offline/pipeline.py`

## What Must Stay True

- downstream training and replay assume the parquet schema created here
- timestamp normalization is part of correctness, not cosmetic preprocessing
- Phase 1 output is the silver parquet, not the train/valid/test split
- label normalization and `label_binary` generation are part of the project contract

## Why This Phase Matters

If this phase changes carelessly, later phases may still run but train on different semantics or replay from inconsistent data without obvious failures.
