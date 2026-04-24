# Phase 2: Silver To Gold Dataset Split

## Goal

Turn the cleaned silver dataset into train/valid/test splits that preserve temporal meaning and define the benchmark boundary for later phases.

## Status

- completed

## Inputs

- required input dataset:
  - `data/silver/ids2018_cleaned.parquet`
- split rules defined in code:
  - `TRAIN_DAYS`
  - `VALID_DAYS`
  - `TEST_DAYS`

## Tasks

### Task 1: Day-Block Split Strategy

- Status: done
- Goal: avoid optimistic leakage from mixing attack campaigns across time
- What was done:
  - assigned explicit calendar days to train, valid, and test
  - avoided intra-day splitting
  - used valid/test as temporal-shift evaluation sets
- Outputs:
  - split policy encoded in code:
    - `TRAIN_DAYS`
    - `VALID_DAYS`
    - `TEST_DAYS`
- Main code:
  - `src/ids_platform/offline/dataset_split.py`

### Task 2: Silver To Gold Split Materialization

- Status: done
- Goal: create reusable parquet splits for training, validation, testing, and streaming replay
- What was done:
  - read metadata first for per-day analysis
  - streamed the silver parquet in batches
  - wrote three parquet outputs directly without loading the full dataset at once
- Outputs:
  - `data/gold/splits/train.parquet`
  - `data/gold/splits/valid.parquet`
  - `data/gold/splits/test.parquet`
- Main code:
  - `src/ids_platform/offline/dataset_split.py`

### Task 3: Coverage Metadata

- Status: done
- Goal: make split composition explicit so later evaluation can be interpreted correctly
- What was done:
  - logged per-day attack coverage
  - recorded class distribution and attack-family coverage by split
  - marked unseen attack types in valid/test relative to train
- Outputs:
  - `data/gold/splits/split_metadata.yaml`
- Main code:
  - `src/ids_platform/offline/dataset_split.py`

### Task 4: Runtime Logging

- Status: done
- Goal: leave a readable execution trace for split coverage and row counts
- What was done:
  - wrote per-day and per-split stats to the offline split log during execution
- Outputs:
  - runtime log when phase 2 is executed:
    - `logs/offline/phase02_split.log`
- Main code:
  - `src/ids_platform/offline/dataset_split.py`

## What Must Stay True

- Phase 2 output is the gold split set, not model artifacts
- split semantics are part of benchmark correctness, not just data preparation
- `label` is preserved for diagnostics and per-attack analysis
- online task remains binary detection through `label_binary`
- `split_metadata.yaml` is the durable description of the split
- the log file is an execution artifact, not the core dataset contract

## Why This Phase Matters

If the split strategy changes, offline metrics, online replay difficulty, and paper conclusions can all shift even if the code still runs successfully.
