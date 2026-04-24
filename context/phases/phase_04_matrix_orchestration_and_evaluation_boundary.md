# Phase 4: Matrix Orchestration and Evaluation Boundary

## Goal

Turn the streaming system into a repeatable evaluation platform.

## Status

- completed

## Inputs

- streaming runtime and replay behavior from phase 3
- profile YAMLs under:
  - `experiments/streaming/profiles/local_profiles.yaml`
  - `experiments/streaming/profiles/online_profiles.yaml`
- matrix script arguments and summary CSV output paths
- `ids.metrics` telemetry emitted by the SUT

## Tasks

### Task 1: Matrix Runner Layer

- Status: done
- Goal: turn experiment dimensions into repeatable scripts with structured outputs
- What was done:
  - implemented dedicated matrix entrypoints for:
    - Layer A
    - Layer B
    - Layer C
    - watermark
    - load-quality
    - benchmark
  - kept most streaming matrix logic in `src/ids_platform/streaming/matrices/`
  - kept benchmark orchestration in `src/ids_platform/streaming/orchestration/benchmark_runner.py`
  - made matrix runs write summary CSV artifacts instead of only printing logs
- Inputs:
  - replay + SUT execution path from phase 3
  - matrix-specific CLI/config arguments
- Outputs:
  - per-matrix summary CSVs under `artifacts/streaming/...`
- Main code:
  - `src/ids_platform/streaming/matrices/`
  - `src/ids_platform/streaming/orchestration/benchmark_runner.py`
  - `scripts/streaming/run_layer_a_matrix.py`
  - `scripts/streaming/run_layer_b_matrix.py`
  - `scripts/streaming/run_layer_c_matrix.py`
  - `scripts/streaming/run_watermark_matrix.py`
  - `scripts/streaming/run_load_quality_matrix.py`
  - `scripts/streaming/run_benchmark_matrix.py`

### Task 2: Profile Resolution and Gate Layer

- Status: done
- Goal: make experiment execution configurable, repeatable, and safe to invoke
- What was done:
  - added profile inheritance and deep-merge behavior
  - constrained profile scripts to allowed roots
  - added host/docker command building from profile config
  - added gate evaluation using summary CSV contents
  - added support for dry-run, gate-only, heavy-profile blocking, and list filters
- Inputs:
  - profile definitions from `experiments/streaming/profiles/*.yaml`
  - generated summary CSVs for gate evaluation
- Outputs:
  - resolved execution commands
  - gate verdicts based on summary CSV data
  - profile execution logs/events
- Main code:
  - `src/ids_platform/streaming/orchestration/profile_runner.py`
  - `src/ids_platform/streaming/orchestration/profile_service.py`
  - `scripts/streaming/run_online_profile.py`

### Task 3: Official Report Boundary

- Status: done
- Goal: make matrix summaries the official evidence path for conclusions and reporting
- What was done:
  - added report builder that reads matrix summary CSVs
  - computed layer-specific rollups from summary rows
  - produced consolidated JSON and Markdown reports
  - encoded the rule that official claims come from summaries/reports, not raw live telemetry
- Inputs:
  - summary CSVs from Layer A/B/C, watermark, and load-quality runs
- Outputs:
  - consolidated report JSON
  - consolidated report Markdown
- Main code:
  - `src/ids_platform/streaming/reporting/report_builder.py`
  - `scripts/streaming/build_online_report.py`

## What Must Stay True

- SUT emits debug telemetry, but official evaluation outputs are built later by matrices and reports
- profile/gate logic reads summary CSVs; it does not inspect the live stream directly as the final authority
- CLI scripts stay thin; orchestration/matrix logic belongs in `src/ids_platform/streaming/...`, not buried in scripts
- report builder consumes matrix summaries, not raw Prometheus/Grafana data

## Why This Phase Matters

This phase is where the repo stopped being “a streaming demo with scripts” and became an evaluation system. If this boundary is blurred during refactor, the project may still run but lose the ability to make clean, defensible claims.

## Main Code

- `src/ids_platform/streaming/matrices/`
- `src/ids_platform/streaming/orchestration/benchmark_runner.py`
- `src/ids_platform/streaming/orchestration/profile_runner.py`
- `src/ids_platform/streaming/orchestration/profile_service.py`
- `src/ids_platform/streaming/reporting/report_builder.py`
- `scripts/streaming/run_online_profile.py`
- `scripts/streaming/build_online_report.py`
