# Phase 5: Scale-Up, Reporting, and Operations

## Goal

Run larger profiles, produce report artifacts, and document how the system is operated.

## Status

- completed with drift risk

## Inputs

- matrix runners and report builder from phase 4
- profile definitions under:
  - `experiments/streaming/profiles/local_profiles.yaml`
  - `experiments/streaming/profiles/online_profiles.yaml`
- output artifacts under:
  - `artifacts/streaming/online/`
  - `artifacts/streaming/scale_up/`
  - `artifacts/streaming/benchmark/`

## Tasks

### Task 1: Local And Scale-Up Profile Families

- Status: done
- Goal: support both workstation-scale and heavier scale-up evaluation paths
- What was done:
  - added local profile families sized for the current machine
  - added heavier online/scale-up profiles with larger replay volumes
  - encoded run metadata such as `resource_class`, `mode`, `summary_csv`, and gate thresholds
  - kept Layer C as the host-orchestrated exception while most other profiles stay Docker-first
- Inputs:
  - matrix scripts from phase 4
  - `configs/streaming/online_scale_up.yaml`
- Outputs:
  - runnable profiles in:
    - `experiments/streaming/profiles/local_profiles.yaml`
    - `experiments/streaming/profiles/online_profiles.yaml`
- Main code and config:
  - `experiments/streaming/profiles/local_profiles.yaml`
  - `experiments/streaming/profiles/online_profiles.yaml`

### Task 2: Consolidated Evaluation Execution

- Status: done
- Goal: provide a single orchestration path for running major matrices and rebuilding the final report
- What was done:
  - added a one-shot full evaluation orchestrator
  - composed Layer A/B/C commands plus optional watermark/load-quality runs
  - added a final report build step after matrix execution
- Inputs:
  - matrix summary output paths
  - report output paths
- Outputs:
  - summary CSVs under `artifacts/streaming/online/`
  - report artifacts under `artifacts/streaming/online/`
- Main code:
  - `src/ids_platform/streaming/orchestration/full_evaluation.py`
  - `scripts/streaming/run_online_full_evaluation.py`
  - `scripts/streaming/build_online_report.py`

### Task 3: Regression Runbooks And Operator Flow

- Status: done
- Goal: turn evaluation into repeatable commands for day-to-day use
- What was done:
  - added PowerShell runbooks for light and heavier regression loops
  - documented execution flow in streaming README and runbook docs
  - separated canonical docs from some older historical docs, though drift still exists
- Inputs:
  - profile commands and artifact paths
- Outputs:
  - operator scripts under `scripts/runbooks/`
  - usage docs under `docs/` and `scripts/streaming/README.md`
- Main code and docs:
  - `scripts/runbooks/daily_light_regression.ps1`
  - `scripts/runbooks/daily_regression.ps1`
  - `scripts/runbooks/post_daily_light_regression.ps1`
  - `docs/execution_runbook.md`
  - `scripts/streaming/README.md`

### Task 4: Artifact Consumption UI

- Status: done
- Goal: make completed artifacts easy to inspect without re-running experiments
- What was done:
  - added Streamlit UI that discovers latest summary/report artifacts
  - supported both `online` and `scale_up` artifact directories
  - displayed summaries as post-run evidence, not live telemetry
- Inputs:
  - summary CSVs
  - report JSON/Markdown
- Outputs:
  - post-run evaluation dashboard behavior in Streamlit
- Main code:
  - `apps/streaming_dashboard.py`

## What Must Stay True

- profile YAMLs define execution plans; they are not implementation code
- artifact paths used by profiles, reports, runbooks, and dashboard must stay synchronized
- `online` and `scale_up` artifact families are both first-class in current repo behavior
- some docs in this area are canonical and some are historical; refactor must not treat them all the same

## Why This Phase Matters

This phase is where the repo became operationally usable, but also where drift risk increased the most. If profiles, runbooks, docs, and artifact paths are cleaned independently, the repo may look simpler while becoming harder to run correctly.

## Main Code And Docs

- `experiments/streaming/profiles/local_profiles.yaml`
- `experiments/streaming/profiles/online_profiles.yaml`
- `src/ids_platform/streaming/orchestration/full_evaluation.py`
- `scripts/streaming/run_online_full_evaluation.py`
- `scripts/streaming/build_online_report.py`
- `scripts/runbooks/`
- `apps/streaming_dashboard.py`
- `docs/execution_runbook.md`
- `scripts/streaming/README.md`
