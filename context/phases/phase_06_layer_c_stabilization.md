# Phase 6: Layer C Stabilization

## Goal

Stabilize Layer C so fault-recovery results are trustworthy enough for reruns, scale-up profiles, and paper-ready reporting.

## Status

- active

## Inputs

- Layer C matrix runner:
  - `scripts/streaming/run_layer_c_matrix.py`
- Layer C runtime code:
  - `src/ids_platform/streaming/matrices/layer_c_fault_matrix.py`
  - `src/ids_platform/streaming/orchestration/fault_matrix.py`
  - `src/ids_platform/streaming/runtime/structured_streaming_job.py`
- Layer C profiles:
  - `experiments/streaming/profiles/local_profiles.yaml`
  - `experiments/streaming/profiles/online_profiles.yaml`
- Layer C operational history and rationale:
  - `docs/adr/ADR-0001-layer-c-stabilization.md`

## Why This Phase Exists

Layer C exposed orchestration and runtime correctness problems that were not mainly model-quality problems:

- host vs container Kafka bootstrap mismatch
- checkpoint ownership races
- false readiness from reused logs
- ambiguous terminal metrics
- shutdown noise and forced stop behavior
- missing or misleading timeseries values

## Main Tasks

### Task 1: Host And Docker Orchestration Alignment

- Status: done
- Goal: make Layer C runnable in its hybrid execution model without host/container Kafka mismatches
- What was done:
  - kept Layer C orchestration on host for fault injection
  - ran SUT and replay subprocesses with `execution_mode=docker`
  - added host-vs-docker bootstrap resolution for runtime control and post-run metric collection
  - added Kafka restart readiness checks for host bootstrap, docker bootstrap, and topic availability
- Outputs:
  - Layer C can run as host-orchestrated + docker-backed instead of failing on bootstrap mismatch
- Main code:
  - `src/ids_platform/streaming/matrices/layer_c_fault_matrix.py`
  - `src/ids_platform/streaming/orchestration/fault_matrix.py`

### Task 2: Startup, Recovery, and Metric Validation

- Status: done
- Goal: prevent false success and false failure during warmup and post-fault recovery
- What was done:
  - added per-run runtime log capture under `logs/streaming/runtime/<run_tag>.log`
  - hardened readiness detection against reused log content
  - validated warmup/recovery metrics before accepting a scenario as `ok`
  - rejected terminal marker payloads as proof of recovery
  - increased run-tag uniqueness to avoid checkpoint/log collisions
- Outputs:
  - more trustworthy warmup/recovery state transitions
  - runtime logs per Layer C scenario run
- Main code:
  - `src/ids_platform/streaming/matrices/layer_c_fault_matrix.py`
  - `src/ids_platform/streaming/matrices/common.py`
  - `src/ids_platform/common/subprocess.py`

### Task 3: Restart And Shutdown Semantics

- Status: done
- Goal: reduce checkpoint races and shutdown-side corruption during restart scenarios
- What was done:
  - added cooperative shutdown request flow for docker-backed stream processes
  - improved `spark_process_restart` quiescence handling before restart
  - changed Layer C to use input sentinel more intentionally
  - tightened graceful-stop detection so sentinel receipt alone is not treated as full shutdown
  - extended graceful wait before external fallback stop
- Outputs:
  - fewer checkpoint ownership races
  - less false restart success
  - cleaner end-of-scenario shutdown behavior
- Main code:
  - `src/ids_platform/streaming/runtime/control.py`
  - `src/ids_platform/streaming/orchestration/fault_matrix.py`
  - `src/ids_platform/streaming/runtime/structured_streaming_job.py`

### Task 4: Timeseries And Reporting Semantics

- Status: done with follow-up risk
- Goal: make Layer C timeseries and summaries interpretable instead of cosmetically complete but misleading
- What was done:
  - clarified missing-value behavior for metrics that are genuinely undefined
  - improved downstream handling of metric warnings and timeseries semantics
  - exported per-run Layer C metrics timeseries CSVs
- Outputs:
  - `artifacts/streaming/metrics_timeseries/<run_tag>.csv`
  - cleaner interpretation of Layer C summary and timeseries artifacts
- Main code:
  - `src/ids_platform/streaming/reporting/report_builder.py`
  - `scripts/streaming/build_timeseries_plots.py`

## What Matters

- Layer C failures were mostly orchestration/runtime issues, not model-quality issues
- warnings during fault windows are not automatically scenario failures
- some messy behavior exists because fault injection is controlled from host
- host orchestration for Layer C is intentional, not accidental technical debt
- missing values in timeseries can be the correct representation, not a bug by default

## Outputs

- Layer C summary CSVs, for example:
  - `artifacts/streaming/scale_up/layer_c_summary_local_medium.csv`
  - `artifacts/streaming/scale_up/layer_c_summary_700k_fault.csv`
- per-run Layer C runtime logs:
  - `logs/streaming/runtime/<run_tag>.log`
- per-run Layer C timeseries CSVs:
  - `artifacts/streaming/metrics_timeseries/<run_tag>.csv`
- architectural rationale:
  - `docs/adr/ADR-0001-layer-c-stabilization.md`

## Main Code

- `src/ids_platform/streaming/matrices/layer_c_fault_matrix.py`
- `src/ids_platform/streaming/orchestration/fault_matrix.py`
- `src/ids_platform/streaming/runtime/structured_streaming_job.py`
- `src/ids_platform/streaming/reporting/report_builder.py`
- `docs/adr/ADR-0001-layer-c-stabilization.md`

## Refactor Notes

- `kafka_restart` still produces transient warning windows that must be interpreted carefully
- do not remove host orchestration blindly
- do not fake missing metrics to make plots prettier
- keep ADR-0001 in mind before cleaning Layer C code
