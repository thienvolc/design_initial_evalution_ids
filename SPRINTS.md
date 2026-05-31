# Sprints & Checklists

## Current Sprint: Remove Streamlit
- [ ] Delete `apps/streaming_dashboard.py`
- [ ] Remove `streamlit` from `requirements.txt`
- [ ] Remove `streamlit` from `docker/base/requirements.txt`
- [ ] Remove streamlit comments from `scripts/runbooks/post_daily_light_regression.ps1`
- [ ] Remove streamlit references from `docs/execution_runbook.md`
- [ ] Remove streamlit references from `docs/component_guide.md`

## Refactor Roadmap: Maintainable Streaming IDS Harness

### Phase 0: Architecture Audit and Refactor Boundary
- [ ] Freeze the refactor goal: keep the project as a reproducible streaming IDS harness, not a broad research platform.
- [ ] Classify current paths as official, secondary, ablation, or legacy.
- [ ] Map the current input -> handler -> output flow for offline and streaming.
- [ ] Identify public CLI contracts that must remain stable during refactor.
- [ ] Identify artifact contracts that must remain stable during refactor.
- [ ] Record dependency hotspots from code-review-graph and AST import audit.
- [ ] Decide the first handler boundary to refactor before changing profile/config inputs.

### Phase 1: Streaming Handler Cleanup
- [ ] Keep existing CLI flags and YAML profile inputs unchanged.
- [ ] Extract a shared trial handler for the repeated matrix flow: build run context, optional warmup, stream/replay execution, metrics collection, timeseries write, summary row materialization.
- [ ] Refactor `layer_a_matrix.py` to call the shared trial handler while preserving output CSV columns.
- [ ] Refactor `layer_b_matrix.py` to call the shared trial handler while preserving output CSV columns.
- [ ] Refactor `load_quality_matrix.py` only after A/B prove the shared handler is stable.
- [ ] Leave `watermark_matrix.py` untouched until the main handler abstraction has settled.
- [ ] Add focused tests for trial planning, command construction, summary row preservation, and missing-metrics behavior.
- [ ] Run smoke/dry verification without requiring Kafka where possible.

### Phase 2: SUT Runtime Decomposition
- [ ] Keep `run_structured_streaming_job(options)` as the public entrypoint.
- [ ] Move Kafka topic setup and metrics publishing out of `structured_streaming_job.py`.
- [ ] Move Spark query lifecycle helpers out of `structured_streaming_job.py`.
- [ ] Move shutdown/sentinel handling out of `structured_streaming_job.py`.
- [ ] Move batch statistics and runtime metric payload construction out of `structured_streaming_job.py`.
- [ ] Verify SUT CLI help, runtime artifact tests, and at least one container smoke run.

### Phase 3: Common Config and Dependency Cleanup
- [ ] Move generic YAML/JSON helpers from offline-specific modules to `ids_platform.common`.
- [ ] Remove streaming imports of `ids_platform.offline.config`.
- [ ] Separate offline artifact reading from streaming runtime config loading.
- [ ] Keep artifact file paths backward compatible.
- [ ] Add tests for config loading and artifact path resolution.

### Phase 4: Input/Profile Simplification
- [ ] Simplify `experiments/streaming/profiles/local_profiles.yaml` around a small official local path.
- [ ] Mark heavy, diagnostic, and exploratory profiles clearly as ablation or legacy.
- [ ] Decide whether `experiments/streaming/benchmark.yaml` remains secondary or moves to archive.
- [ ] Decide whether watermark remains diagnostic instead of main report path.
- [ ] Update docs to explain only the official path first, then secondary paths.

### Phase 5: Output and Report Contract Cleanup
- [ ] Standardize summary CSV naming and output directories for official runs.
- [ ] Standardize timeseries and runtime log locations.
- [ ] Add a small report-input manifest that maps report tables/figures to artifact files.
- [ ] Keep paper plot scripts downstream of artifacts, not part of the benchmark handler.
- [ ] Add verification for report-input files when summaries exist.

## Phase 1 Detailed Plan: Streaming Handler Cleanup

### Goal
- [ ] Reduce duplicated orchestration logic in Layer A, Layer B, and load-quality matrices without changing user-facing commands or existing profile YAML.
- [ ] Make the handler layer explicit before changing inputs.
- [ ] Preserve current summary CSV schemas so existing paper scripts and tests do not break.

### Non-Goals
- [ ] Do not rename Layer A/B/load files in this phase.
- [ ] Do not change `local_profiles.yaml` or `streaming_profiles.yaml` semantics in this phase.
- [ ] Do not modify `structured_streaming_job.py` in this phase unless a test reveals a blocking bug.
- [ ] Do not remove watermark, Layer C, benchmark, or paper scripts in this phase.

### Step 1: Baseline the Current Flow
- [ ] Read `layer_a_matrix.py`, `layer_b_matrix.py`, `load_quality_matrix.py`, and throughput helpers.
- [ ] Document the repeated flow in code comments or a short internal note before editing.
- [ ] Capture current summary columns for Layer A, Layer B, and load-quality outputs.
- [ ] Run existing unit tests that do not require Kafka/Spark runtime dependencies.
- [ ] Record current failing tests caused by missing environment dependencies separately from refactor failures.

### Step 2: Introduce Shared Trial Concepts
- [ ] Add a small internal dataclass for trial identity: run tag, repeat index, run index, profile/model/feature labels.
- [ ] Add a small internal dataclass for stream/replay commands.
- [ ] Add a shared function for optional warmup execution.
- [ ] Add a shared function for main stream/replay execution.
- [ ] Add a shared function for metrics collection plus timeseries materialization.
- [ ] Keep row-building delegated to existing row builders or matrix-specific functions.

### Step 3: Refactor Layer A First
- [ ] Replace only the duplicated execution body inside `layer_a_matrix.run`.
- [ ] Keep `_parse_profiles`, `_aggregate_row`, and `_reported_model_label` behavior unchanged.
- [ ] Verify generated command arguments match the pre-refactor flow.
- [ ] Verify Layer A summary column names remain unchanged.
- [ ] Run focused tests and `run_layer_a_matrix.py --help`.

### Step 4: Refactor Layer B Second
- [ ] Reuse the shared trial handler with model/feature pair identity.
- [ ] Keep `_build_model_feature_pairs` behavior unchanged.
- [ ] Keep `build_layer_b_summary_row` output unchanged.
- [ ] Verify generated command arguments match the pre-refactor flow.
- [ ] Run focused tests and `run_layer_b_matrix.py --help`.

### Step 5: Decide Whether Load-Quality Is Ready
- [ ] Compare load-quality flow against the shared handler after A/B refactor.
- [ ] If the abstraction fits cleanly, refactor `load_quality_matrix.py`.
- [ ] If it requires special overload behavior, extract only the safe common pieces and leave scenario-specific code local.
- [ ] Keep load-quality output schema unchanged.

### Step 6: Verification Gate
- [ ] Run syntax checks for touched modules.
- [ ] Run unit tests for profile runner, benchmark contract, matrix helpers, and entrypoint help where dependencies are installed.
- [ ] Run dry command construction checks where possible.
- [ ] If Docker/Kafka is available, run the smallest smoke profile.
- [ ] Update `SPRINTS.md` checkboxes as each step completes.
