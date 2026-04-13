# Project Overview

- Hybrid IDS project with two main parts:
  - Offline pipeline to clean data, split datasets, train classical ML models, and evaluate them.
  - Streaming evaluation platform to replay parquet traces into Kafka, score them with Spark Structured Streaming, run experiment matrices, and build evaluation reports.
- Primary use cases:
  - Train offline binary intrusion-detection models from CSE-CIC-IDS2018-derived parquet data.
  - Run controlled streaming experiments against Kafka-backed replay traffic.
  - Compare runtime/system configurations (Layer A), model/feature combinations (Layer B), fault recovery (Layer C), watermark handling, load-quality tradeoffs, and pandas UDF benchmarks.
  - Present completed evaluation results in Streamlit and monitor live runtime telemetry in Prometheus/Grafana.
- Tech stack:
  - Python 3
  - Spark / PySpark
  - Kafka + Zookeeper
  - pandas, NumPy, PyArrow
  - scikit-learn + joblib
  - PyYAML
  - Streamlit
  - Prometheus + Grafana
  - Docker Compose

# Architecture

- High-level architecture:
  - Modular monorepo with one Python package: `src/ids_platform`
  - Thin CLI scripts in `scripts/`
  - Docker-first runtime for streaming and observability
- Main subsystems:
  - Offline ML pipeline: train models and write artifacts consumed by streaming.
  - Streaming runtime: Kafka input -> Spark Structured Streaming scoring -> prediction outputs + debug metrics.
  - Matrix summary path: matrix runners currently aggregate `ids.metrics` into official summary rows.
  - Replay subsystem: parquet trace -> Kafka producer for synthetic online traffic.
  - Experiment orchestration: profiles, matrices, benchmark plans, report generation.
  - Presentation/observability:
    - Streamlit for post-run evaluation results
    - Prometheus/Grafana for live runtime/debug telemetry
- Key design patterns:
  - Thin entrypoints / fat `src` modules
  - Dataclass-based options objects for runtime and orchestration services
  - YAML-driven configuration and experiment profiles
  - Shared artifact registry / path resolution helpers
  - Subprocess orchestration for experiment pipelines
- Data flow overview:
  - Offline:
    - raw/silver/gold parquet -> cleaning -> split -> train -> evaluate -> model artifacts + metrics CSV + feature manifest
  - Streaming:
    - replay parquet -> Kafka input topic (`ids.raw.flows`) -> Structured Streaming scorer -> predictions parquet/Kafka + metrics topic (`ids.metrics`)
  - Evaluation:
    - matrix/orchestration scripts -> `ids.metrics`-derived summary CSVs under `artifacts/streaming/...` -> report builder -> Streamlit UI
  - Observability:
    - live Kafka metrics topic -> Prometheus exporter -> Prometheus -> Grafana

# Folder & Module Structure

- `src/ids_platform/common`
  - Shared config/path/subprocess helpers.
- `src/ids_platform/offline`
  - Offline data cleaning, loading, split, training, evaluation, metrics, and pipeline orchestration.
- `src/ids_platform/streaming/runtime`
  - Structured Streaming runtime, scoring helpers, Spark query helpers, benchmark job.
- `src/ids_platform/streaming/replay`
  - Replay config parsing and parquet-to-Kafka replay runtime.
- `src/ids_platform/streaming/matrices`
  - Layer A/B/C, watermark, and load-quality experiment implementations.
- `src/ids_platform/streaming/orchestration`
  - Profile resolution, full evaluation orchestration, benchmark runner, fault helpers, profile service.
- `src/ids_platform/streaming/reporting`
  - Summary readers, report builder, metrics readers.
- `src/ids_platform/streaming/metrics`
  - Runtime exporter and system metric probes.
- `scripts/offline`
  - Thin offline CLI entrypoints and utility scripts.
- `scripts/streaming`
  - Thin streaming CLI entrypoints; default execution path is Docker.
- `configs/streaming`
  - Runtime configs (`online.yaml`, `online_scale_up.yaml`, `detect.yaml`).
- `experiments/streaming`
  - Benchmark plan and profile YAMLs (`local_profiles.yaml`, `online_profiles.yaml`).
- `artifacts`
  - Offline model artifacts and streaming experiment outputs/summaries/reports.
- `apps`
  - Streamlit evaluation dashboard.
- `ops/observability`
  - Prometheus/Grafana configs and dashboards.
- `tests`
  - Focused unit/integration tests around streaming config, orchestration, reporting, and offline paths.

# Key Components

- Offline pipeline:
  - `ids_platform.offline.pipeline`
    - Main phase runner for offline pipeline stages.
  - `data_loading.py`, `dataset_split.py`, `training.py`, `evaluation.py`
    - Core offline stages; training writes `.joblib`, feature manifest, metrics CSVs.
- Streaming runtime:
  - `ids_platform.streaming.runtime.structured_streaming_job`
    - Main SUT runtime. Reads Kafka, scores records, writes predictions, emits debug metrics.
  - `ids_platform.streaming.runtime.pipeline`
    - Parses raw Kafka payloads, prepares features, adds timing columns.
  - `ids_platform.streaming.runtime.scoring`
    - Creates scoring pandas UDF and attaches prediction columns.
  - `ids_platform.streaming.runtime.query`
    - Applies Spark trigger mode and sanitizes tags.
  - `ids_platform.streaming.runtime.pandas_udf_job`
    - Batch-like Spark pandas UDF benchmark over parquet input.
- Replay:
  - `ids_platform.streaming.replay.config`
    - Rate schedule parsing and runtime estimation.
  - `ids_platform.streaming.replay.runner`
    - Produces replayed parquet rows into Kafka.
- Experiment orchestration:
  - `ids_platform.streaming.matrices.*`
    - Layer A/B/C, watermark, and load-quality scenarios.
  - `ids_platform.streaming.orchestration.profile_runner`
    - Resolves profile inheritance and builds safe commands.
  - `ids_platform.streaming.orchestration.profile_service`
    - Profile execution/listing/gate support.
  - `ids_platform.streaming.orchestration.full_evaluation`
    - Runs multiple matrices then builds final report.
  - `ids_platform.streaming.orchestration.benchmark_runner`
    - Runs benchmark plan and maintains summary CSV.
- Reporting/UI:
  - `ids_platform.streaming.reporting.report_builder`
    - Consumes summary CSVs and writes markdown/JSON evaluation reports.
  - `apps/streaming_dashboard.py`
    - Narrow post-run evaluation viewer for completed artifacts.
- Observability:
  - `ids_platform.streaming.metrics.prometheus_exporter`
    - Reads live `ids.metrics` topic and exposes runtime gauges.

# Important Conventions

- Thin scripts, real logic in `src/ids_platform/...`.
- Use dataclass option objects for services/matrices instead of passing `argparse.Namespace` into `src`.
- YAML is the primary configuration format for runtime configs and experiment profiles.
- Streaming script names are flat and action-oriented:
  - `run_structured_streaming.py`
  - `run_layer_a_matrix.py`
  - `build_online_report.py`
- Artifact naming:
  - Offline artifacts under `artifacts/offline/...`
  - Streaming summaries/reports under `artifacts/streaming/{online,scale_up,benchmark}/...`
- `run_tag` is a core identity field:
  - replay writes it into Kafka payloads
  - stream filters by `input_run_tag`
  - outputs and debug metrics carry `run_tag`
- Evaluation boundary:
  - official summaries currently come from `ids.metrics` aggregated by matrix runners
  - Prometheus/Grafana are for live runtime/debug telemetry only

# Dependencies

- Python libraries:
  - `pyspark`
  - `pandas`
  - `numpy`
  - `pyarrow`
  - `scikit-learn`
  - `joblib`
  - `PyYAML`
  - `confluent-kafka`
  - `streamlit`
  - `prometheus-client`
- External services:
  - Kafka broker
  - Zookeeper
  - Spark local runtime inside container
  - Prometheus + Grafana for observability
- Important externalized assets:
  - parquet datasets in `data/`
  - trained model artifacts and manifests in `artifacts/offline/`

# Environment & Setup

- Default execution model:
  - Docker-first for streaming and observability
  - `ids-dev` container is the main execution environment
- Main startup:
  - `docker compose up -d zookeeper kafka ids-dev`
- Typical streaming workflow:
  - start scorer: `scripts/streaming/run_structured_streaming.py`
  - replay parquet: `scripts/streaming/replay_parquet_to_kafka.py`
  - run matrices/profiles as needed
  - build report: `scripts/streaming/build_online_report.py`
  - open Streamlit for completed results
- Important config/profile files:
  - `configs/streaming/online.yaml`
  - `configs/streaming/online_scale_up.yaml`
  - `configs/streaming/detect.yaml`
  - `experiments/streaming/profiles/local_profiles.yaml`
  - `experiments/streaming/profiles/online_profiles.yaml`
  - `experiments/streaming/benchmark.yaml`
- Important environment/config variables:
  - `PROJECT_DIR`
    - used by Docker volume mounts in `.env.example`
  - `PYTHONPATH`
    - expected to include `/workspace/src` inside containers
  - `KAFKA_BOOTSTRAP_SERVERS`
    - bootstrap servers for Kafka clients
  - Spark runtime env:
    - `PYSPARK_PYTHON`
    - `PYSPARK_DRIVER_PYTHON`
    - `SPARK_LOCAL_IP`

# Known Constraints / Decisions

- Streaming is Docker-first because runtime configs use container hostnames like `kafka:29092`.
- The streaming job is the SUT and it currently emits the metrics used to build official matrix summaries.
- Prometheus exporter uses Kafka metrics topic for live telemetry; it is not the authoritative evaluation store.
- Structured Streaming and replay share common topics; `run_tag` / `input_run_tag` are critical to avoid cross-run interference.
- Matrix/orchestration logic relies heavily on subprocess invocation; many relationships are workflow-based rather than import-based.
- Some compatibility wrappers still exist from earlier refactors, especially around offline legacy layout.
- Performance-sensitive areas:
  - Spark microbatch metrics collection
  - replay throughput
  - pandas UDF benchmark scaling with number of models
- Security-sensitive areas:
  - profile-driven script execution is constrained to allowed script roots
  - Layer C/fault orchestration can manipulate runtime processes and Docker services
- Visible technical debt:
  - large configs/profiles and experiment orchestration are still operationally complex
  - some matrix runs are long and sensitive to timing assumptions
  - repo lacks a single canonical architecture doc; this file is intended to fill that gap

# Common Tasks

- Add a new offline model or training behavior:
  - update `src/ids_platform/offline/training.py`
  - ensure artifacts/threshold outputs remain compatible with streaming
  - update configs and tests if model names/features change
- Modify streaming scoring behavior:
  - start in `src/ids_platform/streaming/runtime/scoring.py`
  - then review `pipeline.py` and `structured_streaming_job.py`
- Modify Kafka replay behavior:
  - check `src/ids_platform/streaming/replay/runner.py`
  - rate/timing helpers live in `replay/config.py`
- Add or adjust experiment scenarios:
  - matrix logic in `src/ids_platform/streaming/matrices/`
  - profile YAMLs in `experiments/streaming/profiles/`
- Adjust reporting/UI:
  - summary aggregation/report generation in `src/ids_platform/streaming/reporting/`
  - post-run UI in `apps/streaming_dashboard.py`
- Adjust observability:
  - exporter logic in `src/ids_platform/streaming/metrics/prometheus_exporter.py`
  - Prometheus/Grafana configs in `ops/observability/`
- Modify scripts safely:
  - keep `scripts/` as CLI-only wrappers
  - put reusable logic in `src/ids_platform/...`
- Find where to change specific behaviors:
  - runtime scoring/output: `streaming/runtime/`
  - replay/input shaping: `streaming/replay/`
  - experiment sequencing: `streaming/matrices/` and `streaming/orchestration/`
  - report generation: `streaming/reporting/`
  - offline artifacts/model generation: `offline/`

# Testing Notes

- Test suite is `unittest`-based.
- Existing tests focus on:
  - replay config helpers
  - profile resolution/service behavior
  - benchmark/report orchestration
  - offline path layout
  - streaming entrypoint integration
- When changing orchestration/reporting/profile logic, start by updating or adding tests under `tests/`.
