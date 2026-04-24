# Project Context

## Project

- Hybrid IDS Evaluation Platform
- one repo with:
  - offline data/model pipeline
  - streaming evaluation system on Kafka + Spark Structured Streaming

## Current Truth

- core offline, replay, streaming, matrix, and report paths already exist
- current risky area is Layer C and the sync between code, docs, and runbooks
- `context/` is intentionally minimal:
  - project identity and stable truths live here
  - code and canonical docs remain the primary source of detail

## Execution Truth

- streaming workflow is Docker-first
- official outputs are summary CSVs and consolidated reports
- Prometheus/Grafana are observability only
- Layer C is the main exception:
  - host orchestrator injects faults
  - SUT and replay subprocesses run in Docker

## What Must Stay Correct

- `run_tag` / `input_run_tag` isolation
- SUT vs evaluation boundary
- artifact contract:
  - SUT emits predictions and `ids.metrics`
  - matrices build summary CSVs
  - reports read summary CSVs

## Current Refactor Risk

- `docs/`, `scripts/`, and `src/` are not fully synchronized
- Layer C has non-obvious behavior that exists for correctness, not style

## Canonical Docs

- [../docs/README.md](../docs/README.md)
- [../docs/architecture.md](../docs/architecture.md)
- [../docs/evaluation_methodology.md](../docs/evaluation_methodology.md)
- [../docs/execution_runbook.md](../docs/execution_runbook.md)
- [../docs/component_guide.md](../docs/component_guide.md)
- [../docs/glossary.md](../docs/glossary.md)
- [../docs/adr/ADR-0001-layer-c-stabilization.md](../docs/adr/ADR-0001-layer-c-stabilization.md)
- [../scripts/streaming/README.md](../scripts/streaming/README.md)
