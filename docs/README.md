# Documentation Index

Use these files as the canonical documentation set for the current project state.

## Canonical Docs

- [architecture.md](architecture.md)
  - high-level architecture, major components, and data flow
- [execution_runbook.md](execution_runbook.md)
  - Docker-first run order for manual runs, profiles, matrices, and report generation
- [glossary.md](glossary.md)
  - project-specific concepts and terms
- [component_guide.md](component_guide.md)
  - where to look for each major responsibility in the codebase
- [evaluation_methodology.md](evaluation_methodology.md)
  - official SUT vs evaluation-system boundary and artifact contract
- [adr/ADR-0001-layer-c-stabilization.md](adr/ADR-0001-layer-c-stabilization.md)
  - canonical rationale for Layer C hybrid host-orchestrated and package-split behavior

## Repo-Level Context

- [../context/project_context.md](../context/project_context.md)
  - canonical minimal project context for future AI and engineer sessions
- [../context/phase_list.md](../context/phase_list.md)
  - ordered phase index for project evolution
- [../context/canonical_legacy_map.md](../context/canonical_legacy_map.md)
  - short map of canonical, legacy, and compatibility surfaces
- [../scripts/streaming/README.md](../scripts/streaming/README.md)
  - operator-focused usage for streaming entrypoints

## Legacy Docs

The following files are historical notes, not the authoritative source for current execution guidance:

- `01_overview.md`
- `05_final_operational_summary.md`
- `06_phase4_runbook.md`
- `07_test_machine_run_guide.md`
- `scale_up_plan.md`

Read them only for historical context or prior decisions. Prefer the canonical docs above for current behavior.
