from __future__ import annotations

import argparse
import gc
import importlib
import inspect
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from ids_platform.offline.config import PreprocessingConfig
from ids_platform.offline.log import get_logger
from ids_platform.offline.paths import Paths


PROJECT_ROOT = Path(__file__).resolve().parents[3]
OFFLINE_PACKAGE_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class PhaseDefinition:
    module_name: str
    function_name: str
    description: str


PHASES: dict[int, PhaseDefinition] = {
    1: PhaseDefinition("ids_platform.offline.data_loading", "run", "Ingest & Clean  -> Silver Parquet"),
    2: PhaseDefinition("ids_platform.offline.dataset_split", "run", "Split Silver -> Gold train/valid/test"),
    3: PhaseDefinition("ids_platform.offline.training", "run", "Train models -> Model artifacts"),
    4: PhaseDefinition("ids_platform.offline.evaluation", "run", "Evaluate -> Test metrics & benchmark"),
}

SUPPORTED_MODELS = ("logistic_regression", "random_forest", "gradient_boosting")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Offline IDS pipeline for CSE-CIC-IDS2018",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--phases",
        nargs="*",
        type=int,
        default=None,
        help="Phase numbers to run (e.g. 1 2 3 4). Default: all.",
    )
    parser.add_argument(
        "--feature-set",
        choices=["reduced", "full"],
        default="reduced",
        help="Feature registry to use (default: reduced = 17 features).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print phases that would run without executing.",
    )
    parser.add_argument(
        "--models",
        nargs="*",
        default=None,
        help="Optional model filter for phases 3 and 4 (e.g. logistic_regression random_forest). Default: all enabled models.",
    )
    return parser.parse_args()


def _resolve_requested_phases(raw_phase_ids: list[int] | None) -> list[int]:
    selected_phase_ids = raw_phase_ids if raw_phase_ids else sorted(PHASES.keys())
    invalid_phase_ids = [phase_id for phase_id in selected_phase_ids if phase_id not in PHASES]
    if invalid_phase_ids:
        raise ValueError(
            f"Unknown phase(s): {invalid_phase_ids} (valid: {list(PHASES.keys())})"
        )
    return selected_phase_ids


def _resolve_requested_models(raw_models: list[str] | None) -> list[str] | None:
    if raw_models is None:
        return None

    selected_models = [str(model).strip() for model in raw_models if str(model).strip()]
    if not selected_models:
        return None

    unknown_models = sorted(set(selected_models) - set(SUPPORTED_MODELS))
    if unknown_models:
        raise ValueError(
            f"Unknown model(s): {unknown_models} (valid: {list(SUPPORTED_MODELS)})"
        )

    deduped_models: list[str] = []
    seen: set[str] = set()
    for model_name in selected_models:
        if model_name not in seen:
            deduped_models.append(model_name)
            seen.add(model_name)
    return deduped_models


def _load_phase_runner(phase_id: int):
    phase_definition = PHASES[phase_id]
    phase_module = importlib.import_module(phase_definition.module_name)
    return getattr(phase_module, phase_definition.function_name), phase_definition


def run_phase(phase_id: int, paths: Paths, log, *, selected_models: list[str] | None = None) -> None:
    phase_runner, phase_definition = _load_phase_runner(phase_id)
    log.info("=" * 60)
    log.info("PHASE %d - %s", phase_id, phase_definition.description)
    log.info("=" * 60)
    if selected_models and phase_id in (3, 4):
        log.info("Model filter : %s", selected_models)

    started = time.perf_counter()
    runner_signature = inspect.signature(phase_runner)
    if "selected_models" in runner_signature.parameters:
        phase_runner(paths, selected_models=selected_models)
    else:
        phase_runner(paths)
    elapsed = time.perf_counter() - started
    log.info("Phase %d completed in %.1f s", phase_id, elapsed)

    gc.collect()
    log.info("  memory freed (gc.collect)")


def main() -> None:
    args = parse_args()
    try:
        phases = _resolve_requested_phases(args.phases)
        selected_models = _resolve_requested_models(args.models)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    paths = Paths.build(OFFLINE_PACKAGE_DIR, feature_set=args.feature_set)
    log = get_logger("pipeline", paths.log_dir / "pipeline.log")

    log.info("Project root : %s", paths.root)
    log.info("Feature set  : %s", args.feature_set)
    log.info("Phases       : %s", phases)
    log.info("Models       : %s", selected_models or "all enabled")

    if args.dry_run:
        for phase_id in phases:
            log.info("  [DRY-RUN] Phase %d - %s", phase_id, PHASES[phase_id].description)
        return

    started = time.perf_counter()
    for phase_id in phases:
        run_phase(phase_id, paths, log, selected_models=selected_models)

    log.info("=" * 60)
    log.info("ALL DONE  total=%.1f s", time.perf_counter() - started)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
