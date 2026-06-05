from __future__ import annotations

import csv
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.common.paths import resolve_project_path  # noqa: E402
from ids_platform.streaming.config.calibration import (  # noqa: E402
    build_capacity_runtime_sensitivity_300_fresh_config,
)
from ids_platform.streaming.evaluation.matrices.capacity_calibration_matrix import (  # noqa: E402
    run_capacity_calibration_matrix,
)
from ids_platform.streaming.evaluation.matrices.common.outputs import (  # noqa: E402
    write_summary_rows,
)
from ids_platform.streaming.runtime.query import safe_tag  # noqa: E402

SINGLE_RUN_INDEX_ENV = "IDS_STREAMING_SINGLE_RUN_INDEX"
SINGLE_RUN_SUMMARY_ENV = "IDS_STREAMING_SINGLE_RUN_SUMMARY"
TOPIC_NAMESPACE_ENV = "IDS_STREAMING_TOPIC_NAMESPACE"

SUMMARY_CSV = resolve_project_path(
    "artifacts/streaming/evaluation/capacity_runtime_sensitivity_300_fresh.csv"
)
PARTS_DIR = resolve_project_path(
    "artifacts/streaming/evaluation/runtime_sensitivity_300_fresh_parts"
)


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit(
            "run_runtime_sensitivity_300_fresh.py is config-driven and accepts no CLI arguments."
        )

    single_run_index = os.environ.get(SINGLE_RUN_INDEX_ENV)
    if single_run_index:
        return _run_single_process(int(single_run_index))

    return _run_fresh_process_matrix()


def _run_single_process(run_index: int) -> int:
    summary_csv = os.environ.get(SINGLE_RUN_SUMMARY_ENV)
    if not summary_csv:
        raise RuntimeError(f"{SINGLE_RUN_SUMMARY_ENV} is required for single-run mode")

    config = build_capacity_runtime_sensitivity_300_fresh_config(summary_csv=summary_csv)
    if run_index < 1 or run_index > len(config.runs):
        raise RuntimeError(f"single run index out of range: {run_index}")

    single_run = config.runs[run_index - 1]
    single_config = replace(
        config,
        name=f"{config.name}_run_{run_index:02d}",
        runs=(single_run,),
        summary_csv=resolve_project_path(summary_csv),
    )
    print(
        "running "
        f"{single_config.name} "
        f"profile={single_run.benchmark.profile.name} "
        f"mode={single_run.mode} "
        f"rps={single_run.target_rps}",
        flush=True,
    )
    return int(run_capacity_calibration_matrix(single_config))


def _run_fresh_process_matrix() -> int:
    config = build_capacity_runtime_sensitivity_300_fresh_config(
        summary_csv=str(SUMMARY_CSV)
    )
    PARTS_DIR.mkdir(parents=True, exist_ok=True)
    if SUMMARY_CSV.exists():
        SUMMARY_CSV.unlink()

    rows: list[dict] = []
    base_namespace = safe_tag(
        os.environ.get(TOPIC_NAMESPACE_ENV, "").strip()
        or "runtime_sensitivity_300_fresh"
    )
    print(f"running {config.name} fresh_process_runs={len(config.runs)}", flush=True)
    for run_index, run in enumerate(config.runs, start=1):
        part_csv = PARTS_DIR / f"run_{run_index:02d}.csv"
        if part_csv.exists():
            part_csv.unlink()

        namespace = f"{base_namespace}_run{run_index:02d}"
        env = dict(os.environ)
        env[SINGLE_RUN_INDEX_ENV] = str(run_index)
        env[SINGLE_RUN_SUMMARY_ENV] = str(part_csv)
        env[TOPIC_NAMESPACE_ENV] = namespace
        print(
            "fresh process "
            f"{run_index}/{len(config.runs)} "
            f"profile={run.benchmark.profile.name} "
            f"mode={run.mode} "
            f"namespace={namespace}",
            flush=True,
        )
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve())],
            cwd=str(PROJECT_ROOT),
            env=env,
            check=False,
        )
        if completed.returncode != 0:
            return int(completed.returncode)

        part_rows = _read_rows(part_csv)
        for row in part_rows:
            row["fresh_process_index"] = run_index
            row["fresh_process_namespace"] = namespace
        rows.extend(part_rows)
        write_summary_rows(SUMMARY_CSV, rows)

    return 0


def _read_rows(path: Path) -> list[dict]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


if __name__ == "__main__":
    raise SystemExit(main())
