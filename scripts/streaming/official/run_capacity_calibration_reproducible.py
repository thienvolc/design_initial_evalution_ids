from __future__ import annotations

import csv
import os
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ids_platform.common.paths import resolve_project_path  # noqa: E402
from ids_platform.streaming.config.calibration import (  # noqa: E402
    build_capacity_load_threshold_config,
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
SUMMARY_CSV_ENV = "IDS_STREAMING_SUMMARY_CSV"
REPEAT_COUNT_ENV = "IDS_STREAMING_REPEAT_COUNT"
RUN_STAMP_ENV = "IDS_STREAMING_RUN_STAMP"


def main() -> int:
    if len(sys.argv) > 1:
        raise SystemExit(
            "run_capacity_calibration_reproducible.py is config-driven and accepts no CLI arguments."
        )

    single_run_index = os.environ.get(SINGLE_RUN_INDEX_ENV)
    if single_run_index:
        return _run_single_process(int(single_run_index))

    return _run_fresh_process_matrix()


def _repeat_count() -> int:
    raw_value = os.environ.get(REPEAT_COUNT_ENV, "3")
    try:
        return max(int(raw_value), 1)
    except ValueError as exc:
        raise RuntimeError(f"{REPEAT_COUNT_ENV} must be an integer") from exc


def _summary_csv() -> Path:
    explicit_path = os.environ.get(SUMMARY_CSV_ENV, "").strip()
    if explicit_path:
        return resolve_project_path(explicit_path)

    stamp = os.environ.get(RUN_STAMP_ENV, "").strip() or datetime.now(
        timezone.utc
    ).strftime("%Y%m%d%H%M%S")
    return resolve_project_path(
        f"artifacts/streaming/evaluation/capacity_load_threshold_3run_{stamp}.csv"
    )


def _parts_dir(summary_csv: Path) -> Path:
    return summary_csv.parent / f"{summary_csv.stem}_parts"


def _run_single_process(run_index: int) -> int:
    summary_csv = os.environ.get(SINGLE_RUN_SUMMARY_ENV)
    if not summary_csv:
        raise RuntimeError(f"{SINGLE_RUN_SUMMARY_ENV} is required for single-run mode")

    config = build_capacity_load_threshold_config(
        repeats=_repeat_count(),
        summary_csv=summary_csv,
    )
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
        f"repeat={single_run.benchmark.repeat_index} "
        f"profile={single_run.benchmark.profile.name} "
        f"mode={single_run.mode} "
        f"rps={single_run.target_rps}",
        flush=True,
    )
    return int(run_capacity_calibration_matrix(single_config))


def _run_fresh_process_matrix() -> int:
    repeat_count = _repeat_count()
    summary_csv = _summary_csv()
    parts_dir = _parts_dir(summary_csv)
    if summary_csv.exists():
        raise RuntimeError(f"summary already exists, refusing to overwrite: {summary_csv}")
    if parts_dir.exists():
        raise RuntimeError(f"parts directory already exists, refusing to overwrite: {parts_dir}")

    config = build_capacity_load_threshold_config(
        repeats=repeat_count,
        summary_csv=str(summary_csv),
    )
    summary_csv.parent.mkdir(parents=True, exist_ok=True)
    parts_dir.mkdir(parents=True, exist_ok=False)

    rows: list[dict] = []
    base_namespace = safe_tag(
        os.environ.get(TOPIC_NAMESPACE_ENV, "").strip()
        or f"capacity_threshold_{summary_csv.stem}"
    )
    print(
        f"running {config.name} "
        f"fresh_process_runs={len(config.runs)} "
        f"repeats={repeat_count} "
        f"summary={summary_csv}",
        flush=True,
    )
    for run_index, run in enumerate(config.runs, start=1):
        part_csv = parts_dir / f"run_{run_index:02d}.csv"
        if part_csv.exists():
            raise RuntimeError(f"part summary already exists: {part_csv}")

        namespace = f"{base_namespace}_run{run_index:02d}"
        env = dict(os.environ)
        env[SINGLE_RUN_INDEX_ENV] = str(run_index)
        env[SINGLE_RUN_SUMMARY_ENV] = str(part_csv)
        env[TOPIC_NAMESPACE_ENV] = namespace
        env[REPEAT_COUNT_ENV] = str(repeat_count)
        print(
            "fresh process "
            f"{run_index}/{len(config.runs)} "
            f"repeat={run.benchmark.repeat_index} "
            f"profile={run.benchmark.profile.name} "
            f"mode={run.mode} "
            f"rps={run.target_rps} "
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
        write_summary_rows(summary_csv, rows)

    return 0


def _read_rows(path: Path) -> list[dict]:
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


if __name__ == "__main__":
    raise SystemExit(main())
