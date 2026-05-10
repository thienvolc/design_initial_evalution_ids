from __future__ import annotations

import csv
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from ids_platform.common.paths import PROJECT_ROOT
from ids_platform.common.subprocess import run_command
from ids_platform.streaming.benchmark.contract import load_benchmark_plan, materialize_run_params


def run_detect_benchmark(plan, run, run_params: dict[str, Any], *, python_executable: str = sys.executable) -> dict:
    run_dir = plan.output_root / "runs" / run.run_id
    output_dir = run_dir / "predictions"
    summary_path = run_dir / "run_summary.json"

    run_dir.mkdir(parents=True, exist_ok=True)

    command = [
        python_executable,
        str(plan.detect_script),
        "--config",
        str(plan.detect_config),
        "--output-parquet-dir",
        str(output_dir),
        "--summary-json",
        str(summary_path),
        "--run-tag",
        run.run_id,
        "--feature-set",
        str(run_params.get("feature_set", "full")),
        "--batch-size",
        str(int(run_params.get("batch_size", 50000))),
    ]

    if run_params.get("max_rows") is not None:
        command.extend(["--max-rows", str(int(run_params["max_rows"]))])

    models = run_params.get("models") or []
    if models:
        command.extend(["--models", *[str(model_name) for model_name in models]])

    started = time.perf_counter()
    result = run_command(command, cwd=PROJECT_ROOT)
    wall_seconds = time.perf_counter() - started

    if result.returncode != 0:
        raise RuntimeError(
            f"Run {run.run_id} failed with code {result.returncode}.\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    with summary_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    payload["runner"] = {
        "run_id": run.run_id,
        "layer": run.layer,
        "params": run_params,
        "wall_seconds": round(wall_seconds, 6),
        "finished_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    return payload


def append_benchmark_summary_row(summary_csv: Path, payload: dict[str, Any]) -> None:
    existing_rows = load_benchmark_summary_rows(summary_csv)
    row = build_benchmark_summary_row(payload)
    existing_rows = [previous_row for previous_row in existing_rows if previous_row.get("run_id") != row["run_id"]]
    existing_rows.append(row)
    write_benchmark_summary_rows(summary_csv, existing_rows)


def build_benchmark_summary_row(payload: dict[str, Any]) -> dict[str, Any]:
    run_info = payload.get("runner") or {}
    timing = payload.get("timing_seconds") or {}
    latency = payload.get("latency_ms") or {}
    rows_scored = (payload.get("source") or {}).get("rows_scored", 0)
    total_seconds = float(timing.get("total", 0.0) or 0.0)
    rows_per_second = (rows_scored / total_seconds) if total_seconds > 0 else 0.0

    model_fields: dict[str, Any] = {}
    for model_name, model_data in (payload.get("models") or {}).items():
        metrics = model_data.get("metrics") or {}
        model_fields[f"{model_name}_f1"] = metrics.get("f1")
        model_fields[f"{model_name}_recall"] = metrics.get("recall")
        model_fields[f"{model_name}_fpr"] = metrics.get("fpr")

    return {
        "run_id": run_info.get("run_id", ""),
        "layer": run_info.get("layer", ""),
        "feature_set": (payload.get("profile") or {}).get("feature_set", ""),
        "models": ",".join((payload.get("profile") or {}).get("models", [])),
        "rows_scored": rows_scored,
        "rows_per_second": round(rows_per_second, 3),
        "total_seconds": timing.get("total", 0.0),
        "read_seconds": timing.get("read", 0.0),
        "transform_seconds": timing.get("transform", 0.0),
        "inference_seconds": timing.get("inference", 0.0),
        "sink_seconds": timing.get("sink", 0.0),
        "source_to_ingest_p95_ms": (latency.get("source_to_ingest") or {}).get("p95", 0.0),
        "processing_p95_ms": (latency.get("processing") or {}).get("p95", 0.0),
        "end_to_end_p95_ms": (latency.get("end_to_end") or {}).get("p95", 0.0),
        **model_fields,
    }


@lru_cache(maxsize=8)
def _load_benchmark_summary_rows_cached(summary_csv_key: str) -> tuple[dict[str, Any], ...]:
    summary_csv = Path(summary_csv_key)
    if not summary_csv.exists():
        return ()

    with summary_csv.open("r", encoding="utf-8", newline="") as handle:
        return tuple(dict(row) for row in csv.DictReader(handle))


def load_benchmark_summary_rows(summary_csv: Path) -> list[dict[str, Any]]:
    return [dict(row) for row in _load_benchmark_summary_rows_cached(str(summary_csv.resolve()))]


def write_benchmark_summary_rows(summary_csv: Path, rows: list[dict[str, Any]]) -> None:
    summary_csv.parent.mkdir(parents=True, exist_ok=True)

    fieldnames: list[str] = []
    for item in rows:
        for key in item.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    normalized_rows = [{key: item.get(key, "") for key in fieldnames} for item in rows]
    temp_path = summary_csv.with_name(f"{summary_csv.name}.tmp")
    with temp_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(normalized_rows)
    temp_path.replace(summary_csv)

    _load_benchmark_summary_rows_cached.cache_clear()


@dataclass(frozen=True)
class BenchmarkMatrixOptions:
    benchmark_config_path: str = "experiments/streaming/benchmark.yaml"
    only_runs: tuple[str, ...] | None = None
    python_executable: str = sys.executable
    project_root: Path = PROJECT_ROOT


def run_benchmark_matrix(options: BenchmarkMatrixOptions) -> int:
    plan = load_benchmark_plan(options.project_root / options.benchmark_config_path, options.project_root)
    summary_csv = plan.output_root / "benchmark_summary.csv"

    runs = plan.runs
    if options.only_runs:
        wanted = set(options.only_runs)
        runs = [run for run in runs if run.run_id in wanted]

    if not runs:
        print("No runs selected.")
        return 0

    print(f"Running benchmark plan '{plan.name}' with {len(runs)} runs")
    summary_rows = load_benchmark_summary_rows(summary_csv)
    selected_run_ids = {run.run_id for run in runs}
    summary_rows = [row for row in summary_rows if row.get("run_id") not in selected_run_ids]
    for index, run in enumerate(runs, start=1):
        run_params = materialize_run_params(plan, run)
        print(f"[{index}/{len(runs)}] {run.run_id} layer={run.layer} params={run_params}")
        payload = run_detect_benchmark(plan, run, run_params, python_executable=options.python_executable)
        summary_rows.append(build_benchmark_summary_row(payload))

    write_benchmark_summary_rows(summary_csv, summary_rows)

    print(f"Done. Summary CSV: {summary_csv}")
    return 0
