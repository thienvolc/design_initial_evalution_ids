from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ids_platform.offline.config import load_yaml


@dataclass(frozen=True)
class FairnessLayer:
    name: str
    varied_group: set[str]
    fixed: dict[str, Any]


@dataclass(frozen=True)
class BenchmarkRun:
    run_id: str
    layer: str
    params: dict[str, Any]


@dataclass(frozen=True)
class BenchmarkPlan:
    name: str
    output_root: Path
    detect_script: Path
    detect_config: Path
    primary_latency_metric: str
    sustainable_throughput_metric: str
    feature_sets: set[str]
    model_profiles: set[str]
    layers: dict[str, FairnessLayer]
    runs: list[BenchmarkRun]


class FairnessError(ValueError):
    pass


_VALID_LATENCY_METRICS = {
    "source_to_ingest_p95_ms",
    "processing_p95_ms",
    "end_to_end_p95_ms",
}

_VALID_THROUGHPUT_METRICS = {
    "rows_per_second",
}


def load_benchmark_plan(path: Path, project_root: Path) -> BenchmarkPlan:
    payload = load_yaml(path)
    benchmark = payload.get("benchmark") or {}
    contracts = payload.get("contracts") or {}
    matrix = payload.get("matrix") or {}

    fairness_payload = contracts.get("fairness_layers") or {}
    layers: dict[str, FairnessLayer] = {}
    for layer_name, layer_cfg in fairness_payload.items():
        if not isinstance(layer_cfg, dict):
            continue
        varied = set(layer_cfg.get("varied_group") or [])
        fixed = dict(layer_cfg.get("fixed") or {})
        layers[layer_name] = FairnessLayer(name=layer_name, varied_group=varied, fixed=fixed)

    runs: list[BenchmarkRun] = []
    for _, run_list in matrix.items():
        if not isinstance(run_list, list):
            continue
        for row in run_list:
            if not isinstance(row, dict):
                continue
            run_id = str(row.get("run_id", "")).strip()
            layer = str(row.get("layer", "")).strip()
            if not run_id or not layer:
                continue
            params = {k: v for k, v in row.items() if k not in {"run_id", "layer"}}
            runs.append(BenchmarkRun(run_id=run_id, layer=layer, params=params))

    if not layers:
        raise ValueError(f"No fairness_layers found in {path}")
    if not runs:
        raise ValueError(f"No benchmark runs found in {path}")

    plan = BenchmarkPlan(
        name=str(benchmark.get("name", "benchmark")),
        output_root=_resolve(project_root, str(benchmark.get("output_root", "artifacts/streaming/benchmark"))),
        detect_script=_resolve(project_root, str(benchmark.get("detect_script", "scripts/streaming/run_pandas_udf_benchmark.py"))),
        detect_config=_resolve(project_root, str(benchmark.get("detect_config", "configs/streaming/detect.yaml"))),
        primary_latency_metric=str(contracts.get("primary_latency_metric", "end_to_end_p95_ms")),
        sustainable_throughput_metric=str(contracts.get("sustainable_throughput_metric", "rows_per_second")),
        feature_sets={str(x) for x in (contracts.get("feature_sets") or [])},
        model_profiles={str(x) for x in (contracts.get("model_profiles") or [])},
        layers=layers,
        runs=runs,
    )

    validate_fairness(plan)
    return plan


def validate_fairness(plan: BenchmarkPlan) -> None:
    if not plan.detect_script.exists():
        raise FairnessError(f"Detect script not found: {plan.detect_script}")
    if not plan.detect_config.exists():
        raise FairnessError(f"Detect config not found: {plan.detect_config}")

    if plan.primary_latency_metric not in _VALID_LATENCY_METRICS:
        raise FairnessError(
            f"Unsupported primary_latency_metric '{plan.primary_latency_metric}'. "
            f"Allowed: {sorted(_VALID_LATENCY_METRICS)}"
        )
    if plan.sustainable_throughput_metric not in _VALID_THROUGHPUT_METRICS:
        raise FairnessError(
            f"Unsupported sustainable_throughput_metric '{plan.sustainable_throughput_metric}'. "
            f"Allowed: {sorted(_VALID_THROUGHPUT_METRICS)}"
        )

    for layer_name, layer in plan.layers.items():
        overlap = layer.varied_group.intersection(layer.fixed.keys())
        if overlap:
            raise FairnessError(
                f"Layer {layer_name} has keys in both varied_group and fixed: {sorted(overlap)}"
            )

    for run in plan.runs:
        if run.layer not in plan.layers:
            raise FairnessError(f"Run {run.run_id} references unknown layer '{run.layer}'")

        layer = plan.layers[run.layer]
        illegal = set(run.params.keys()) - set(layer.varied_group)
        if illegal:
            raise FairnessError(
                f"Run {run.run_id} violates fairness for layer {run.layer}. "
                f"Illegal changed keys: {sorted(illegal)}; allowed: {sorted(layer.varied_group)}"
            )

        merged = materialize_run_params(plan, run)

        feature_set = merged.get("feature_set")
        if plan.feature_sets and feature_set not in plan.feature_sets:
            raise FairnessError(
                f"Run {run.run_id} uses unsupported feature_set '{feature_set}'. "
                f"Allowed: {sorted(plan.feature_sets)}"
            )

        models = merged.get("models")
        if not isinstance(models, list) or not models:
            raise FairnessError(f"Run {run.run_id} must materialize with a non-empty models list")

        model_names = [str(model_name) for model_name in models]
        if plan.model_profiles:
            unknown_models = [model_name for model_name in model_names if model_name not in plan.model_profiles]
            if unknown_models:
                raise FairnessError(
                    f"Run {run.run_id} uses unsupported models {unknown_models}. "
                    f"Allowed: {sorted(plan.model_profiles)}"
                )

        if "batch_size" in merged:
            try:
                batch_size = int(merged["batch_size"])
            except (TypeError, ValueError) as exc:
                raise FairnessError(f"Run {run.run_id} has invalid batch_size={merged['batch_size']}") from exc
            if batch_size <= 0:
                raise FairnessError(f"Run {run.run_id} must have batch_size > 0")


def materialize_run_params(plan: BenchmarkPlan, run: BenchmarkRun) -> dict[str, Any]:
    layer = plan.layers[run.layer]
    merged = dict(layer.fixed)
    merged.update(run.params)
    return merged


def _resolve(project_root: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return project_root / path

