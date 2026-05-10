from __future__ import annotations

from datetime import datetime

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.core.config import resolve_kafka_bootstrap_servers
from ids_platform.streaming.evaluation.matrices.common import collect_matching_metrics
from ids_platform.streaming.evaluation.orchestration.fault_matrix import parse_iso_datetime, safe_float

from .logging_utils import _log_phase, _mark_row_failed, _mark_row_succeeded
from .runtime import validate_runtime_metric
from .types import LayerCFaultMatrixOptions


def _entrypoint_module():
    from ids_platform.streaming.evaluation.matrices import layer_c_fault_matrix

    return layer_c_fault_matrix


def apply_recovery_metric_to_row(
    *,
    row: dict,
    recovered_metric: dict,
    fault_time: datetime,
) -> None:
    recover_ts_utc = str(recovered_metric.get("ts_utc", ""))
    if recover_ts_utc:
        recover_time = parse_iso_datetime(recover_ts_utc)
        row["recover_ts_utc"] = recover_ts_utc
        row["recovery_seconds"] = max((recover_time - fault_time).total_seconds(), 0.0)

    row["rows_after_fault"] = int(recovered_metric.get("rows") or 0)
    row["rows_per_sec_after_fault"] = safe_float(recovered_metric.get("rows_per_sec"))

    latency = recovered_metric.get("latency_ms") or {}
    processing = latency.get("processing") or {}
    ingest_to_emit = latency.get("ingest_to_emit") or {}
    end_to_end = latency.get("end_to_end") or {}
    source_to_emit = latency.get("source_to_emit") or {}
    row["ingest_to_emit_p95_ms_after_fault"] = safe_float(
        ingest_to_emit.get("p95", processing.get("p95"))
    )
    row["source_to_emit_p95_ms_after_fault"] = safe_float(
        source_to_emit.get("p95", end_to_end.get("p95"))
    )
    row["proc_p95_ms_after_fault"] = safe_float(processing.get("p95"))
    row["e2e_p95_ms_after_fault"] = safe_float(end_to_end.get("p95"))
    row["metric_warnings"] = "; ".join(
        str(item).strip() for item in (recovered_metric.get("metric_warnings") or []) if str(item).strip()
    )
    _mark_row_succeeded(row)


def wait_for_post_fault_recovery(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
    fault_time: datetime,
    fault_epoch_ms: int,
) -> bool:
    _log_phase("recovery_wait_start", run_tag=run_tag, scenario=scenario, timeout_sec=options.metrics_timeout_sec)
    recovered_metric = _entrypoint_module().fetch_metric(
        config=options.config,
        run_tag=run_tag,
        timeout_sec=options.metrics_timeout_sec,
        after_ts_utc=row["fault_ts_utc"],
        after_epoch_ms=fault_epoch_ms,
        execution_mode=options.execution_mode,
        python_executable=options.python_executable,
        bootstrap_servers=options.bootstrap_servers,
    )
    if recovered_metric is None:
        _mark_row_failed(row, "post-fault metric not found")
        _log_phase("recovery_wait_timeout", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return False

    recovered_metric_ok, recovered_metric_reason = validate_runtime_metric(recovered_metric)
    if not recovered_metric_ok:
        _mark_row_failed(row, f"post-fault metric invalid: {recovered_metric_reason}")
        _log_phase("recovery_wait_invalid", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return False

    _log_phase("recovery_wait_done", run_tag=run_tag, scenario=scenario)
    apply_recovery_metric_to_row(
        row=row,
        recovered_metric=recovered_metric,
        fault_time=fault_time,
    )
    return True


def collect_scenario_metrics(
    *,
    config: str,
    run_tag: str,
    timeout_sec: int,
    start_timestamp_ms: int,
    bootstrap_servers_override: str,
) -> list[dict]:
    cfg = load_yaml_mapping(resolve_project_path(config))
    kafka_cfg = cfg.get("kafka") or {}
    bootstrap_servers = resolve_kafka_bootstrap_servers(
        bootstrap_servers_override or str(kafka_cfg.get("bootstrap_servers", "kafka:29092")),
        execution_mode="host",
    )
    metrics_topic = str(kafka_cfg.get("metrics_topic", "ids.metrics"))
    return collect_matching_metrics(
        bootstrap_servers=bootstrap_servers,
        topic=metrics_topic,
        run_tag=run_tag,
        timeout_sec=timeout_sec,
        idle_sec=5,
        group_prefix="layer-c-metrics",
        start_timestamp_ms=max(start_timestamp_ms - 30_000, 0),
    )
