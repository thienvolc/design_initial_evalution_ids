from __future__ import annotations

import time
from dataclasses import replace
from datetime import datetime, timezone

from ids_platform.streaming.config.layer_c import LayerCMatrixConfig, LayerCScenarioConfig
from ids_platform.streaming.evaluation.matrices.common import (
    collect_matching_metrics,
    summarize_runtime_metrics,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.replay.runner import publish_input_sentinel, run_replay_job
from ids_platform.streaming.runtime.config import RuntimeLifecycleConfig
from ids_platform.streaming.runtime.structured_streaming_job import start_structured_streaming_job


def _row_template(config: LayerCScenarioConfig) -> dict:
    return {
        "run_tag": config.run_tag,
        "load_profile": config.scenario,
        "scenario": config.scenario,
        "model": config.runtime.model.reported_name,
        "feature_set": config.runtime.features.feature_set,
        "fault_ts_utc": "",
        "recover_ts_utc": "",
        "recovery_seconds": "",
        "rows_after_fault": "",
        "rows_per_sec_after_fault": "",
        "ingest_to_emit_p95_ms_after_fault": "",
        "source_to_emit_p95_ms_after_fault": "",
        "proc_p95_ms_after_fault": "",
        "e2e_p95_ms_after_fault": "",
        "metric_warnings": "",
        "status": "failed",
        "notes": "",
    }


def _runtime_for_restart(config: LayerCScenarioConfig):
    lifecycle = replace(config.runtime.lifecycle, reset_outputs=False)
    return replace(config.runtime, lifecycle=lifecycle)


def _apply_fault(config: LayerCScenarioConfig, job) -> object:
    scenario = config.scenario
    if scenario == "producer_restart":
        time.sleep(max(int(config.producer_restart_pause_sec), 0))
        return job
    if scenario == "spark_process_restart":
        job.stop()
        restarted = start_structured_streaming_job(_runtime_for_restart(config))
        if config.stream_startup_wait_sec > 0:
            time.sleep(config.stream_startup_wait_sec)
        return restarted
    if scenario == "network_slowdown":
        return job
    raise ValueError(f"unsupported Layer C scenario: {scenario}")


def _collect_after_fault(config: LayerCScenarioConfig, *, fault_epoch_ms: int) -> list[dict]:
    return collect_matching_metrics(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        topic=config.runtime.kafka.metrics_topic,
        run_tag=config.run_tag,
        timeout_sec=config.metrics_timeout_sec,
        idle_sec=config.metrics_idle_sec,
        group_prefix="layer-c-metrics",
        start_timestamp_ms=max(int(fault_epoch_ms) - 5_000, 0),
    )


def _finalize_row(row: dict, metrics_rows: list[dict], *, fault_started_at: float) -> dict:
    if not metrics_rows:
        row["status"] = "metrics_missing"
        row["notes"] = "no post-fault metrics collected"
        return row

    summary = summarize_runtime_metrics(metrics_rows)
    row["status"] = "ok"
    row["notes"] = ""
    row["recover_ts_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row["recovery_seconds"] = max(time.time() - fault_started_at, 0.0)
    row["rows_after_fault"] = summary.get("rows_total", "")
    row["rows_per_sec_after_fault"] = summary.get("rows_per_sec_avg", "")
    row["ingest_to_emit_p95_ms_after_fault"] = summary.get("ingest_to_emit_p95_ms_max", "")
    row["source_to_emit_p95_ms_after_fault"] = summary.get("source_to_emit_p95_ms_max", "")
    row["proc_p95_ms_after_fault"] = summary.get("proc_p95_ms_max", "")
    row["e2e_p95_ms_after_fault"] = summary.get("e2e_p95_ms_max", "")
    return row


def run_layer_c_scenario(config: LayerCScenarioConfig) -> tuple[dict, list[dict]]:
    row = _row_template(config)
    job = start_structured_streaming_job(config.runtime)
    try:
        if config.stream_startup_wait_sec > 0:
            time.sleep(config.stream_startup_wait_sec)

        run_replay_job(config.warmup_replay)
        fault_started_at = time.time()
        fault_time = datetime.now(timezone.utc)
        fault_epoch_ms = int(fault_started_at * 1000)
        row["fault_ts_utc"] = fault_time.isoformat(timespec="seconds")

        job = _apply_fault(config, job)
        run_replay_job(config.post_fault_replay)
        publish_input_sentinel(config.post_fault_replay.runtime)
        job.wait(timeout_sec=config.stream_wait_timeout_sec)

        metrics_rows = _collect_after_fault(config, fault_epoch_ms=fault_epoch_ms)
        return _finalize_row(row, metrics_rows, fault_started_at=fault_started_at), metrics_rows
    except Exception as exc:
        row["status"] = "failed"
        row["notes"] = str(exc)
        return row, []
    finally:
        job.stop()


def run_layer_c_matrix(config: LayerCMatrixConfig) -> int:
    rows: list[dict] = []
    for scenario_config in config.scenarios:
        row, metrics_rows = run_layer_c_scenario(scenario_config)
        write_metrics_timeseries(metrics_rows, run_tag=scenario_config.run_tag)
        rows.append(row)
    write_summary_rows(config.summary_csv, rows)
    return 0


run = run_layer_c_matrix
