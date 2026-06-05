from __future__ import annotations

import multiprocessing
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone

from ids_platform.streaming.config.fault_recovery import (
    FaultRecoveryMatrixConfig,
    FaultRecoveryScenarioConfig,
)
from ids_platform.streaming.evaluation.matrices.common import (
    summarize_prediction_latency,
    write_summary_rows,
)
from ids_platform.streaming.evaluation.matrices.throughput.prediction_artifact import (
    collect_prediction_response_artifact,
)
from ids_platform.streaming.evaluation.matrices.throughput.benchmark_run import (
    cleanup_run_topics,
    reset_run_topics,
)
from ids_platform.streaming.evaluation.matrices.throughput.collectors import (
    KafkaLagSampler,
)
from ids_platform.streaming.replay.runner import publish_input_sentinel, run_replay_job
from ids_platform.streaming.runtime.config import RuntimeConfig
from ids_platform.streaming.runtime.structured_streaming_job import run_structured_streaming_job


@dataclass
class RuntimeProcessHandle:
    process: object
    started_at: float
    started_epoch_ms: int

    def wait_for_exit(self, *, timeout_sec: int) -> None:
        self.process.join(max(int(timeout_sec), 1))
        if self.process.is_alive():
            self.kill()
            raise RuntimeError("runtime process did not exit before timeout")
        exitcode = getattr(self.process, "exitcode", None)
        if exitcode not in (0, None):
            raise RuntimeError(f"runtime process exited with code {exitcode}")

    def kill(self) -> None:
        if not self.process.is_alive():
            return
        kill = getattr(self.process, "kill", None)
        if callable(kill):
            kill()
        else:
            self.process.terminate()
        self.process.join(30)


def _run_runtime_process(config: RuntimeConfig) -> None:
    raise SystemExit(run_structured_streaming_job(config))


def start_runtime_process(config: RuntimeConfig) -> RuntimeProcessHandle:
    process = multiprocessing.Process(target=_run_runtime_process, args=(config,))
    started_at = time.time()
    process.start()
    return RuntimeProcessHandle(
        process=process,
        started_at=started_at,
        started_epoch_ms=int(started_at * 1000),
    )


def _runtime_for_restart(config: RuntimeConfig) -> RuntimeConfig:
    lifecycle = replace(config.lifecycle, reset_outputs=False)
    return replace(config, lifecycle=lifecycle)


def _checkpoint_present(config: RuntimeConfig) -> bool:
    return config.output.prediction_checkpoint.exists()


def _collect_artifact(
    config: FaultRecoveryScenarioConfig,
    *,
    expected_phase_rows: dict[str, int],
) -> dict:
    return collect_prediction_response_artifact(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        input_topic=config.runtime.kafka.input_topic,
        prediction_topic=config.runtime.kafka.prediction_topic,
        run_tag=config.run_tag,
        artifact_output=config.artifact_output,
        expected_phase_rows=expected_phase_rows,
        timeout_sec=max(int(config.stream_wait_timeout_sec), int(config.collector_timeout_sec), 30),
        idle_sec=max(int(config.collector_idle_sec), 5),
    )


def _phase_rows(config: FaultRecoveryScenarioConfig, phase: str) -> int:
    summary = summarize_prediction_latency(config.artifact_output, phase=phase)
    return int(summary.get("artifact_rows_total") or 0)


def _first_artifact_emit_delay_seconds(
    config: FaultRecoveryScenarioConfig,
    *,
    phase: str,
    since_epoch_ms: int,
) -> float | str:
    if not config.artifact_output.exists():
        return ""

    import pandas as pd

    try:
        frame = pd.read_parquet(
            config.artifact_output,
            columns=["benchmark_phase", "emit_time"],
        )
    except Exception:
        return ""
    if frame.empty:
        return ""

    expected_phase = str(phase).strip().lower()
    phases = frame["benchmark_phase"].fillna("measure").astype(str).str.lower()
    emit_times = pd.to_datetime(
        frame.loc[phases == expected_phase, "emit_time"],
        errors="coerce",
        utc=True,
    ).dropna()
    if emit_times.empty:
        return ""

    first_emit_ms = int(emit_times.min().timestamp() * 1000)
    return max((first_emit_ms - int(since_epoch_ms)) / 1000.0, 0.0)


def _row_template(config: FaultRecoveryScenarioConfig) -> dict:
    return {
        "run_tag": config.run_tag,
        "repeat_index": config.repeat_index,
        "scenario": config.scenario,
        "fault_kind": config.fault_kind,
        "target_rps": config.target_rps,
        "model": config.runtime.model.reported_name,
        "feature_set": config.runtime.features.feature_set,
        "fault_ts_utc": "",
        "restart_ts_utc": "",
        "startup_seconds": "",
        "recovery_seconds": "",
        "initial_expected_rows": config.initial_replay.source.expected_rows,
        "initial_rows": "",
        "post_fault_expected_rows": (
            config.recovery_replay.source.expected_rows
            if config.recovery_replay is not None
            else ""
        ),
        "post_fault_rows": "",
        "drain_rps_after_fault": "",
        "proc_p95_ms_after_fault": "",
        "e2e_p95_ms_after_fault": "",
        "kafka_lag_max_after_fault": "",
        "kafka_lag_sampler_status": "",
        "kafka_lag_samples": "",
        "kafka_lag_peak_records": "",
        "kafka_lag_peak_phase": "",
        "kafka_lag_at_replay_end_records": "",
        "kafka_lag_clear_sec": "",
        "kafka_lag_area_records_sec": "",
        "kafka_lag_end_after_drain_records": "",
        "kafka_lag_timeseries_path": "",
        "checkpoint_present_before_restart": "",
        "checkpoint_reused": "",
        "status": "failed",
        "notes": "",
    }


def _fill_post_fault_summary(row: dict, config: FaultRecoveryScenarioConfig) -> None:
    summary = summarize_prediction_latency(config.artifact_output, phase="post_fault")
    row["post_fault_rows"] = summary.get("rows_total", "")
    row["drain_rps_after_fault"] = summary.get("drain_rps", "")
    row["proc_p95_ms_after_fault"] = summary.get("artifact_processing_p95_ms", "")
    row["e2e_p95_ms_after_fault"] = summary.get("artifact_e2e_p95_ms", "")


def _start_lag_sampler(config: FaultRecoveryScenarioConfig) -> KafkaLagSampler:
    return KafkaLagSampler(
        bootstrap_servers=config.runtime.kafka.bootstrap_servers,
        topic=config.runtime.kafka.input_topic,
        prediction_topic=config.runtime.kafka.prediction_topic,
        artifact_output=config.artifact_output,
        run_tag=config.run_tag,
    ).start()


def _merge_lag_summary(row: dict, summary: dict) -> None:
    row["kafka_lag_sampler_status"] = summary.get("kafka_lag_sampler_status", "")
    row["kafka_lag_samples"] = summary.get("kafka_lag_samples", "")
    row["kafka_lag_peak_records"] = summary.get("kafka_lag_peak_records", "")
    row["kafka_lag_peak_phase"] = summary.get("kafka_lag_peak_phase", "")
    row["kafka_lag_at_replay_end_records"] = summary.get("kafka_lag_at_replay_end_records", "")
    row["kafka_lag_clear_sec"] = summary.get("kafka_lag_clear_sec", "")
    row["kafka_lag_area_records_sec"] = summary.get("kafka_lag_area_records_sec", "")
    row["kafka_lag_end_after_drain_records"] = summary.get("kafka_lag_end_after_drain_records", "")
    row["kafka_lag_timeseries_path"] = summary.get("kafka_lag_timeseries_path", "")
    row["kafka_lag_max_after_fault"] = summary.get("kafka_lag_peak_records", "")


def _run_cold_start_scenario(config: FaultRecoveryScenarioConfig, row: dict) -> dict:
    reset_run_topics(config)
    lag_sampler = _start_lag_sampler(config)
    runtime_process = None
    try:
        runtime_process = start_runtime_process(config.runtime)
        time.sleep(max(int(config.stream_startup_wait_sec), 0))
        lag_sampler.set_phase("measure_cold_start_replay")
        run_replay_job(config.initial_replay)
        lag_sampler.mark("measure_replay_end")
        publish_input_sentinel(config.initial_replay.runtime)
        lag_sampler.set_control_tail_records(1)
        lag_sampler.mark("measure_sentinel_published")
        lag_sampler.set_phase("measure_cold_start_drain")
        runtime_process.wait_for_exit(timeout_sec=config.stream_wait_timeout_sec)

        lag_sampler.set_phase("measure_cold_start_artifact_collect")
        _collect_artifact(
            config,
            expected_phase_rows={"cold_start": int(config.initial_replay.source.expected_rows)},
        )
        lag_sampler.mark("measure_artifact_collected")
        _merge_lag_summary(row, lag_sampler.stop())
        row["initial_rows"] = _phase_rows(config, "cold_start")
        row["startup_seconds"] = _first_artifact_emit_delay_seconds(
            config,
            phase="cold_start",
            since_epoch_ms=runtime_process.started_epoch_ms,
        )
        expected_rows = int(config.initial_replay.source.expected_rows)
        if int(row["initial_rows"] or 0) < expected_rows * float(config.pre_fault_min_rows_ratio):
            row["status"] = "artifact_incomplete"
            row["notes"] = f"cold_start_rows={row['initial_rows']} expected={expected_rows}"
        else:
            row["status"] = "ok"
            row["notes"] = ""
        return row
    except Exception as exc:
        if runtime_process is not None:
            runtime_process.kill()
        _merge_lag_summary(row, lag_sampler.stop())
        row["status"] = "failed"
        row["notes"] = str(exc)
        return row


def _run_spark_crash_scenario(config: FaultRecoveryScenarioConfig, row: dict) -> dict:
    if config.recovery_replay is None:
        row["status"] = "failed"
        row["notes"] = "missing recovery replay config"
        return row

    reset_run_topics(config)
    lag_sampler = _start_lag_sampler(config)
    runtime_process = None
    restart_process = None
    try:
        runtime_process = start_runtime_process(config.runtime)
        time.sleep(max(int(config.stream_startup_wait_sec), 0))
        lag_sampler.set_phase("pre_fault_replay")
        run_replay_job(config.initial_replay)
        lag_sampler.mark("pre_fault_replay_end")
        lag_sampler.set_phase("pre_fault_artifact_collect")
        _collect_artifact(
            config,
            expected_phase_rows={"pre_fault": int(config.initial_replay.source.expected_rows)},
        )
        pre_fault_rows = _phase_rows(config, "pre_fault")
        row["initial_rows"] = pre_fault_rows
        expected_initial_rows = int(config.initial_replay.source.expected_rows)
        if pre_fault_rows < expected_initial_rows * float(config.pre_fault_min_rows_ratio):
            row["status"] = "pre_fault_incomplete"
            row["notes"] = f"pre_fault_rows={pre_fault_rows} expected={expected_initial_rows}"
            runtime_process.kill()
            _merge_lag_summary(row, lag_sampler.stop())
            return row

        lag_sampler.set_phase("fault")
        row["fault_ts_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        runtime_process.kill()
        row["checkpoint_present_before_restart"] = _checkpoint_present(config.runtime)

        restart_runtime = _runtime_for_restart(config.runtime)
        lag_sampler.set_phase("restart")
        restart_process = start_runtime_process(restart_runtime)
        row["restart_ts_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        row["checkpoint_reused"] = (
            bool(row["checkpoint_present_before_restart"])
            and not bool(restart_runtime.lifecycle.reset_outputs)
        )
        try:
            time.sleep(max(int(config.stream_startup_wait_sec), 0))
            lag_sampler.set_phase("measure_post_fault_replay")
            run_replay_job(config.recovery_replay)
            lag_sampler.mark("measure_replay_end")
            publish_input_sentinel(config.recovery_replay.runtime)
            lag_sampler.set_control_tail_records(1)
            lag_sampler.mark("measure_sentinel_published")
            lag_sampler.set_phase("measure_post_fault_drain")
            restart_process.wait_for_exit(timeout_sec=config.stream_wait_timeout_sec)
        except Exception:
            restart_process.kill()
            raise

        lag_sampler.set_phase("measure_post_fault_artifact_collect")
        _collect_artifact(
            config,
            expected_phase_rows={
                "pre_fault": int(config.initial_replay.source.expected_rows),
                "post_fault": int(config.recovery_replay.source.expected_rows),
            },
        )
        lag_sampler.mark("measure_artifact_collected")
        _merge_lag_summary(row, lag_sampler.stop())
        _fill_post_fault_summary(row, config)
        row["recovery_seconds"] = _first_artifact_emit_delay_seconds(
            config,
            phase="post_fault",
            since_epoch_ms=restart_process.started_epoch_ms,
        )

        expected_post_fault_rows = int(config.recovery_replay.source.expected_rows)
        post_fault_rows = int(row["post_fault_rows"] or 0)
        if post_fault_rows < expected_post_fault_rows * float(config.post_fault_min_rows_ratio):
            row["status"] = "post_fault_incomplete"
            row["notes"] = f"post_fault_rows={post_fault_rows} expected={expected_post_fault_rows}"
        elif not row["checkpoint_reused"]:
            row["status"] = "checkpoint_missing"
            row["notes"] = "checkpoint was not present before restart"
        else:
            row["status"] = "ok"
            row["notes"] = ""
        return row
    except Exception as exc:
        if runtime_process is not None:
            runtime_process.kill()
        if restart_process is not None:
            restart_process.kill()
        _merge_lag_summary(row, lag_sampler.stop())
        row["status"] = "failed"
        row["notes"] = str(exc)
        return row


def run_fault_recovery_scenario(config: FaultRecoveryScenarioConfig) -> dict:
    row = _row_template(config)
    if config.fault_kind == "none":
        return _run_cold_start_scenario(config, row)
    if config.fault_kind == "spark_process_crash":
        return _run_spark_crash_scenario(config, row)
    row["status"] = "failed"
    row["notes"] = f"unsupported fault kind: {config.fault_kind}"
    return row


def run_fault_recovery_matrix(config: FaultRecoveryMatrixConfig) -> int:
    rows: list[dict] = []
    for scenario_config in config.scenarios:
        row = run_fault_recovery_scenario(scenario_config)
        cleanup_run_topics(scenario_config, ignore_errors=True)
        rows.append(row)
    write_summary_rows(config.summary_csv, rows)
    return 0


run = run_fault_recovery_matrix
