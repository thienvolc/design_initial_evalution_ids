from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.config import resolve_kafka_bootstrap_servers
from ids_platform.streaming.matrices.common import (
    annotate_sut_debug_summary,
    collect_matching_metrics,
    describe_process_startup_state,
    wait_for_log_quiescence,
    wait_for_log_patterns,
    wait_for_process_startup,
    write_metrics_timeseries,
    write_summary_rows,
)
from ids_platform.streaming.orchestration.fault_matrix import (
    cleanup_stream_processes,
    fetch_metric,
    normalize_rate_schedule,
    parse_iso_datetime,
    replay_with_retries,
    restart_service,
    safe_float,
    scenario_row_template,
    start_stream_process,
    stop_stream_process,
    wait_for_kafka_bootstrap_ready,
    wait_for_kafka_topics_ready,
    wait_for_process_exit,
    wait_for_stream_shutdown,
)
from ids_platform.streaming.replay.config import parse_rate_schedule


@dataclass(frozen=True)
class LayerCFaultMatrixOptions:
    config: str
    model: str
    feature_set: str
    scenarios: tuple[str, ...]
    warmup_rows: int
    warmup_rows_per_sec: float
    warmup_rate_schedule: str
    fault_delay_sec: int
    post_fault_rows: int
    post_fault_rows_per_sec: float
    batch_size: int
    trace_input_parquet: str
    trace_order_column: str
    slowdown_rows_per_sec: float
    producer_restart_pause_sec: int
    replay_retries: int
    replay_retry_wait_sec: int
    stream_run_seconds: int
    startup_wait_sec: int
    metrics_timeout_sec: int
    execution_mode: str
    python_executable: str
    bootstrap_servers: str
    summary_csv: str


def _log_phase(event: str, **fields) -> None:
    parts = [f"[phase] matrix=layer_c event={event}"]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" ".join(parts), flush=True)


def _next_run_tag(*, index: int, scenario: str) -> str:
    return f"layerC_{index:02d}_{scenario}_{int(time.time() * 1000)}"


def _validate_runtime_metric(payload: dict | None) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "metric payload missing"
    event_type = str(payload.get("event_type", "")).strip().lower()
    if event_type and event_type != "batch_metrics":
        return False, f"unexpected metric event_type={event_type}"
    try:
        rows_value = int(payload.get("rows") or 0)
    except Exception:
        rows_value = 0
    if rows_value <= 0:
        return False, "metric rows missing_or_zero"
    return True, ""


def _load_host_kafka_runtime_targets(*, config_path: str, bootstrap_override: str) -> tuple[str, list[str]]:
    cfg = load_yaml_mapping(resolve_project_path(config_path))
    kafka_cfg = cfg.get("kafka") or {}
    bootstrap_servers = resolve_kafka_bootstrap_servers(
        bootstrap_override or str(kafka_cfg.get("bootstrap_servers", "kafka:29092")),
        execution_mode="host",
    )
    return bootstrap_servers, [
        str(kafka_cfg.get("input_topic", "ids.raw.flows")),
        str(kafka_cfg.get("output_topic", "ids.predictions.binary")),
        str(kafka_cfg.get("metrics_topic", "ids.metrics")),
    ]


def _load_docker_kafka_bootstrap(*, config_path: str, bootstrap_override: str) -> str:
    cfg = load_yaml_mapping(resolve_project_path(config_path))
    kafka_cfg = cfg.get("kafka") or {}
    return resolve_kafka_bootstrap_servers(
        bootstrap_override or str(kafka_cfg.get("bootstrap_servers", "kafka:29092")),
        execution_mode="docker",
    )


def _estimate_replay_seconds_from_schedule(*, rows: int, rate_schedule: str) -> float:
    remaining_rows = max(int(rows), 0)
    if remaining_rows <= 0:
        return 0.0
    total_seconds = 0.0
    for rows_per_second, duration_seconds in parse_rate_schedule(rate_schedule):
        segment_capacity = rows_per_second * duration_seconds
        if remaining_rows <= segment_capacity:
            total_seconds += remaining_rows / rows_per_second
            return total_seconds
        total_seconds += duration_seconds
        remaining_rows -= int(segment_capacity)
    return total_seconds


def _estimate_layer_c_stream_runtime_seconds(
    *,
    options: LayerCFaultMatrixOptions,
    scenario: str,
    warmup_rate_schedule: str,
    is_restart: bool = False,
) -> int:
    buffer_seconds = 300
    startup_budget = max(int(options.startup_wait_sec), 0)
    fault_delay_seconds = max(int(options.fault_delay_sec), 0)

    warmup_seconds = 0.0
    if not is_restart:
        if warmup_rate_schedule.strip():
            warmup_seconds = _estimate_replay_seconds_from_schedule(
                rows=options.warmup_rows,
                rate_schedule=warmup_rate_schedule,
            )
        elif float(options.warmup_rows_per_sec) > 0:
            warmup_seconds = float(options.warmup_rows) / float(options.warmup_rows_per_sec)

    if scenario == "producer_restart":
        post_fault_seconds = 0.0
        if float(options.post_fault_rows_per_sec) > 0:
            post_fault_seconds = float(options.post_fault_rows) / float(options.post_fault_rows_per_sec)
        post_fault_seconds += max(int(options.producer_restart_pause_sec), 0)
    elif scenario == "network_slowdown":
        slowdown_rps = max(float(options.slowdown_rows_per_sec), 0.0)
        post_fault_seconds = (
            float(options.post_fault_rows) / slowdown_rps if slowdown_rps > 0 else 0.0
        )
    else:
        replay_rps = max(float(options.post_fault_rows_per_sec), 0.0)
        post_fault_seconds = (
            float(options.post_fault_rows) / replay_rps if replay_rps > 0 else 0.0
        )

    estimated_seconds = startup_budget + warmup_seconds + fault_delay_seconds + post_fault_seconds + buffer_seconds
    return max(int(estimated_seconds), int(options.stream_run_seconds), 1)


def _execute_scenario(
    options: LayerCFaultMatrixOptions,
    scenario: str,
    index: int,
    warmup_rate_schedule: str,
) -> dict:
    scenario_started_at = time.time()
    run_tag = _next_run_tag(index=index, scenario=scenario)
    _log_phase("scenario_start", run_tag=run_tag, scenario=scenario, run_index=index)
    row = scenario_row_template(
        run_tag=run_tag,
        scenario=scenario,
        model=options.model,
        feature_set=options.feature_set,
    )

    stream_process = None
    expect_natural_shutdown = False
    try:
        cleanup_stream_processes(run_tag=run_tag, execution_mode=options.execution_mode)
        time.sleep(2)
        stream_run_seconds = _estimate_layer_c_stream_runtime_seconds(
            options=options,
            scenario=scenario,
            warmup_rate_schedule=warmup_rate_schedule,
            is_restart=False,
        )

        _log_phase("stream_start", run_tag=run_tag, scenario=scenario, reset_checkpoint=True)
        stream_process = start_stream_process(
            config=options.config,
            model=options.model,
            feature_set=options.feature_set,
            run_tag=run_tag,
            load_profile=scenario,
            run_seconds=stream_run_seconds,
            reset_checkpoint=True,
            stop_on_input_sentinel=(scenario != "spark_process_restart"),
            execution_mode=options.execution_mode,
            python_executable=options.python_executable,
            bootstrap_servers=options.bootstrap_servers,
        )
        _log_phase(
            "stream_runtime_log",
            run_tag=run_tag,
            scenario=scenario,
            path=str(getattr(stream_process, "ids_log_path", "") or ""),
        )
        if not wait_for_process_startup(
            stream_process,
            startup_wait_sec=options.startup_wait_sec,
            ready_log_path=str(getattr(stream_process, "ids_log_path", "") or ""),
            ready_pattern=f"[stream] event=job_start run_tag={run_tag}",
            require_ready_marker=True,
        ):
            log_path = str(getattr(stream_process, "ids_log_path", "") or "")
            startup_debug = describe_process_startup_state(
                stream_process,
                log_path=log_path,
                ready_pattern=f"[stream] event=job_start run_tag={run_tag}",
            )
            row["notes"] = (
                f"stream process not ready before warmup; log={log_path}; {startup_debug}"
                if log_path else "stream process not ready before warmup"
            )
            _log_phase("stream_start_failed", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)

        _log_phase("warmup_replay_start", run_tag=run_tag, scenario=scenario, warmup_rows=options.warmup_rows)
        replay_with_retries(
            config=options.config,
            run_tag=run_tag,
            rows=options.warmup_rows,
            batch_size=options.batch_size,
            input_parquet=options.trace_input_parquet,
            trace_order_column=options.trace_order_column,
            rows_per_sec=max(float(options.warmup_rows_per_sec), 0.0),
            rate_schedule=warmup_rate_schedule,
            retries=options.replay_retries,
            retry_wait_sec=options.replay_retry_wait_sec,
            emit_input_sentinel=False,
            execution_mode=options.execution_mode,
            python_executable=options.python_executable,
            bootstrap_servers=options.bootstrap_servers,
        )
        _log_phase("warmup_replay_done", run_tag=run_tag, scenario=scenario)

        _log_phase("warmup_metric_wait_start", run_tag=run_tag, scenario=scenario, timeout_sec=options.metrics_timeout_sec)
        warmup_metric = fetch_metric(
            config=options.config,
            run_tag=run_tag,
            timeout_sec=options.metrics_timeout_sec,
            execution_mode=options.execution_mode,
            python_executable=options.python_executable,
            bootstrap_servers=options.bootstrap_servers,
        )
        if warmup_metric is None:
            row["notes"] = "warmup metric not found"
            _log_phase("warmup_metric_missing", run_tag=run_tag, scenario=scenario)
            return annotate_sut_debug_summary(row)
        warmup_metric_ok, warmup_metric_reason = _validate_runtime_metric(warmup_metric)
        if not warmup_metric_ok:
            row["notes"] = f"warmup metric invalid: {warmup_metric_reason}"
            _log_phase("warmup_metric_invalid", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)
        _log_phase("warmup_metric_found", run_tag=run_tag, scenario=scenario)

        if options.fault_delay_sec > 0:
            _log_phase("fault_delay_wait_start", run_tag=run_tag, scenario=scenario, fault_delay_sec=options.fault_delay_sec)
            time.sleep(max(int(options.fault_delay_sec), 1))

        fault_time = datetime.now(timezone.utc)
        fault_epoch_ms = int(time.time() * 1000)
        row["fault_ts_utc"] = fault_time.isoformat(timespec="seconds")
        _log_phase("fault_inject_start", run_tag=run_tag, scenario=scenario, fault_ts_utc=row["fault_ts_utc"])

        if scenario == "kafka_restart":
            restart_service("kafka", execution_mode=options.execution_mode)
            host_bootstrap_servers, kafka_topics = _load_host_kafka_runtime_targets(
                config_path=options.config,
                bootstrap_override=options.bootstrap_servers,
            )
            docker_bootstrap_servers = _load_docker_kafka_bootstrap(
                config_path=options.config,
                bootstrap_override=options.bootstrap_servers,
            )
            host_bootstrap_ready = wait_for_kafka_bootstrap_ready(
                bootstrap_servers=host_bootstrap_servers,
                timeout_sec=min(max(options.metrics_timeout_sec, 60), 120),
                poll_sec=2.0,
                consecutive_successes=2,
                execution_mode="host",
            )
            docker_bootstrap_ready = wait_for_kafka_bootstrap_ready(
                bootstrap_servers=docker_bootstrap_servers,
                timeout_sec=min(max(options.metrics_timeout_sec, 60), 120),
                poll_sec=2.0,
                consecutive_successes=2,
                execution_mode="docker",
            )
            if not host_bootstrap_ready or not docker_bootstrap_ready:
                row["notes"] = (
                    "kafka bootstrap not ready after restart; "
                    f"host_bootstrap={host_bootstrap_servers}; docker_bootstrap={docker_bootstrap_servers}; "
                    f"host_ready={host_bootstrap_ready}; docker_ready={docker_bootstrap_ready}"
                )
                _log_phase("kafka_restart_bootstrap_not_ready", run_tag=run_tag, scenario=scenario, notes=row["notes"])
                return annotate_sut_debug_summary(row)
            kafka_ready = wait_for_kafka_topics_ready(
                bootstrap_servers=host_bootstrap_servers,
                topic_names=kafka_topics,
                timeout_sec=min(max(options.metrics_timeout_sec, 60), 120),
                poll_sec=2.0,
            )
            if not kafka_ready:
                row["notes"] = (
                    "kafka topics not ready after restart; "
                    f"bootstrap={host_bootstrap_servers}; topics={','.join(kafka_topics)}"
                )
                _log_phase("kafka_restart_not_ready", run_tag=run_tag, scenario=scenario, notes=row["notes"])
                return annotate_sut_debug_summary(row)
            time.sleep(8)
            _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action="restart_service:kafka")
        elif scenario == "spark_process_restart":
            restart_log_path = str(getattr(stream_process, "ids_log_path", "") or "")
            _log_phase("stream_drain_wait_start", run_tag=run_tag, scenario=scenario)
            drained = wait_for_log_quiescence(
                process=stream_process,
                log_path=restart_log_path,
                idle_sec=15.0,
                timeout_sec=min(max(options.metrics_timeout_sec, 30), 120),
                poll_seconds=0.5,
            )
            if not drained:
                row["notes"] = (
                    f"stream did not quiesce before restart; log={restart_log_path}"
                    if restart_log_path else "stream did not quiesce before restart"
                )
                _log_phase("stream_drain_wait_timeout", run_tag=run_tag, scenario=scenario, notes=row["notes"])
                return annotate_sut_debug_summary(row)
            _log_phase("stream_drain_wait_done", run_tag=run_tag, scenario=scenario)
            stop_stream_process(stream_process)
            _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action="stop_stream_process")
            _log_phase("stream_restart_start", run_tag=run_tag, scenario=scenario)
            restart_stream_run_seconds = _estimate_layer_c_stream_runtime_seconds(
                options=options,
                scenario=scenario,
                warmup_rate_schedule=warmup_rate_schedule,
                is_restart=True,
            )
            stream_process = start_stream_process(
                config=options.config,
                model=options.model,
                feature_set=options.feature_set,
                run_tag=run_tag,
                load_profile=scenario,
                run_seconds=restart_stream_run_seconds,
                reset_checkpoint=False,
                stop_on_input_sentinel=True,
                execution_mode=options.execution_mode,
                python_executable=options.python_executable,
                bootstrap_servers=options.bootstrap_servers,
            )
            _log_phase(
                "stream_runtime_log",
                run_tag=run_tag,
                scenario=scenario,
                path=str(getattr(stream_process, "ids_log_path", "") or ""),
            )
            if not wait_for_process_startup(
                stream_process,
                startup_wait_sec=options.startup_wait_sec,
                ready_log_path=str(getattr(stream_process, "ids_log_path", "") or ""),
                ready_pattern=f"[stream] event=job_start run_tag={run_tag}",
                require_ready_marker=True,
            ):
                log_path = str(getattr(stream_process, "ids_log_path", "") or "")
                startup_debug = describe_process_startup_state(
                    stream_process,
                    log_path=log_path,
                    ready_pattern=f"[stream] event=job_start run_tag={run_tag}",
                )
                row["notes"] = (
                    f"stream process not ready after restart; log={log_path}; {startup_debug}"
                    if log_path else "stream process not ready after restart"
                )
                _log_phase("stream_restart_failed", run_tag=run_tag, scenario=scenario, notes=row["notes"])
                return annotate_sut_debug_summary(row)
            _log_phase("stream_restart_done", run_tag=run_tag, scenario=scenario)
        elif scenario not in {"producer_restart", "network_slowdown"}:
            row["notes"] = f"unsupported scenario={scenario}"
            _log_phase("fault_inject_unsupported", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)
        else:
            _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action=scenario)

        if stream_process.poll() is not None:
            row["notes"] = "stream process exited after fault"
            _log_phase("stream_exited_after_fault", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)

        if scenario == "producer_restart":
            first_rows = max(options.post_fault_rows // 2, 1)
            second_rows = max(options.post_fault_rows - first_rows, 1)
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=first_rows, segment="first")
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=first_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
                emit_input_sentinel=False,
                execution_mode=options.execution_mode,
                python_executable=options.python_executable,
                bootstrap_servers=options.bootstrap_servers,
            )
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=first_rows, segment="first")
            time.sleep(max(options.producer_restart_pause_sec, 1))
            _log_phase("producer_pause_done", run_tag=run_tag, scenario=scenario, pause_sec=max(options.producer_restart_pause_sec, 1))
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=second_rows, segment="second")
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=second_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
                emit_input_sentinel=True,
                execution_mode=options.execution_mode,
                python_executable=options.python_executable,
                bootstrap_servers=options.bootstrap_servers,
            )
            expect_natural_shutdown = True
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=second_rows, segment="second")
        elif scenario == "network_slowdown":
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows, rows_per_sec=max(float(options.slowdown_rows_per_sec), 1.0))
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=options.post_fault_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.slowdown_rows_per_sec), 1.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
                emit_input_sentinel=True,
                execution_mode=options.execution_mode,
                python_executable=options.python_executable,
                bootstrap_servers=options.bootstrap_servers,
            )
            expect_natural_shutdown = True
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows)
        else:
            _log_phase("post_fault_replay_start", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows, rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0))
            replay_with_retries(
                config=options.config,
                run_tag=run_tag,
                rows=options.post_fault_rows,
                batch_size=options.batch_size,
                input_parquet=options.trace_input_parquet,
                trace_order_column=options.trace_order_column,
                rows_per_sec=max(float(options.post_fault_rows_per_sec), 0.0),
                retries=options.replay_retries,
                retry_wait_sec=options.replay_retry_wait_sec,
                emit_input_sentinel=True,
                execution_mode=options.execution_mode,
                python_executable=options.python_executable,
                bootstrap_servers=options.bootstrap_servers,
            )
            expect_natural_shutdown = True
            _log_phase("post_fault_replay_done", run_tag=run_tag, scenario=scenario, rows=options.post_fault_rows)

        _log_phase("recovery_wait_start", run_tag=run_tag, scenario=scenario, timeout_sec=options.metrics_timeout_sec)
        recovered_metric = fetch_metric(
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
            row["notes"] = "post-fault metric not found"
            _log_phase("recovery_wait_timeout", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)
        recovered_metric_ok, recovered_metric_reason = _validate_runtime_metric(recovered_metric)
        if not recovered_metric_ok:
            row["notes"] = f"post-fault metric invalid: {recovered_metric_reason}"
            _log_phase("recovery_wait_invalid", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return annotate_sut_debug_summary(row)
        _log_phase("recovery_wait_done", run_tag=run_tag, scenario=scenario)

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
        row["status"] = "ok"
        _log_phase(
            "scenario_summary",
            run_tag=run_tag,
            scenario=scenario,
            status=row["status"],
            recovery_seconds=row["recovery_seconds"],
            rows_after_fault=row["rows_after_fault"],
            elapsed_sec=f"{time.time() - scenario_started_at:.2f}",
        )
        return annotate_sut_debug_summary(row)
    except Exception as exc:
        row["notes"] = str(exc)
        _log_phase(
            "scenario_error",
            run_tag=run_tag,
            scenario=scenario,
            error=type(exc).__name__,
            elapsed_sec=f"{time.time() - scenario_started_at:.2f}",
        )
        return annotate_sut_debug_summary(row)
    finally:
        graceful_shutdown_complete = False
        if expect_natural_shutdown and stream_process is not None and stream_process.poll() is None:
            log_path = str(getattr(stream_process, "ids_log_path", "") or "")
            graceful_pattern = wait_for_log_patterns(
                process=stream_process,
                log_path=log_path,
                patterns=[
                    f"[stream] event=stop_condition_met run_tag={run_tag} reason=input_sentinel",
                    f"[stream] event=job_stop run_tag={run_tag}",
                ],
                timeout_sec=120.0,
                poll_seconds=0.5,
            )
            if graceful_pattern:
                _log_phase(
                    "stream_graceful_stop_marker_seen",
                    run_tag=run_tag,
                    scenario=scenario,
                    marker=graceful_pattern,
                )
            graceful_shutdown_complete = wait_for_stream_shutdown(
                run_tag=run_tag,
                execution_mode=options.execution_mode,
                timeout_sec=300,
                poll_sec=0.5,
                settle_sec=2.0,
            )
            if graceful_shutdown_complete and wait_for_process_exit(stream_process, timeout_sec=30):
                stop_stream_process(stream_process)
                stream_process = None
        if stream_process is not None and stream_process.poll() is not None:
            stop_stream_process(stream_process)
        elif stream_process is not None and stream_process.poll() is None and not graceful_shutdown_complete:
            stop_stream_process(stream_process)
        _log_phase("scenario_cleanup_done", run_tag=run_tag, scenario=scenario, elapsed_sec=f"{time.time() - scenario_started_at:.2f}")


def _collect_scenario_metrics(
    *,
    config: str,
    run_tag: str,
    timeout_sec: int,
    start_timestamp_ms: int,
    execution_mode: str,
    bootstrap_servers_override: str,
) -> list[dict]:
    cfg = load_yaml_mapping(resolve_project_path(config))
    kafka_cfg = cfg.get("kafka") or {}
    # Layer C orchestration stays on the host so it can inject Docker faults.
    # This post-run collector therefore needs host-resolvable bootstrap servers
    # even when the SUT/replay subprocesses run with execution_mode=docker.
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


def run(options: LayerCFaultMatrixOptions) -> int:
    warmup_rate_schedule = normalize_rate_schedule(options.warmup_rate_schedule)
    if warmup_rate_schedule and options.warmup_rows_per_sec > 0:
        raise ValueError("Use either --warmup-rows-per-sec or --warmup-rate-schedule, not both")

    summary_rows: list[dict] = []
    for index, scenario in enumerate(options.scenarios, start=1):
        scenario_run_start_ms = int(time.time() * 1000)
        row = _execute_scenario(
            options,
            scenario,
            index,
            warmup_rate_schedule,
        )
        metrics_rows = _collect_scenario_metrics(
            config=options.config,
            run_tag=str(row["run_tag"]),
            timeout_sec=options.metrics_timeout_sec,
            start_timestamp_ms=scenario_run_start_ms,
            execution_mode=options.execution_mode,
            bootstrap_servers_override=options.bootstrap_servers,
        )
        timeseries_path = write_metrics_timeseries(metrics_rows, run_tag=str(row["run_tag"]))
        if timeseries_path is not None:
            _log_phase("timeseries_write_done", run_tag=row["run_tag"], scenario=scenario, path=timeseries_path)
        summary_rows.append(row)
        _log_phase("run_summary", run_tag=row["run_tag"], scenario=scenario, status=row["status"], recovery_seconds=row["recovery_seconds"], notes=row["notes"])

    summary_path = resolve_project_path(options.summary_csv)
    write_summary_rows(summary_path, summary_rows)
    print(f"Saved Layer C summary: {summary_path}", flush=True)
    return 0
