from __future__ import annotations

import time

from .logging_utils import _log_phase, _mark_row_failed
from .runtime import (
    KAFKA_BOOTSTRAP_READY_MAX_TIMEOUT_SEC,
    KAFKA_BOOTSTRAP_READY_MIN_TIMEOUT_SEC,
    KAFKA_LATE_READY_RECHECK_POLL_SEC,
    KAFKA_LATE_READY_RECHECK_TIMEOUT_SEC,
    KAFKA_POST_READY_SETTLE_POLL_SEC,
    KAFKA_POST_READY_SETTLE_TIMEOUT_SEC,
    KAFKA_POST_RESTART_GRACE_SEC,
    KAFKA_RESTART_POLL_SEC,
    KAFKA_RESTART_REQUIRED_SUCCESSES,
    KAFKA_SETTLE_DELAY_SEC,
    KAFKA_TOPIC_READY_MAX_TIMEOUT_SEC,
    KAFKA_TOPIC_READY_MIN_TIMEOUT_SEC,
)
from .types import LayerCFaultMatrixOptions, LayerCKafkaReadinessResult


def _entrypoint_module():
    from ids_platform.streaming.evaluation.matrices import layer_c_fault_matrix

    return layer_c_fault_matrix


def wait_for_kafka_topic_readiness(
    *,
    bootstrap_servers: str,
    topic_names: list[str],
    timeout_sec: int,
    poll_sec: float,
    execution_mode: str,
) -> bool:
    return _entrypoint_module().wait_for_kafka_topics_ready(
        bootstrap_servers=bootstrap_servers,
        topic_names=topic_names,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        execution_mode=execution_mode,
        consecutive_successes=KAFKA_RESTART_REQUIRED_SUCCESSES,
    )


def describe_kafka_readiness_state(
    *,
    host_bootstrap_servers: str,
    docker_bootstrap_servers: str,
    kafka_topics: list[str],
    host_ready: bool,
    docker_ready: bool,
) -> LayerCKafkaReadinessResult:
    return LayerCKafkaReadinessResult(
        host_ready=host_ready,
        docker_ready=docker_ready,
        host_state=_entrypoint_module().describe_kafka_topics_state(
            bootstrap_servers=host_bootstrap_servers,
            topic_names=kafka_topics,
            execution_mode="host",
        ),
        docker_state=_entrypoint_module().describe_kafka_topics_state(
            bootstrap_servers=docker_bootstrap_servers,
            topic_names=kafka_topics,
            execution_mode="docker",
        ),
    )


def maybe_recheck_kafka_late_ready(
    *,
    run_tag: str,
    scenario: str,
    host_bootstrap_servers: str,
    docker_bootstrap_servers: str,
    kafka_topics: list[str],
    readiness: LayerCKafkaReadinessResult,
) -> LayerCKafkaReadinessResult:
    if readiness.host_state != "ready" or readiness.docker_state != "ready":
        return readiness

    _log_phase(
        "kafka_restart_late_ready_recheck_start",
        run_tag=run_tag,
        scenario=scenario,
        host_bootstrap=host_bootstrap_servers,
        docker_bootstrap=docker_bootstrap_servers,
    )
    host_ready = wait_for_kafka_topic_readiness(
        bootstrap_servers=host_bootstrap_servers,
        topic_names=kafka_topics,
        timeout_sec=KAFKA_LATE_READY_RECHECK_TIMEOUT_SEC,
        poll_sec=KAFKA_LATE_READY_RECHECK_POLL_SEC,
        execution_mode="host",
    )
    docker_ready = wait_for_kafka_topic_readiness(
        bootstrap_servers=docker_bootstrap_servers,
        topic_names=kafka_topics,
        timeout_sec=KAFKA_LATE_READY_RECHECK_TIMEOUT_SEC,
        poll_sec=KAFKA_LATE_READY_RECHECK_POLL_SEC,
        execution_mode="docker",
    )
    updated = describe_kafka_readiness_state(
        host_bootstrap_servers=host_bootstrap_servers,
        docker_bootstrap_servers=docker_bootstrap_servers,
        kafka_topics=kafka_topics,
        host_ready=host_ready,
        docker_ready=docker_ready,
    )
    if updated.host_ready and updated.docker_ready:
        _log_phase(
            "kafka_restart_late_ready_recheck_done",
            run_tag=run_tag,
            scenario=scenario,
            host_bootstrap=host_bootstrap_servers,
            docker_bootstrap=docker_bootstrap_servers,
        )
        return updated

    if updated.host_state == "ready" and updated.docker_state == "ready":
        _log_phase(
            "kafka_restart_late_ready_state_override",
            run_tag=run_tag,
            scenario=scenario,
            host_bootstrap=host_bootstrap_servers,
            docker_bootstrap=docker_bootstrap_servers,
        )
        return LayerCKafkaReadinessResult(
            host_ready=True,
            docker_ready=True,
            host_state=updated.host_state,
            docker_state=updated.docker_state,
        )
    return updated


def maybe_override_kafka_settle_stability(
    *,
    run_tag: str,
    scenario: str,
    host_bootstrap_servers: str,
    docker_bootstrap_servers: str,
    readiness: LayerCKafkaReadinessResult,
) -> LayerCKafkaReadinessResult:
    if readiness.host_ready and readiness.docker_ready:
        return readiness
    if readiness.host_state != "ready" or readiness.docker_state != "ready":
        return readiness
    _log_phase(
        "kafka_restart_settle_state_override",
        run_tag=run_tag,
        scenario=scenario,
        host_bootstrap=host_bootstrap_servers,
        docker_bootstrap=docker_bootstrap_servers,
    )
    return LayerCKafkaReadinessResult(
        host_ready=True,
        docker_ready=True,
        host_state=readiness.host_state,
        docker_state=readiness.docker_state,
    )


def handle_kafka_restart_fault(
    *,
    options: LayerCFaultMatrixOptions,
    run_tag: str,
    scenario: str,
    row: dict,
) -> bool:
    _entrypoint_module().restart_service("kafka", execution_mode=options.execution_mode)
    host_bootstrap_servers, kafka_topics = _entrypoint_module()._load_host_kafka_runtime_targets(
        config_path=options.config,
        bootstrap_override=options.bootstrap_servers,
    )
    docker_bootstrap_servers = _entrypoint_module()._load_docker_kafka_bootstrap(
        config_path=options.config,
        bootstrap_override=options.bootstrap_servers,
    )
    host_bootstrap_ready = _entrypoint_module().wait_for_kafka_bootstrap_ready(
        bootstrap_servers=host_bootstrap_servers,
        timeout_sec=min(max(options.metrics_timeout_sec, KAFKA_BOOTSTRAP_READY_MIN_TIMEOUT_SEC), KAFKA_BOOTSTRAP_READY_MAX_TIMEOUT_SEC),
        poll_sec=KAFKA_RESTART_POLL_SEC,
        consecutive_successes=KAFKA_RESTART_REQUIRED_SUCCESSES,
        execution_mode="host",
    )
    docker_bootstrap_ready = _entrypoint_module().wait_for_kafka_bootstrap_ready(
        bootstrap_servers=docker_bootstrap_servers,
        timeout_sec=min(max(options.metrics_timeout_sec, KAFKA_BOOTSTRAP_READY_MIN_TIMEOUT_SEC), KAFKA_BOOTSTRAP_READY_MAX_TIMEOUT_SEC),
        poll_sec=KAFKA_RESTART_POLL_SEC,
        consecutive_successes=KAFKA_RESTART_REQUIRED_SUCCESSES,
        execution_mode="docker",
    )
    if not host_bootstrap_ready or not docker_bootstrap_ready:
        _mark_row_failed(
            row,
            "kafka bootstrap not ready after restart; "
            f"host_bootstrap={host_bootstrap_servers}; docker_bootstrap={docker_bootstrap_servers}; "
            f"host_ready={host_bootstrap_ready}; docker_ready={docker_bootstrap_ready}",
        )
        _log_phase("kafka_restart_bootstrap_not_ready", run_tag=run_tag, scenario=scenario, notes=row["notes"])
        return False

    kafka_ready = wait_for_kafka_topic_readiness(
        bootstrap_servers=host_bootstrap_servers,
        topic_names=kafka_topics,
        timeout_sec=min(max(options.metrics_timeout_sec, KAFKA_TOPIC_READY_MIN_TIMEOUT_SEC), KAFKA_TOPIC_READY_MAX_TIMEOUT_SEC),
        poll_sec=KAFKA_RESTART_POLL_SEC,
        execution_mode="host",
    )
    docker_kafka_ready = wait_for_kafka_topic_readiness(
        bootstrap_servers=docker_bootstrap_servers,
        topic_names=kafka_topics,
        timeout_sec=min(max(options.metrics_timeout_sec, KAFKA_TOPIC_READY_MIN_TIMEOUT_SEC), KAFKA_TOPIC_READY_MAX_TIMEOUT_SEC),
        poll_sec=KAFKA_RESTART_POLL_SEC,
        execution_mode="docker",
    )
    if not kafka_ready or not docker_kafka_ready:
        readiness = describe_kafka_readiness_state(
            host_bootstrap_servers=host_bootstrap_servers,
            docker_bootstrap_servers=docker_bootstrap_servers,
            kafka_topics=kafka_topics,
            host_ready=kafka_ready,
            docker_ready=docker_kafka_ready,
        )
        readiness = maybe_recheck_kafka_late_ready(
            run_tag=run_tag,
            scenario=scenario,
            host_bootstrap_servers=host_bootstrap_servers,
            docker_bootstrap_servers=docker_bootstrap_servers,
            kafka_topics=kafka_topics,
            readiness=readiness,
        )
        _mark_row_failed(
            row,
            "kafka topics not ready after restart; "
            f"host_bootstrap={host_bootstrap_servers}; docker_bootstrap={docker_bootstrap_servers}; "
            f"topics={','.join(kafka_topics)}; host_ready={readiness.host_ready}; docker_ready={readiness.docker_ready}; "
            f"host_state={readiness.host_state}; docker_state={readiness.docker_state}",
        )
        if not readiness.host_ready or not readiness.docker_ready:
            _log_phase("kafka_restart_not_ready", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return False

    _log_phase(
        "kafka_restart_topics_ready",
        run_tag=run_tag,
        scenario=scenario,
        host_bootstrap=host_bootstrap_servers,
        docker_bootstrap=docker_bootstrap_servers,
    )
    time.sleep(KAFKA_SETTLE_DELAY_SEC)
    host_kafka_stable = wait_for_kafka_topic_readiness(
        bootstrap_servers=host_bootstrap_servers,
        topic_names=kafka_topics,
        timeout_sec=KAFKA_POST_READY_SETTLE_TIMEOUT_SEC,
        poll_sec=KAFKA_POST_READY_SETTLE_POLL_SEC,
        execution_mode="host",
    )
    docker_kafka_stable = wait_for_kafka_topic_readiness(
        bootstrap_servers=docker_bootstrap_servers,
        topic_names=kafka_topics,
        timeout_sec=KAFKA_POST_READY_SETTLE_TIMEOUT_SEC,
        poll_sec=KAFKA_POST_READY_SETTLE_POLL_SEC,
        execution_mode="docker",
    )
    if not host_kafka_stable or not docker_kafka_stable:
        settle_readiness = describe_kafka_readiness_state(
            host_bootstrap_servers=host_bootstrap_servers,
            docker_bootstrap_servers=docker_bootstrap_servers,
            kafka_topics=kafka_topics,
            host_ready=host_kafka_stable,
            docker_ready=docker_kafka_stable,
        )
        settle_readiness = maybe_override_kafka_settle_stability(
            run_tag=run_tag,
            scenario=scenario,
            host_bootstrap_servers=host_bootstrap_servers,
            docker_bootstrap_servers=docker_bootstrap_servers,
            readiness=settle_readiness,
        )
        _mark_row_failed(
            row,
            "kafka topics unstable after restart settle window; "
            f"host_bootstrap={host_bootstrap_servers}; docker_bootstrap={docker_bootstrap_servers}; "
            f"topics={','.join(kafka_topics)}; host_stable={settle_readiness.host_ready}; docker_stable={settle_readiness.docker_ready}; "
            f"host_state={settle_readiness.host_state}; docker_state={settle_readiness.docker_state}",
        )
        if not settle_readiness.host_ready or not settle_readiness.docker_ready:
            _log_phase("kafka_restart_unstable_after_settle", run_tag=run_tag, scenario=scenario, notes=row["notes"])
            return False

    time.sleep(KAFKA_POST_RESTART_GRACE_SEC)
    _log_phase("fault_inject_done", run_tag=run_tag, scenario=scenario, action="restart_service:kafka")
    return True
