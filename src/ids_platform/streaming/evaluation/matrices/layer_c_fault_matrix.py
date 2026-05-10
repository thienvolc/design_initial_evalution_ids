"""Compatibility facade for Layer C fault-matrix orchestration.

The implementation now lives under
``ids_platform.streaming.evaluation.matrices.layer_c``.
This module intentionally re-exports the historic symbols that tests and
callers still patch/import directly.
"""

from __future__ import annotations

import time

from ids_platform.common.config import load_yaml_mapping
from ids_platform.common.paths import resolve_project_path
from ids_platform.streaming.core.config import resolve_kafka_bootstrap_servers
from ids_platform.streaming.evaluation.matrices.common import (
    describe_process_startup_state,
    wait_for_log_quiescence,
    wait_for_log_patterns,
    wait_for_process_startup,
)
from ids_platform.streaming.evaluation.matrices.layer_c.logging_utils import (
    _next_run_tag,
)
from ids_platform.streaming.evaluation.matrices.layer_c.runtime import (
    validate_runtime_metric as _validate_runtime_metric,
)
from ids_platform.streaming.evaluation.matrices.layer_c.scenario_flow import (
    execute_scenario as _execute_scenario,
    run_layer_c_fault_matrix,
    scenario_requires_real_network_fault as _scenario_requires_real_network_fault,
)
from ids_platform.streaming.evaluation.orchestration.fault_matrix import (
    cleanup_stream_processes,
    describe_kafka_topics_state,
    fetch_metric,
    replay_with_retries,
    restart_service,
    start_stream_process,
    stop_stream_process,
    wait_for_kafka_bootstrap_ready,
    wait_for_kafka_topics_ready,
    wait_for_process_exit,
    wait_for_stream_shutdown,
)
from ids_platform.streaming.evaluation.matrices.layer_c.types import LayerCFaultMatrixOptions

__all__ = [
    "LayerCFaultMatrixOptions",
    "_execute_scenario",
    "_load_docker_kafka_bootstrap",
    "_load_host_kafka_runtime_targets",
    "_next_run_tag",
    "_scenario_requires_real_network_fault",
    "_validate_runtime_metric",
    "cleanup_stream_processes",
    "describe_process_startup_state",
    "describe_kafka_topics_state",
    "fetch_metric",
    "replay_with_retries",
    "restart_service",
    "run",
    "run_layer_c_fault_matrix",
    "start_stream_process",
    "stop_stream_process",
    "time",
    "wait_for_kafka_bootstrap_ready",
    "wait_for_kafka_topics_ready",
    "wait_for_log_quiescence",
    "wait_for_log_patterns",
    "wait_for_process_exit",
    "wait_for_process_startup",
    "wait_for_stream_shutdown",
]


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


def run(options: LayerCFaultMatrixOptions) -> int:
    return run_layer_c_fault_matrix(options)
