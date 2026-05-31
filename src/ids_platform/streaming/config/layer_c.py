from __future__ import annotations

from ids_platform.streaming.evaluation.matrices.layer_c.types import LayerCFaultMatrixOptions


LAYER_C_SMOKE_CONFIG = LayerCFaultMatrixOptions(
    config="configs/streaming/streaming.yaml",
    model="random_forest",
    feature_set="full",
    scenarios=("producer_restart",),
    warmup_rows=1_000,
    warmup_rows_per_sec=500.0,
    warmup_rate_schedule="",
    fault_delay_sec=0,
    post_fault_rows=1_000,
    post_fault_rows_per_sec=500.0,
    batch_size=500,
    trace_input_parquet="data/gold/splits/test.parquet",
    trace_order_column="event_time",
    slowdown_rows_per_sec=50.0,
    producer_restart_pause_sec=1,
    replay_retries=2,
    replay_retry_wait_sec=1,
    stream_run_seconds=120,
    startup_wait_sec=10,
    metrics_timeout_sec=120,
    execution_mode="docker",
    python_executable="python",
    bootstrap_servers="kafka:29092",
    summary_csv="artifacts/streaming/evaluation/layer_c_smoke.csv",
)

LAYER_C_MAIN_CONFIG = LayerCFaultMatrixOptions(
    config="configs/streaming/streaming.yaml",
    model="random_forest",
    feature_set="full",
    scenarios=("kafka_restart", "spark_process_restart", "producer_restart", "network_slowdown"),
    warmup_rows=120_000,
    warmup_rows_per_sec=0.0,
    warmup_rate_schedule="5000:24",
    fault_delay_sec=45,
    post_fault_rows=180_000,
    post_fault_rows_per_sec=10_000.0,
    batch_size=5_000,
    trace_input_parquet="data/gold/splits/test.parquet",
    trace_order_column="event_time",
    slowdown_rows_per_sec=50.0,
    producer_restart_pause_sec=5,
    replay_retries=8,
    replay_retry_wait_sec=5,
    stream_run_seconds=1_200,
    startup_wait_sec=90,
    metrics_timeout_sec=1_200,
    execution_mode="docker",
    python_executable="python",
    bootstrap_servers="kafka:29092",
    summary_csv="artifacts/streaming/evaluation/layer_c_summary_700k_fault.csv",
)

LAYER_C_CONFIG = LAYER_C_SMOKE_CONFIG
