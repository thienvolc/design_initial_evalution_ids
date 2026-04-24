from __future__ import annotations

import os
import statistics
import sys
import time

_PROCESS_CPU_PRIMED = False
_LAST_PROCESS_CPU_TIME: float | None = None
_LAST_PROCESS_WALL_TIME: float | None = None


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def safe_ratio_or_none(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def prime_process_metrics_probe() -> None:
    global _PROCESS_CPU_PRIMED, _LAST_PROCESS_CPU_TIME, _LAST_PROCESS_WALL_TIME
    try:
        import psutil  # type: ignore

        proc = psutil.Process(os.getpid())
        cpu_times = proc.cpu_times()
        _LAST_PROCESS_CPU_TIME = float(getattr(cpu_times, "user", 0.0) + getattr(cpu_times, "system", 0.0))
        _LAST_PROCESS_WALL_TIME = time.perf_counter()
        _PROCESS_CPU_PRIMED = True
    except Exception:
        # Leave the probe unprimed and let the regular collection path surface diagnostics.
        return


def probe_process_metrics() -> dict:
    global _PROCESS_CPU_PRIMED, _LAST_PROCESS_CPU_TIME, _LAST_PROCESS_WALL_TIME
    cpu_percent = None
    rss_mb = None
    warnings: list[str] = []
    try:
        import psutil  # type: ignore

        proc = psutil.Process(os.getpid())
        now_wall = time.perf_counter()
        cpu_times = proc.cpu_times()
        now_cpu = float(getattr(cpu_times, "user", 0.0) + getattr(cpu_times, "system", 0.0))
        cpu_count = max(int(psutil.cpu_count() or 1), 1)
        if (
            _PROCESS_CPU_PRIMED
            and _LAST_PROCESS_CPU_TIME is not None
            and _LAST_PROCESS_WALL_TIME is not None
        ):
            cpu_delta = max(now_cpu - _LAST_PROCESS_CPU_TIME, 0.0)
            wall_delta = max(now_wall - _LAST_PROCESS_WALL_TIME, 0.0)
            if wall_delta > 0:
                cpu_percent = max(0.0, min((cpu_delta / wall_delta) * 100.0 / cpu_count, 100.0))
            else:
                warnings.append("driver_cpu_percent_wall_delta_zero")
        else:
            warnings.append("driver_cpu_percent_warmup_sample")
        _PROCESS_CPU_PRIMED = True
        _LAST_PROCESS_CPU_TIME = now_cpu
        _LAST_PROCESS_WALL_TIME = now_wall
        rss_mb = float(proc.memory_info().rss) / (1024.0 * 1024.0)
    except Exception as exc:
        warnings.append(f"driver_process_probe_psutil_failed:{type(exc).__name__}")
        try:
            import resource

            rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            if sys.platform == "darwin":
                rss_mb = rss / (1024.0 * 1024.0)
            else:
                rss_mb = rss / 1024.0
            warnings.append("driver_cpu_percent_unavailable_without_psutil")
        except Exception as resource_exc:
            warnings.append(f"driver_process_probe_resource_failed:{type(resource_exc).__name__}")

    return {
        "driver_cpu_percent": cpu_percent,
        "driver_rss_mb": rss_mb,
        "metric_warnings": warnings,
    }


def probe_executor_memory_utilization(spark) -> dict:
    utilization_values: list[float] = []
    warnings: list[str] = []
    try:
        jmap = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
        iterator = jmap.toSeq().iterator()
        while iterator.hasNext():
            entry = iterator.next()
            pair = entry._2()
            max_mem = float(pair._1())
            free_mem = float(pair._2())
            if max_mem > 0:
                utilization_values.append(max(0.0, min(1.0, 1.0 - (free_mem / max_mem))))
    except Exception as exc:
        warnings.append(f"executor_memory_probe_failed:{type(exc).__name__}")

    if not utilization_values:
        try:
            master = str(spark.sparkContext.master or "").strip().lower()
        except Exception:
            master = ""
        if master.startswith("local"):
            try:
                runtime = spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
                max_mem = float(runtime.maxMemory())
                free_mem = float(runtime.freeMemory())
                if max_mem > 0:
                    utilization_values.append(max(0.0, min(1.0, 1.0 - (free_mem / max_mem))))
                    warnings.append("executor_memory_probe_local_driver_fallback")
            except Exception as exc:
                warnings.append(f"executor_memory_probe_local_fallback_failed:{type(exc).__name__}")

    if not utilization_values:
        warnings.append("executor_memory_probe_empty")
        return {
            "executor_mem_util_avg": None,
            "executor_mem_util_p95": None,
            "executor_count": 0,
            "metric_warnings": warnings,
        }

    return {
        "executor_mem_util_avg": float(statistics.fmean(utilization_values)),
        "executor_mem_util_p95": float(
            sorted(utilization_values)[max(0, int(0.95 * len(utilization_values)) - 1)]
        ),
        "executor_count": int(len(utilization_values)),
        "metric_warnings": warnings,
    }


def probe_kafka_lag(bootstrap_servers: str, topic: str, offsets: list[tuple[int, int]]) -> dict:
    try:
        from confluent_kafka import Consumer, TopicPartition
    except Exception as exc:
        return {
            "lag_records_total": None,
            "lag_records_max_partition": None,
            "metric_warnings": [f"kafka_lag_probe_import_failed:{type(exc).__name__}"],
        }

    if not offsets:
        return {
            "lag_records_total": 0,
            "lag_records_max_partition": 0,
            "metric_warnings": [],
        }

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"ids-lag-probe-{int(time.time() * 1000)}",
            "enable.auto.commit": False,
        }
    )
    lags: list[int] = []
    try:
        for partition, max_offset in offsets:
            topic_partition = TopicPartition(topic, int(partition))
            _, high = consumer.get_watermark_offsets(topic_partition, timeout=5.0, cached=False)
            processed_next = int(max_offset) + 1
            lags.append(max(int(high) - processed_next, 0))
    except Exception as exc:
        return {
            "lag_records_total": None,
            "lag_records_max_partition": None,
            "metric_warnings": [f"kafka_lag_probe_failed:{type(exc).__name__}"],
        }
    finally:
        consumer.close()

    return {
        "lag_records_total": int(sum(lags)),
        "lag_records_max_partition": int(max(lags) if lags else 0),
        "metric_warnings": [],
    }
