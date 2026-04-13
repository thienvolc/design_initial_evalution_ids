from __future__ import annotations

import os
import statistics
import sys
import time

_PROCESS_CPU_PRIMED = False


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def probe_process_metrics() -> dict:
    global _PROCESS_CPU_PRIMED
    cpu_percent = None
    rss_mb = None
    try:
        import psutil  # type: ignore

        proc = psutil.Process(os.getpid())
        if not _PROCESS_CPU_PRIMED:
            proc.cpu_percent(interval=None)
            _PROCESS_CPU_PRIMED = True
        else:
            cpu_percent = float(proc.cpu_percent(interval=None))
        rss_mb = float(proc.memory_info().rss) / (1024.0 * 1024.0)
    except Exception:
        try:
            import resource

            rss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            if sys.platform == "darwin":
                rss_mb = rss / (1024.0 * 1024.0)
            else:
                rss_mb = rss / 1024.0
        except Exception:
            pass

    return {
        "driver_cpu_percent": cpu_percent,
        "driver_rss_mb": rss_mb,
    }


def probe_executor_memory_utilization(spark) -> dict:
    utilization_values: list[float] = []
    try:
        jmap = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
        for entry in list(jmap.entrySet()):
            pair = entry.getValue()
            max_mem = float(pair._1())
            free_mem = float(pair._2())
            if max_mem > 0:
                utilization_values.append(max(0.0, min(1.0, 1.0 - (free_mem / max_mem))))
    except Exception:
        pass

    if not utilization_values:
        return {
            "executor_mem_util_avg": None,
            "executor_mem_util_p95": None,
            "executor_count": 0,
        }

    return {
        "executor_mem_util_avg": float(statistics.fmean(utilization_values)),
        "executor_mem_util_p95": float(
            sorted(utilization_values)[max(0, int(0.95 * len(utilization_values)) - 1)]
        ),
        "executor_count": int(len(utilization_values)),
    }


def probe_kafka_lag(bootstrap_servers: str, topic: str, offsets: list[tuple[int, int]]) -> dict:
    try:
        from confluent_kafka import Consumer, TopicPartition
    except Exception:
        return {
            "lag_records_total": None,
            "lag_records_max_partition": None,
        }

    if not offsets:
        return {
            "lag_records_total": 0,
            "lag_records_max_partition": 0,
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
    except Exception:
        return {
            "lag_records_total": None,
            "lag_records_max_partition": None,
        }
    finally:
        consumer.close()

    return {
        "lag_records_total": int(sum(lags)),
        "lag_records_max_partition": int(max(lags) if lags else 0),
    }
