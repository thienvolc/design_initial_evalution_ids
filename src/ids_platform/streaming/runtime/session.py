from __future__ import annotations

import os
import sys

from ids_platform.streaming.runtime.system_metrics import prime_process_metrics_probe


def prepare_spark_environment(spark_config) -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", spark_config.driver_host)


def create_spark_session(config):
    spark_config = config.spark
    prepare_spark_environment(spark_config)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName(spark_config.app_name)
        .master(spark_config.master)
        .config("spark.driver.host", spark_config.driver_host)
        .config("spark.driver.bindAddress", spark_config.driver_bind_address)
        .config("spark.sql.shuffle.partitions", str(spark_config.shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", str(spark_config.arrow_enabled).lower())
        .config("spark.python.worker.reuse", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    kafka_packages = str(spark_config.kafka_packages or "").strip()
    if kafka_packages:
        spark = spark.config("spark.jars.packages", kafka_packages)

    spark = spark.getOrCreate()
    prime_process_metrics_probe()
    return spark
