from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd

EVALUATION_DIR = PROJECT_ROOT / "artifacts" / "streaming" / "evaluation"
PREDICTION_DIR = PROJECT_ROOT / "artifacts" / "streaming" / "predictions"
LATENCY_TIMESERIES_DIR = PROJECT_ROOT / "artifacts" / "streaming" / "latency_timeseries"
PLOTS_DIR = PROJECT_ROOT / "paper" / "figures" / "plots"
TABLES_DIR = EVALUATION_DIR / "paper_tables"
OFFLINE_SPARK_DIR = PROJECT_ROOT / "artifacts" / "offline" / "spark"
PLOT_BLUE = "#4e7ca6"
PLOT_RED = "#be4d44"
PLOT_GREEN = "#99b34e"
PLOT_ORANGE = "#D98A3A"
PLOT_PALETTE = [PLOT_BLUE, PLOT_RED, PLOT_GREEN, PLOT_ORANGE]
PLOT_BAR_WIDTH = 0.18
PLOT_GROUP_GAP_FACTOR = 1.5


def _workspace_path(value) -> Path:
    text = str(value or "")
    if text.startswith("/workspace/"):
        return PROJECT_ROOT / text.removeprefix("/workspace/")
    path = Path(text)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _latest(pattern: str) -> Path | None:
    matches = sorted(EVALUATION_DIR.glob(pattern), key=lambda path: path.stat().st_mtime)
    return matches[-1] if matches else None


def _read_latest(pattern: str) -> tuple[Path | None, pd.DataFrame]:
    path = _latest(pattern)
    if path is None:
        return None, pd.DataFrame()
    return path, pd.read_csv(path)


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _first_numeric(frame: pd.DataFrame, *columns: str) -> pd.Series:
    result = pd.Series(index=frame.index, dtype=float)
    for column in columns:
        if column not in frame.columns:
            continue
        values = pd.to_numeric(frame[column], errors="coerce")
        mask = result.isna() & values.notna()
        if mask.any():
            result.loc[mask] = values.loc[mask]
    return result


def _safe_median(frame: pd.DataFrame, column: str):
    values = _numeric(frame, column).dropna()
    if values.empty:
        return pd.NA
    return values.median()


def _series_median(series: pd.Series):
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return pd.NA
    return values.median()


def _coalesce_numeric(primary: pd.Series, fallback: pd.Series) -> pd.Series:
    result = pd.to_numeric(primary, errors="coerce")
    fallback_values = pd.to_numeric(fallback, errors="coerce")
    mask = result.isna() & fallback_values.notna()
    if mask.any():
        result.loc[mask] = fallback_values.loc[mask]
    return result


def _safe_first(frame: pd.DataFrame, column: str, default="NA"):
    if column not in frame.columns or frame.empty:
        return default
    values = frame[column].dropna()
    if values.empty:
        return default
    return values.iloc[0]


def _mode_label(value: str) -> str:
    value = str(value or "")
    if value == "pass_through":
        return "Đi qua"
    if value == "random_forest_full":
        return "RF-Full"
    return value.replace("_", " ").title()


def _profile_label(value: str) -> str:
    value = str(value or "")
    if "latency_500ms_500offsets" in value:
        return "500 ms / 500"
    if "balanced_1s_1000offsets" in value:
        return "1 s / 1000"
    if "balanced_high_1s_2000offsets" in value:
        return "1 s / 2000"
    if "latency_high_500ms_1000offsets" in value:
        return "500 ms / 1000"
    if "batch_2s_2000offsets" in value:
        return "2 s / 2000"
    return value.replace("runtime_sensitivity_", "").replace("runtime_candidate_", "")


def _scenario_label(value: str) -> str:
    value = str(value or "")
    if value == "cold_start_300rps":
        return "Khởi động 300"
    if value == "spark_process_crash_300rps":
        return "Lỗi Spark 300"
    if value == "spark_process_crash_500rps":
        return "Lỗi Spark 500"
    return value.replace("_", " ")


def _model_label(value: str, *, reduced: bool = False) -> str:
    value = str(value or "")
    if reduced and value == "random_forest":
        return "RF-17"
    if value == "random_forest":
        return "RF-Full"
    if value == "logistic_regression":
        return "LR-Full"
    if value == "gradient_boosting":
        return "GBT-Full"
    return value.replace("_", " ").title()


def _split_label(value: str) -> str:
    value = str(value or "")
    if value == "calibration":
        return "Hiệu chỉnh"
    if value == "test_seen_temporal":
        return "Test theo thời gian"
    if value == "test_unseen_family":
        return "Test họ mới"
    if value == "test_rare_web":
        return "Test Web hiếm"
    return value.replace("_", " ")


def _run_label(row: pd.Series) -> str:
    if "mode" in row and pd.notna(row.get("mode")):
        return _mode_label(str(row.get("mode")))
    if "candidate_label" in row and pd.notna(row.get("candidate_label")):
        return str(row.get("candidate_label"))
    if "overload_profile" in row and pd.notna(row.get("overload_profile")):
        return str(row.get("overload_profile"))
    if "scenario" in row and pd.notna(row.get("scenario")):
        return str(row.get("scenario"))
    return str(row.get("run_tag", "run"))


def _format_number(value, *, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "NA"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if decimals == 0:
        return f"{number:.0f}"
    return f"{number:.{decimals}f}"


def _format_score(value) -> str:
    return _format_number(value, decimals=4)


def _format_mean_std(values: pd.Series, *, decimals: int = 1) -> str:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return "NA"
    mean = numeric.mean()
    std = numeric.std(ddof=1) if len(numeric) > 1 else 0.0
    return f"{mean:.{decimals}f} +/- {std:.{decimals}f}"


def _write_table(frame: pd.DataFrame, stem: str) -> tuple[Path, Path]:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = TABLES_DIR / f"{stem}.csv"
    markdown_path = TABLES_DIR / f"{stem}.md"
    frame = frame.fillna("NA")
    frame.to_csv(csv_path, index=False)
    markdown_path.write_text(_to_markdown(frame), encoding="utf-8")
    return csv_path, markdown_path


def _to_markdown(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "\n"
    headers = [str(column) for column in frame.columns]
    rows = [[str(value) for value in row] for row in frame.to_numpy()]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    lines = [
        "| " + " | ".join(headers[index].ljust(widths[index]) for index in range(len(headers))) + " |",
        "| " + " | ".join("-" * widths[index] for index in range(len(headers))) + " |",
    ]
    for row in rows:
        lines.append(
            "| " + " | ".join(row[index].ljust(widths[index]) for index in range(len(headers))) + " |"
        )
    return "\n".join(lines) + "\n"


def export_latency_timeseries(summary_frames: list[pd.DataFrame]) -> pd.DataFrame:
    LATENCY_TIMESERIES_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    for summary in summary_frames:
        if summary.empty or "run_tag" not in summary.columns:
            continue
        for _, summary_row in summary.dropna(subset=["run_tag"]).iterrows():
            run_tag = str(summary_row["run_tag"])
            artifact_path = PREDICTION_DIR / run_tag
            if not artifact_path.exists():
                continue
            try:
                timeseries = _latency_timeseries_for_artifact(artifact_path, run_tag)
            except Exception as exc:
                rows.append(
                    {
                        "run_tag": run_tag,
                        "latency_timeseries_path": "",
                        "rows": 0,
                        "status": f"error:{type(exc).__name__}",
                    }
                )
                continue
            output_path = LATENCY_TIMESERIES_DIR / f"{run_tag}.csv"
            timeseries.to_csv(output_path, index=False)
            rows.append(
                {
                    "run_tag": run_tag,
                    "latency_timeseries_path": str(output_path),
                    "rows": int(timeseries["rows"].sum()) if not timeseries.empty else 0,
                    "status": "ok",
                }
            )
    manifest = pd.DataFrame(rows)
    manifest_path = EVALUATION_DIR / "streaming_latency_timeseries_manifest.csv"
    manifest.to_csv(manifest_path, index=False)
    return manifest


def _latency_timeseries_for_artifact(artifact_path: Path, run_tag: str) -> pd.DataFrame:
    columns = [
        "benchmark_phase",
        "emit_time",
        "source_to_ingest_ms",
        "processing_ms",
        "end_to_end_ms",
    ]
    frame = pd.read_parquet(artifact_path, columns=columns)
    if frame.empty:
        return pd.DataFrame()
    frame["emit_time"] = pd.to_datetime(frame["emit_time"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["emit_time"])
    if frame.empty:
        return pd.DataFrame()
    frame["benchmark_phase"] = frame["benchmark_phase"].fillna("measure").astype(str)
    run_start = frame["emit_time"].min()
    frame["second"] = ((frame["emit_time"] - run_start).dt.total_seconds()).astype(int)

    grouped_rows: list[dict] = []
    for (phase, second), group in frame.groupby(["benchmark_phase", "second"], sort=True):
        grouped_rows.append(
            {
                "run_tag": run_tag,
                "benchmark_phase": phase,
                "second": int(second),
                "rows": int(len(group)),
                "throughput_rps": float(len(group)),
                "source_p95_ms": _quantile(group, "source_to_ingest_ms", 0.95),
                "processing_p50_ms": _quantile(group, "processing_ms", 0.50),
                "processing_p95_ms": _quantile(group, "processing_ms", 0.95),
                "e2e_p50_ms": _quantile(group, "end_to_end_ms", 0.50),
                "e2e_p95_ms": _quantile(group, "end_to_end_ms", 0.95),
                "e2e_p99_ms": _quantile(group, "end_to_end_ms", 0.99),
            }
        )
    return pd.DataFrame(grouped_rows)


def _quantile(frame: pd.DataFrame, column: str, quantile: float) -> float | None:
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.quantile(float(quantile)))


def _artifact_latency_distribution(
    frame: pd.DataFrame,
    *,
    phase: str | None = "measure",
    phase_by_fault_kind: bool = False,
) -> dict[str, object]:
    values: list[pd.Series] = []
    for _, row in frame.iterrows():
        run_tag = str(row.get("run_tag") or "")
        artifact_path = PREDICTION_DIR / run_tag
        if not run_tag or not artifact_path.exists():
            continue
        expected_phase = phase
        if phase_by_fault_kind:
            fault_kind = str(row.get("fault_kind") or "")
            expected_phase = "cold_start" if fault_kind == "none" else "post_fault"
        columns = ["end_to_end_ms"]
        if expected_phase is not None:
            columns.append("benchmark_phase")
        try:
            artifact = pd.read_parquet(artifact_path, columns=columns)
        except Exception:
            continue
        if artifact.empty:
            continue
        if expected_phase is not None and "benchmark_phase" in artifact.columns:
            phases = artifact["benchmark_phase"].fillna("measure").astype(str).str.lower()
            artifact = artifact[phases == str(expected_phase).lower()]
        latency = pd.to_numeric(artifact.get("end_to_end_ms"), errors="coerce").dropna()
        if not latency.empty:
            values.append(latency)

    if not values:
        return {
            "latency_rows": pd.NA,
            "latency_avg_ms": pd.NA,
            "latency_min_ms": pd.NA,
            "latency_max_ms": pd.NA,
            "latency_q90_ms": pd.NA,
            "latency_q95_ms": pd.NA,
            "latency_q99_ms": pd.NA,
        }

    combined = pd.concat(values, ignore_index=True)
    return {
        "latency_rows": int(len(combined)),
        "latency_avg_ms": float(combined.mean()),
        "latency_min_ms": float(combined.min()),
        "latency_max_ms": float(combined.max()),
        "latency_q90_ms": float(combined.quantile(0.90)),
        "latency_q95_ms": float(combined.quantile(0.95)),
        "latency_q99_ms": float(combined.quantile(0.99)),
    }


def _format_latency_distribution(stats: dict[str, object]) -> dict[str, str]:
    return {
        "latency_rows": _format_number(stats.get("latency_rows"), decimals=0),
        "e2e_latency_avg_ms": _format_number(stats.get("latency_avg_ms")),
        "e2e_latency_min_ms": _format_number(stats.get("latency_min_ms")),
        "e2e_latency_max_ms": _format_number(stats.get("latency_max_ms")),
        "e2e_latency_q90_ms": _format_number(stats.get("latency_q90_ms")),
        "e2e_latency_q95_ms": _format_number(stats.get("latency_q95_ms")),
        "e2e_latency_q99_ms": _format_number(stats.get("latency_q99_ms")),
    }


def build_tables(
    *,
    runtime_screening: pd.DataFrame,
    runtime_confirmation: pd.DataFrame,
    capacity: pd.DataFrame,
    model_feature: pd.DataFrame,
    fault: pd.DataFrame,
    overload: pd.DataFrame,
) -> dict[str, tuple[Path, Path]]:
    tables = {
        "table_offline_calibration_summary": _offline_calibration_table(),
        "table_offline_test_summary": _offline_test_summary_table(),
        "table_offline_unseen_confusion": _offline_unseen_confusion_table(),
        "table_selected_runtime_configuration": _selected_runtime_configuration_table(
            capacity=capacity,
            runtime_confirmation=runtime_confirmation,
        ),
        "table_capacity_summary": _capacity_summary_table(capacity),
        "table_model_feature_summary": _model_feature_summary_table(model_feature),
        "table_fault_recovery_summary": _fault_recovery_summary_table(fault),
        "table_overload_summary": _overload_summary_table(overload),
    }
    return {name: _write_table(frame, name) for name, frame in tables.items()}


def _offline_calibration_table() -> pd.DataFrame:
    full_path = OFFLINE_SPARK_DIR / "full" / "models" / "valid_metrics.csv"
    reduced_path = OFFLINE_SPARK_DIR / "reduced" / "models" / "valid_metrics.csv"
    frames: list[pd.DataFrame] = []
    if full_path.exists():
        full = pd.read_csv(full_path)
        full["candidate"] = full["model"].map(lambda value: _model_label(value))
        frames.append(full)
    if reduced_path.exists():
        reduced = pd.read_csv(reduced_path)
        reduced["candidate"] = reduced["model"].map(lambda value: _model_label(value, reduced=True))
        frames.append(reduced)
    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)
    for column in ["precision", "recall", "f1", "fpr", "threshold"]:
        data[column] = _numeric(data, column)
    rows = []
    order = {"LR-Full": 0, "RF-Full": 1, "GBT-Full": 2, "RF-17": 3}
    for _, row in data.sort_values("candidate", key=lambda values: values.map(order)).iterrows():
        rows.append(
            {
                "candidate": row["candidate"],
                "threshold": _format_score(row.get("threshold")),
                "precision": _format_score(row.get("precision")),
                "recall": _format_score(row.get("recall")),
                "f1": _format_score(row.get("f1")),
                "fpr": _format_score(row.get("fpr")),
            }
        )
    return pd.DataFrame(rows)


def _offline_test_summary_table() -> pd.DataFrame:
    data = _offline_test_summary()
    if data.empty:
        return pd.DataFrame()
    rows = []
    order = {"LR-Full": 0, "RF-Full": 1, "GBT-Full": 2, "RF-17": 3}
    split_order = {"test_seen_temporal": 0, "test_unseen_family": 1, "test_rare_web": 2}
    data = data.sort_values(
        ["split", "candidate"],
        key=lambda values: values.map(split_order if values.name == "split" else order),
    )
    for _, row in data.iterrows():
        rows.append(
            {
                "split": _split_label(row.get("split")),
                "candidate": row["candidate"],
                "precision": _format_score(row.get("precision")),
                "recall": _format_score(row.get("recall")),
                "f1": _format_score(row.get("f1")),
            }
        )
    return pd.DataFrame(rows)


def _offline_unseen_confusion_table() -> pd.DataFrame:
    specs = [
        ("full", "logistic_regression", "LR-Full"),
        ("full", "random_forest", "RF-Full"),
        ("full", "gradient_boosting", "GBT-Full"),
        ("reduced", "random_forest", "RF-17"),
    ]
    rows = []
    for feature_set, model, label in specs:
        path = (
            OFFLINE_SPARK_DIR
            / feature_set
            / "evaluation"
            / f"confusion_matrix_{model}_test_unseen_family.csv"
        )
        if not path.exists():
            continue
        matrix = pd.read_csv(path, index_col=0)
        values = matrix.to_numpy(dtype=float)
        if values.shape != (2, 2):
            continue
        tn, fp = values[0, 0], values[0, 1]
        fn, tp = values[1, 0], values[1, 1]
        attack_total = tp + fn
        benign_total = tn + fp
        rows.append(
            {
                "candidate": label,
                "tn": _format_number(tn, decimals=0),
                "fp": _format_number(fp, decimals=0),
                "fn": _format_number(fn, decimals=0),
                "tp": _format_number(tp, decimals=0),
                "attack_recall": _format_score(tp / attack_total if attack_total else pd.NA),
                "benign_specificity": _format_score(tn / benign_total if benign_total else pd.NA),
            }
        )
    return pd.DataFrame(rows)


def _selected_runtime_configuration_table(
    *,
    capacity: pd.DataFrame,
    runtime_confirmation: pd.DataFrame,
) -> pd.DataFrame:
    source = capacity
    selected = source[
        (source.get("mode", pd.Series(dtype=str)).astype(str) == "random_forest_full")
        & (_numeric(source, "target_rps") == 500)
    ]
    if selected.empty:
        selected = runtime_confirmation

    return pd.DataFrame(
        [
            {
                  "spark_master": _safe_first(selected, "spark_master", "local[4]"),
                  "offline_environment": "Windows 11, 16 GB RAM, Intel Core i5-1240P, 12 lõi",
                  "streaming_environment": "Docker, 8 GB RAM, 2 GB swap, 8 lõi CPU",
                  "spark_cores": "4 lõi local",
                  "trigger_interval": _safe_first(selected, "trigger_interval", "1 second"),
                "max_offsets_per_trigger": _format_number(
                    _safe_first(selected, "max_offsets_per_trigger", 1000),
                    decimals=0,
                ),
                "shuffle_partitions": _format_number(
                    _safe_first(selected, "shuffle_partitions", 4),
                    decimals=0,
                ),
                "kafka_partitions": _format_number(
                    _safe_first(selected, "topic_partitions", 1),
                    decimals=0,
                ),
                "main_model": "RF-Full",
                "operating_rps": "500",
                "pressure_rps": "600",
                "overload_rps": "750+",
            }
        ]
    )


def _capacity_summary_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    for column in [
        "target_rps",
        "p95_e2e_ms",
        "p99_e2e_ms",
        "drain_rps",
        "throughput_ratio",
        "kafka_lag_peak_records",
    ]:
        data[column] = _numeric(data, column)

    rows: list[dict] = []
    for (mode, target_rps), group in data.groupby(["mode", "target_rps"], dropna=True):
        latency_stats = _format_latency_distribution(_artifact_latency_distribution(group))
        rows.append(
                {
                    "mode": _mode_label(str(mode)),
                    "target_rps": _format_number(target_rps, decimals=0),
                    **latency_stats,
                    "p95_latency_ms_mean_std": _format_mean_std(group["p95_e2e_ms"]),
                "p99_latency_ms_mean_std": _format_mean_std(group["p99_e2e_ms"]),
                "drain_rps_mean_std": _format_mean_std(group["drain_rps"]),
                "throughput_ratio_mean_std": _format_mean_std(group["throughput_ratio"], decimals=3),
                "kafka_lag_peak_mean_std": _format_mean_std(group["kafka_lag_peak_records"], decimals=0),
            }
        )
    return pd.DataFrame(rows).sort_values(["mode", "target_rps"])


def _model_feature_summary_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    for column in [
        "target_rps",
        "e2e_p95_ms",
        "e2e_p99_ms",
        "drain_rps",
        "kafka_lag_peak_records",
        "precision",
        "recall",
        "f1",
    ]:
        data[column] = _numeric(data, column)

    rows: list[dict] = []
    group_columns = ["candidate_label", "feature_set", "target_rps"]
    for keys, group in data.groupby(group_columns, dropna=True):
        candidate, feature_set, target_rps = keys
        latency_stats = _format_latency_distribution(_artifact_latency_distribution(group))
        rows.append(
                {
                    "candidate": str(candidate),
                    "feature_set": str(feature_set),
                    "target_rps": _format_number(target_rps, decimals=0),
                    **latency_stats,
                    "p95_latency_ms_mean_std": _format_mean_std(group["e2e_p95_ms"]),
                "p99_latency_ms_mean_std": _format_mean_std(group["e2e_p99_ms"]),
                "drain_rps_mean_std": _format_mean_std(group["drain_rps"]),
                "kafka_lag_peak_mean_std": _format_mean_std(group["kafka_lag_peak_records"], decimals=0),
                "precision_mean_std": _format_mean_std(group["precision"], decimals=4),
                "recall_mean_std": _format_mean_std(group["recall"], decimals=4),
                "f1_mean_std": _format_mean_std(group["f1"], decimals=4),
            }
        )
    return pd.DataFrame(rows).sort_values(["candidate"])


def _fault_recovery_summary_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    phase_summary = _fault_phase_latency_summary(data)
    if not phase_summary.empty:
        data = data.merge(phase_summary, on="run_tag", how="left")
    for column in [
        "target_rps",
        "startup_seconds",
        "recovery_seconds",
        "initial_expected_rows",
        "initial_rows",
        "post_fault_expected_rows",
        "post_fault_rows",
        "drain_rps_after_fault",
        "e2e_p95_ms_after_fault",
        "kafka_lag_peak_records",
        "kafka_lag_clear_sec",
        "phase_e2e_p95_ms",
        "phase_output_rps",
    ]:
        data[column] = _numeric(data, column)
    data["phase_p95_latency_ms"] = _coalesce_numeric(
        data["phase_e2e_p95_ms"],
        data["e2e_p95_ms_after_fault"],
    )
    data["phase_drain_rps"] = _coalesce_numeric(
        data["phase_output_rps"],
        data["drain_rps_after_fault"],
    )

    rows: list[dict] = []
    for (scenario, target_rps), group in data.groupby(["scenario", "target_rps"], dropna=True):
        expected_rows = _coalesce_numeric(group["post_fault_expected_rows"], group["initial_expected_rows"])
        observed_rows = _coalesce_numeric(group["post_fault_rows"], group["initial_rows"])
        checkpoint_values = group.get("checkpoint_reused", pd.Series(index=group.index, dtype=object))
        latency_stats = _format_latency_distribution(
            _artifact_latency_distribution(group, phase_by_fault_kind=True)
        )
        rows.append(
                {
                    "scenario": str(scenario),
                    "target_rps": _format_number(target_rps, decimals=0),
                    **latency_stats,
                "rows_expected_mean_std": _format_mean_std(expected_rows, decimals=0),
                "rows_observed_mean_std": _format_mean_std(observed_rows, decimals=0),
                "startup_or_recovery_sec_mean_std": _format_mean_std(
                    _coalesce_numeric(group["startup_seconds"], group["recovery_seconds"])
                ),
                "phase_p95_latency_ms_mean_std": _format_mean_std(group["phase_p95_latency_ms"]),
                "phase_drain_rps_mean_std": _format_mean_std(group["phase_drain_rps"]),
                "kafka_lag_peak_mean_std": _format_mean_std(group["kafka_lag_peak_records"], decimals=0),
                "kafka_lag_clear_sec_mean_std": _format_mean_std(group["kafka_lag_clear_sec"]),
                    "checkpoint_reused": (
                        f"{int((checkpoint_values.astype(str).str.lower() == 'true').sum())}/{int(len(group))}"
                    ),
                }
        )
    return pd.DataFrame(rows).sort_values(["target_rps", "scenario"])


def _overload_summary_table(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    data = frame.copy()
    for column in [
        "target_rps",
        "p95_e2e_ms",
        "p99_e2e_ms",
        "drain_rps",
          "target_to_drain_ratio",
          "kafka_lag_peak_records",
          "kafka_lag_clear_sec",
      ]:
          data[column] = _numeric(data, column)

    rows: list[dict] = []
    for target_rps, group in data.groupby("target_rps", dropna=True):
        latency_stats = _format_latency_distribution(_artifact_latency_distribution(group))
        rows.append(
                {
                    "target_rps": _format_number(target_rps, decimals=0),
                    "overloaded": (
                        f"{int((group.get('degradation_status', '') == 'overloaded').sum())}/{int(len(group))}"
                  ),
                  **latency_stats,
                  "p95_latency_ms_mean_std": _format_mean_std(group["p95_e2e_ms"]),
                  "p99_latency_ms_mean_std": _format_mean_std(group["p99_e2e_ms"]),
                  "drain_rps_mean_std": _format_mean_std(group["drain_rps"]),
                  "target_to_drain_ratio_mean_std": _format_mean_std(group["target_to_drain_ratio"], decimals=3),
                  "kafka_lag_peak_mean_std": _format_mean_std(group["kafka_lag_peak_records"], decimals=0),
                  "kafka_lag_peak_max": _format_number(group["kafka_lag_peak_records"].max(), decimals=0),
                  "kafka_lag_clear_sec_mean_std": _format_mean_std(group["kafka_lag_clear_sec"]),
              }
          )
    return pd.DataFrame(rows).sort_values("target_rps")


def build_plots(
    *,
    runtime_screening: pd.DataFrame,
    capacity: pd.DataFrame,
    model_feature: pd.DataFrame,
    fault: pd.DataFrame,
    overload: pd.DataFrame,
) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    import matplotlib.pyplot as plt

    _apply_plot_style(plt)
    if not runtime_screening.empty:
        _plot_runtime_sensitivity(runtime_screening, plt)
        _plot_runtime_comparative_processing(runtime_screening, plt)
    if not capacity.empty:
        _plot_capacity(capacity, plt)
    if not model_feature.empty:
        _plot_model_feature(model_feature, plt)
    if not fault.empty:
        _plot_fault(fault, plt)
    if not overload.empty:
        _plot_overload(overload, plt)


def _apply_plot_style(plt) -> None:
      plt.rcParams.update(
          {
              "font.family": "sans-serif",
              "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
              "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.dpi": 120,
            "savefig.dpi": 220,
        }
    )


def _offline_test_summary() -> pd.DataFrame:
    full_path = OFFLINE_SPARK_DIR / "full" / "evaluation" / "test_summary.csv"
    reduced_path = OFFLINE_SPARK_DIR / "reduced" / "evaluation" / "test_summary.csv"
    frames: list[pd.DataFrame] = []
    if full_path.exists():
        full = pd.read_csv(full_path)
        full["candidate"] = full["model"].map(lambda value: _model_label(value))
        frames.append(full)
    if reduced_path.exists():
        reduced = pd.read_csv(reduced_path)
        reduced["candidate"] = reduced["model"].map(lambda value: _model_label(value, reduced=True))
        frames.append(reduced)
    if not frames:
        return pd.DataFrame()
    data = pd.concat(frames, ignore_index=True)
    for column in ["precision", "recall", "f1"]:
        data[column] = _numeric(data, column)
    return data


def _metric_summary(
    frame: pd.DataFrame,
    *,
    group_columns: list[str],
    metrics: list[str],
) -> pd.DataFrame:
    data = frame.copy()
    for metric in metrics:
        data[metric] = _numeric(data, metric)
    rows: list[dict] = []
    for keys, group in data.groupby(group_columns, dropna=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_columns, keys))
        for metric in metrics:
            values = group[metric].dropna()
            row[f"{metric}_median"] = values.median() if not values.empty else pd.NA
            row[f"{metric}_min"] = values.min() if not values.empty else pd.NA
            row[f"{metric}_max"] = values.max() if not values.empty else pd.NA
        row["repeat_count"] = int(len(group))
        rows.append(row)
    return pd.DataFrame(rows)


def _plot_runtime_sensitivity(frame: pd.DataFrame, plt) -> None:
    data = frame.copy()
    data = data[
        data.get("profile", pd.Series(dtype=str)).astype(str).str.contains(
            "latency_500ms_500offsets|balanced_1s_1000offsets",
            regex=True,
            na=False,
        )
    ]
    data = data[_numeric(data, "target_rps") == 500]
    if data.empty:
        return

    metrics = ["p95_e2e_ms", "p99_e2e_ms", "drain_rps", "kafka_lag_peak_records"]
    summary = _metric_summary(
        data,
        group_columns=["profile", "mode"],
        metrics=metrics,
    )
    summary["profile_label"] = summary["profile"].map(_profile_label)
    summary["mode_label"] = summary["mode"].map(_mode_label)
    _plot_grouped_metric_panels(
        summary,
        category_column="profile_label",
        series_column="mode_label",
        metric_specs=[
            ("p95_e2e_ms", "Độ trễ p95", "ms"),
            ("p99_e2e_ms", "Độ trễ p99", "ms"),
            ("drain_rps", "Thông lượng tiêu thoát", "bản ghi/s"),
            ("kafka_lag_peak_records", "Tồn đọng Kafka cực đại", "bản ghi"),
        ],
        title_prefix="Độ nhạy cấu hình thời gian chạy",
        output_name="fig_runtime_sensitivity.png",
        plt=plt,
    )


def _plot_runtime_comparative_processing(runtime_screening: pd.DataFrame, plt) -> None:
    palette = {
        "500 ms / 500": PLOT_BLUE,
        "500 ms / 1000": PLOT_RED,
        "1 s / 1000": PLOT_GREEN,
        "1 s / 2000": PLOT_ORANGE,
    }

    data = runtime_screening.copy()
    if not data.empty:
        data["target_rps"] = _numeric(data, "target_rps")
        data["artifact_processing_p95_ms"] = _numeric(data, "artifact_processing_p95_ms")
        data["profile_label"] = data["profile"].map(_profile_label)
        data = data[
            data["profile_label"].isin(["500 ms / 500", "500 ms / 1000", "1 s / 1000", "1 s / 2000"])
        ]
        data = data.dropna(subset=["target_rps", "artifact_processing_p95_ms"])

    rps_values = sorted(data["target_rps"].dropna().unique()) if not data.empty else []
    series_values = [label for label in palette if label in set(data["profile_label"].astype(str))]
    bar_width = PLOT_BAR_WIDTH
    bar_step = bar_width
    group_width = bar_width * max(len(series_values), 1)
    category_step = group_width + bar_width * PLOT_GROUP_GAP_FACTOR
    panel_width = max(3.9, 2.6 + category_step * max(len(rps_values), 1))
    fig, axes = plt.subplots(1, 2, figsize=(panel_width * 2, 3.9), sharey=True)
    y_top = None
    if not data.empty:
        max_value = data["artifact_processing_p95_ms"].dropna().max()
        y_top = float(max_value) * 1.18 if not pd.isna(max_value) else None

    for axis, mode, title in [
        (axes[0], "pass_through", "Đi qua"),
        (axes[1], "random_forest_full", "RF-Full"),
    ]:
        subset = data[data.get("mode", pd.Series(dtype=str)).astype(str) == mode] if not data.empty else data
        if subset.empty:
            axis.text(0.5, 0.5, "Không có dữ liệu", ha="center", va="center", transform=axis.transAxes)
            _style_axes(axis, title, "RPS mục tiêu", "P95 thời gian xử lý (ms)")
            continue
        x_positions = [index * category_step for index in range(len(rps_values))]
        for series_index, label in enumerate(series_values):
            group = subset[subset["profile_label"].astype(str) == label]
            offset = (series_index - (len(series_values) - 1) / 2) * bar_step
            values: list[float] = []
            for rps in rps_values:
                value = _safe_median(group[group["target_rps"] == rps], "artifact_processing_p95_ms")
                values.append(float(value) if not pd.isna(value) else float("nan"))
            axis.bar(
                [position + offset for position in x_positions],
                  values,
                  width=bar_width,
                  label=label,
                  color=palette.get(label, PLOT_BLUE),
                  edgecolor="none",
                  linewidth=0,
              )
        axis.set_xticks(x_positions)
        axis.set_xticklabels([f"{int(value)}" for value in rps_values])
        _style_axes(axis, title, "RPS mục tiêu", "P95 thời gian xử lý (ms)")
        axis.set_ylim(bottom=0)
        if y_top:
            axis.set_ylim(0, y_top)

    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Thời gian xử lý theo RPS và trigger/offset", y=1.03)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "fig_runtime_comparative_processing.png", bbox_inches="tight")
    plt.close(fig)


def _plot_capacity(frame: pd.DataFrame, plt) -> None:
    data = frame.copy()
    metrics = ["p95_e2e_ms", "p99_e2e_ms", "drain_rps", "kafka_lag_peak_records"]
    summary = _metric_summary(
        data,
        group_columns=["mode", "target_rps"],
        metrics=metrics,
    )
    summary["mode_label"] = summary["mode"].map(_mode_label)
    summary["target_rps"] = pd.to_numeric(summary["target_rps"], errors="coerce")

    fig, axes = plt.subplots(1, 3, figsize=(12.4, 3.8))
    colors = {"Đi qua": PLOT_BLUE, "RF-Full": PLOT_RED}
    for mode_label, group in summary.groupby("mode_label"):
        group = group.sort_values("target_rps")
        color = colors.get(mode_label, None)
        axes[0].plot(
            group["target_rps"],
            group["p95_e2e_ms_median"],
            marker="o",
            color=color,
            label=f"{mode_label} P95",
        )
        axes[0].plot(
            group["target_rps"],
            group["p99_e2e_ms_median"],
            marker="s",
            linestyle="--",
            color=color,
            label=f"{mode_label} P99",
        )
        axes[1].plot(group["target_rps"], group["drain_rps_median"], marker="o", color=color, label=mode_label)
        axes[2].plot(
            group["target_rps"],
            group["kafka_lag_peak_records_median"],
            marker="o",
            color=color,
            label=mode_label,
        )
    for axis in axes:
        axis.axvline(500, color=PLOT_GREEN, linestyle=":", linewidth=1.0, alpha=0.7)
        axis.axvline(600, color=PLOT_ORANGE, linestyle=":", linewidth=1.0, alpha=0.55)
    _style_axes(axes[0], "Độ trễ đuôi", "RPS mục tiêu", "Độ trễ (ms)")
    _style_axes(axes[1], "Thông lượng tiêu thoát", "RPS mục tiêu", "Bản ghi/s")
    _style_axes(axes[2], "Tồn đọng Kafka", "RPS mục tiêu", "Lag cực đại (bản ghi)")
    axes[0].legend(frameon=False)
    fig.suptitle("Hiệu chỉnh năng lực xử lý dưới phát lại ổn định", y=1.03)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "fig_capacity_calibration.png", bbox_inches="tight")
    plt.close(fig)


def _plot_model_feature(frame: pd.DataFrame, plt) -> None:
    data = frame.copy()
    metrics = ["e2e_p95_ms", "drain_rps", "f1", "recall"]
    summary = _metric_summary(
        data,
        group_columns=["candidate_label"],
        metrics=metrics,
    ).sort_values("candidate_label")
    _plot_grouped_metric_panels(
        summary,
        category_column="candidate_label",
        series_column=None,
        metric_specs=[
            ("e2e_p95_ms", "Độ trễ p95", "ms"),
            ("drain_rps", "Thông lượng tiêu thoát", "bản ghi/s"),
            ("f1", "F1", "điểm"),
            ("recall", "Recall", "điểm"),
        ],
        title_prefix="Đánh đổi giữa mô hình và đặc trưng",
        output_name="fig_model_feature_tradeoff.png",
        plt=plt,
    )


def _plot_fault(frame: pd.DataFrame, plt) -> None:
    data = frame.copy()
    phase_summary = _fault_phase_latency_summary(data)
    if not phase_summary.empty:
        data = data.merge(phase_summary, on="run_tag", how="left")
    data["startup_or_recovery_seconds"] = _coalesce_numeric(
        _numeric(data, "startup_seconds"),
        _numeric(data, "recovery_seconds"),
    )
    data["phase_p95_latency_ms"] = _coalesce_numeric(
        _numeric(data, "phase_e2e_p95_ms"),
        _numeric(data, "e2e_p95_ms_after_fault"),
    )
    data["phase_output_rps"] = _coalesce_numeric(
        _numeric(data, "phase_output_rps"),
        _numeric(data, "drain_rps_after_fault"),
    )
    metrics = [
        "startup_or_recovery_seconds",
        "phase_p95_latency_ms",
        "phase_output_rps",
        "kafka_lag_peak_records",
    ]
    summary = _metric_summary(
        data,
        group_columns=["scenario"],
        metrics=metrics,
    )
    summary["scenario_label"] = summary["scenario"].map(_scenario_label)
    _plot_grouped_metric_panels(
        summary,
        category_column="scenario_label",
        series_column=None,
        metric_specs=[
            ("startup_or_recovery_seconds", "Khởi động/phục hồi", "s"),
            ("phase_p95_latency_ms", "P95 pha đo", "ms"),
            ("phase_output_rps", "RPS pha đo", "bản ghi/s"),
            ("kafka_lag_peak_records", "Kafka lag cực đại", "bản ghi"),
        ],
        title_prefix="Phục hồi sau gián đoạn",
        output_name="fig_fault_recovery.png",
        plt=plt,
        single_series_legend=True,
    )


def _fault_phase_latency_summary(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for _, row in frame.iterrows():
        run_tag = str(row.get("run_tag") or "")
        artifact_path = PREDICTION_DIR / run_tag
        if not run_tag or not artifact_path.exists():
            continue
        fault_kind = str(row.get("fault_kind") or "")
        expected_phase = "cold_start" if fault_kind == "none" else "post_fault"
        try:
            artifact = pd.read_parquet(
                artifact_path,
                columns=["benchmark_phase", "end_to_end_ms", "emit_time"],
            )
        except Exception:
            continue
        if artifact.empty:
            continue
        phases = artifact["benchmark_phase"].fillna("measure").astype(str).str.lower()
        values = pd.to_numeric(
            artifact.loc[phases == expected_phase, "end_to_end_ms"],
            errors="coerce",
        ).dropna()
        if values.empty:
            continue
        emit_times = pd.to_datetime(
            artifact.loc[phases == expected_phase, "emit_time"],
            errors="coerce",
            utc=True,
        ).dropna()
        emit_span = (
            (emit_times.max() - emit_times.min()).total_seconds()
            if not emit_times.empty
            else 0.0
        )
        output_rps = len(values) / emit_span if emit_span > 0 else pd.NA
        rows.append(
            {
                "run_tag": run_tag,
                "phase_e2e_p95_ms": values.quantile(0.95),
                "phase_output_rps": output_rps,
            }
        )
    return pd.DataFrame(rows)


def _plot_overload(frame: pd.DataFrame, plt) -> None:
    data = frame.copy()
    data["target_rps"] = _numeric(data, "target_rps")
    target = 750 if (data["target_rps"] == 750).any() else data["target_rps"].dropna().max()
    selected = data[data["target_rps"] == target]
    if selected.empty:
        return

    latency_series = _overload_latency_timeseries(selected, "e2e_p95_ms")
    throughput_series = _overload_latency_timeseries(selected, "throughput_rps")
    lag_series = _overload_kafka_lag_timeseries(selected)
    fig, axes = plt.subplots(1, 3, figsize=(12.4, 3.8))
    _plot_timeseries_with_mean(axes[0], latency_series, "Độ trễ p95", "Độ trễ (ms)")
    _plot_timeseries_with_mean(axes[1], throughput_series, "Thông lượng hoàn tất", "Bản ghi/s")
    if not throughput_series.empty and pd.notna(target):
        axes[1].axhline(float(target), color=PLOT_ORANGE, linestyle=":", linewidth=1.0, label="RPS mục tiêu")
        axes[1].legend(frameon=False, loc="upper right")
    _plot_timeseries_with_mean(axes[2], lag_series, "Tồn đọng Kafka", "Lag (bản ghi)")
    for axis in axes:
        axis.set_xlabel("Giây từ bắt đầu pha đo")
    fig.suptitle(f"Suy giảm dưới quá tải tại {int(target)} RPS", y=1.03)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "fig_overload_degradation.png", bbox_inches="tight")
    plt.close(fig)


def _overload_latency_timeseries(frame: pd.DataFrame, metric: str) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for _, row in frame.iterrows():
        run_tag = str(row.get("run_tag") or "")
        path = LATENCY_TIMESERIES_DIR / f"{run_tag}.csv"
        if not path.exists():
            continue
        data = pd.read_csv(path)
        if metric not in data.columns or "second" not in data.columns:
            continue
        phase = data.get("benchmark_phase", pd.Series(index=data.index, dtype=str)).astype(str).str.lower()
        data = data[phase.str.contains("measure", na=False)].copy()
        if data.empty:
            continue
        data["second"] = pd.to_numeric(data["second"], errors="coerce")
        data["value"] = pd.to_numeric(data[metric], errors="coerce")
        data = data.dropna(subset=["second", "value"])
        if data.empty:
            continue
        data["second"] = data["second"] - data["second"].min()
        data["run_tag"] = run_tag
        rows.append(data[["run_tag", "second", "value"]])
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _overload_kafka_lag_timeseries(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for _, row in frame.iterrows():
        run_tag = str(row.get("run_tag") or "")
        path = _workspace_path(row.get("kafka_lag_timeseries_path"))
        if not path.exists():
            continue
        data = pd.read_csv(path)
        if "lag_records" not in data.columns or "ts_epoch_ms" not in data.columns:
            continue
        phase = data.get("phase", pd.Series(index=data.index, dtype=str)).astype(str).str.lower()
        data = data[phase.str.startswith("measure", na=False)].copy()
        if data.empty:
            continue
        data["ts_epoch_ms"] = pd.to_numeric(data["ts_epoch_ms"], errors="coerce")
        data["value"] = pd.to_numeric(data["lag_records"], errors="coerce")
        data = data.dropna(subset=["ts_epoch_ms", "value"])
        if data.empty:
            continue
        data["second"] = ((data["ts_epoch_ms"] - data["ts_epoch_ms"].min()) / 1000).astype(int)
        data["run_tag"] = run_tag
        rows.append(data[["run_tag", "second", "value"]])
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _plot_timeseries_with_mean(axis, frame: pd.DataFrame, title: str, ylabel: str) -> None:
    if frame.empty:
        _style_axes(axis, title, "Giây từ bắt đầu pha đo", ylabel)
        return
    first = True
    for _, group in frame.groupby("run_tag"):
        group = group.sort_values("second")
        axis.plot(
            group["second"],
            group["value"],
              color=PLOT_BLUE,
            alpha=0.25,
            linewidth=1.0,
            label="Từng lượt" if first else None,
        )
        first = False
    mean = frame.groupby("second", as_index=False)["value"].mean().sort_values("second")
    axis.plot(mean["second"], mean["value"], color=PLOT_RED, linewidth=2.2, label="Trung bình")
    _style_axes(axis, title, "Giây từ bắt đầu pha đo", ylabel)
    axis.legend(frameon=False, loc="upper right")


def _plot_grouped_metric_panels(
    summary: pd.DataFrame,
    *,
    category_column: str,
    series_column: str | None,
    metric_specs: list[tuple[str, str, str]],
    title_prefix: str,
      output_name: str,
      plt,
      bar_width_scale: float = 1.0,
      single_series_legend: bool = False,
  ) -> None:
    if summary.empty:
        return
    categories = list(dict.fromkeys(summary[category_column].astype(str)))
    palette = PLOT_PALETTE
    series_count = (
        len(list(dict.fromkeys(summary[series_column].astype(str))))
        if series_column is not None
        else 1
    )
    bar_width = PLOT_BAR_WIDTH * bar_width_scale
    active_width = bar_width
    bar_step = active_width
    group_width = (
        active_width * max(series_count, 1)
        if series_column is not None
        else active_width
    )
    category_step = group_width + active_width * PLOT_GROUP_GAP_FACTOR
    x_positions = [index * category_step for index in range(len(categories))]
    panel_width = max(3.0, 2.35 + category_step * max(len(categories), 1))
    fig, axes = plt.subplots(1, len(metric_specs), figsize=(panel_width * len(metric_specs), 3.8))
    if len(metric_specs) == 1:
        axes = [axes]

    if series_column is None:
        for axis, (metric, title, ylabel) in zip(axes, metric_specs):
            values = _values_by_category(summary, category_column, categories, f"{metric}_median")
            mins = _values_by_category(summary, category_column, categories, f"{metric}_min")
            maxs = _values_by_category(summary, category_column, categories, f"{metric}_max")
            errors = _error_span(values, mins, maxs)
            axis.bar(
                x_positions,
                values,
                width=bar_width,
                yerr=errors,
                capsize=3,
                error_kw={"ecolor": PLOT_ORANGE, "elinewidth": 1.0, "capthick": 1.0},
                color=palette[: len(categories)],
                edgecolor="none",
                linewidth=0,
            )
            if single_series_legend:
                axis.set_xticks([])
            else:
                axis.set_xticks(x_positions)
                axis.set_xticklabels(categories, rotation=25, ha="right")
            _style_axes(axis, title, "", ylabel)
        if single_series_legend:
            from matplotlib.patches import Patch

            handles = [
                Patch(facecolor=palette[index % len(palette)], edgecolor="none", label=category)
                for index, category in enumerate(categories)
            ]
            axes[0].legend(handles=handles, frameon=False, loc="upper left")
    else:
        series_values = list(dict.fromkeys(summary[series_column].astype(str)))
        width = bar_width
        series_step = bar_width
        for axis, (metric, title, ylabel) in zip(axes, metric_specs):
            for series_index, series_value in enumerate(series_values):
                subset = summary[summary[series_column].astype(str) == series_value]
                offset = (series_index - (len(series_values) - 1) / 2) * series_step
                values = _values_by_category(subset, category_column, categories, f"{metric}_median")
                mins = _values_by_category(subset, category_column, categories, f"{metric}_min")
                maxs = _values_by_category(subset, category_column, categories, f"{metric}_max")
                errors = _error_span(values, mins, maxs)
                axis.bar(
                    [position + offset for position in x_positions],
                    values,
                    width=width,
                    yerr=errors,
                    capsize=3,
                    error_kw={"ecolor": PLOT_ORANGE, "elinewidth": 1.0, "capthick": 1.0},
                    label=series_value,
                    color=palette[series_index % len(palette)],
                    edgecolor="none",
                    linewidth=0,
                )
            axis.set_xticks(x_positions)
            axis.set_xticklabels(categories, rotation=20, ha="right")
            _style_axes(axis, title, "", ylabel)
        axes[0].legend(frameon=False)
    fig.suptitle(title_prefix, y=1.03)
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / output_name, bbox_inches="tight")
    plt.close(fig)


def _values_by_category(
    frame: pd.DataFrame,
    category_column: str,
    categories: list[str],
    value_column: str,
) -> list[float]:
    values: list[float] = []
    for category in categories:
        subset = frame[frame[category_column].astype(str) == category]
        value = _safe_median(subset, value_column)
        values.append(float(value) if not pd.isna(value) else float("nan"))
    return values


def _error_span(values: list[float], mins: list[float], maxs: list[float]):
    lower = [
        0.0 if pd.isna(value) or pd.isna(minimum) else max(value - minimum, 0.0)
        for value, minimum in zip(values, mins)
    ]
    upper = [
        0.0 if pd.isna(value) or pd.isna(maximum) else max(maximum - value, 0.0)
        for value, maximum in zip(values, maxs)
    ]
    return [lower, upper]


def _style_axes(ax, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, linestyle="--", alpha=0.25, axis="y")
    ax.tick_params(axis="x", labelrotation=20)


def write_input_manifest(entries: dict[str, Path | None]) -> Path:
    rows: list[dict] = []
    for role, path in entries.items():
        if path is None:
            rows.append(
                {
                    "role": role,
                    "path": "NA",
                    "rows": "NA",
                    "modified_utc": "NA",
                    "status": "missing",
                }
            )
            continue
        try:
            row_count = len(pd.read_csv(path))
        except Exception:
            row_count = "NA"
        rows.append(
            {
                "role": role,
                "path": str(path),
                "rows": row_count,
                "modified_utc": datetime.fromtimestamp(
                    path.stat().st_mtime,
                    tz=timezone.utc,
                ).isoformat(timespec="seconds"),
                "status": "ok",
            }
        )
    path = TABLES_DIR / "paper_inputs_manifest.csv"
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def main() -> int:
    runtime_confirmation_path, runtime_confirmation = _read_latest(
        "capacity_runtime_sensitivity_candidate_confirmation_*.csv"
    )
    runtime_screening_path, runtime_screening = _read_latest(
        "capacity_runtime_sensitivity_screening*.csv"
    )
    capacity_path, capacity = _read_latest("capacity_load_threshold_3run_*.csv")
    model_path, model_feature = _read_latest("model_feature_tradeoff_3run_*.csv")
    fault_path, fault = _read_latest("fault_recovery_3run_*.csv")
    overload_path, overload = _read_latest("overload_degradation_3run_*.csv")

    frames = [capacity, model_feature, fault, overload]
    manifest = export_latency_timeseries(frames)
    table_outputs = build_tables(
        runtime_screening=runtime_screening,
        runtime_confirmation=runtime_confirmation,
        capacity=capacity,
        model_feature=model_feature,
        fault=fault,
        overload=overload,
    )
    build_plots(
        runtime_screening=runtime_screening,
        capacity=capacity,
        model_feature=model_feature,
        fault=fault,
        overload=overload,
    )
    input_manifest = write_input_manifest(
        {
            "runtime_sensitivity_confirmation": runtime_confirmation_path,
            "runtime_sensitivity_screening": runtime_screening_path,
            "capacity_calibration": capacity_path,
            "model_feature_tradeoff": model_path,
            "fault_recovery": fault_path,
            "overload_degradation": overload_path,
        }
    )

    print("Inputs:")
    for path in (
        runtime_confirmation_path,
        runtime_screening_path,
        capacity_path,
        model_path,
        fault_path,
        overload_path,
    ):
        if path is not None:
            print(f"  {path}")
    print(f"Input manifest: {input_manifest}")
    print(f"Latency timeseries manifest: {EVALUATION_DIR / 'streaming_latency_timeseries_manifest.csv'}")
    print(f"Latency timeseries rows: {len(manifest)}")
    print(f"Tables directory: {TABLES_DIR}")
    print(f"Tables: {len(table_outputs)}")
    print(f"Plots directory: {PLOTS_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
