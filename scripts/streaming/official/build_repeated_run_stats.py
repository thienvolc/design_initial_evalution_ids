from __future__ import annotations

import argparse
import csv
import math
import statistics
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]


LAYER_SPECS = {
    "layer_a": {
        "inputs": [
            "artifacts/streaming/evaluation/layer_a_summary_500k.csv",
            "artifacts/streaming/evaluation/layer_a_summary_500k_rerun02.csv",
            "artifacts/streaming/evaluation/layer_a_summary_500k_rerun03.csv",
        ],
        "group_keys": ["profile", "model", "feature_set"],
        "metrics": [
            "rows_per_sec_avg",
            "source_to_emit_p95_ms_max",
            "kafka_lag_records_max",
            "precision_avg",
            "recall_avg",
            "f1_avg",
            "fnr_avg",
        ],
    },
    "layer_b": {
        "inputs": [
            "artifacts/streaming/evaluation/layer_b_summary_500k_with_lr_full_rerun01.csv",
            "artifacts/streaming/evaluation/layer_b_summary_500k_with_lr_full_rerun02.csv",
            "artifacts/streaming/evaluation/layer_b_summary_500k_with_lr_full_rerun03.csv",
        ],
        "group_keys": ["model", "feature_set"],
        "metrics": [
            "source_to_emit_p95_ms",
            "kafka_lag_records",
            "precision",
            "recall",
            "f1",
            "fnr",
        ],
    },
    "layer_c": {
        "inputs": [
            "artifacts/streaming/evaluation/layer_c_summary_700k_fault.csv",
            "artifacts/streaming/evaluation/layer_c_summary_700k_fault_rerun02.csv",
            "artifacts/streaming/evaluation/layer_c_summary_700k_fault_rerun03.csv",
        ],
        "group_keys": ["scenario", "model", "feature_set"],
        "metrics": [
            "recovery_seconds",
            "rows_per_sec_after_fault",
            "source_to_emit_p95_ms_after_fault",
            "ingest_to_emit_p95_ms_after_fault",
        ],
    },
    "watermark": {
        "inputs": [
            "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun01.csv",
            "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun02.csv",
            "artifacts/streaming/evaluation/watermark_summary_fullschedule_rerun03.csv",
        ],
        "group_keys": ["load_profile", "watermark_delay_sec", "drop_late_events"],
        "metrics": [
            "source_to_emit_p95_ms",
            "ingest_to_emit_p95_ms",
            "precision",
            "recall",
            "f1",
            "fnr",
            "late_event_ratio_interpretable",
            "freshness_signal_ratio",
        ],
    },
    "load_quality": {
        "inputs": [
            "artifacts/streaming/evaluation/load_quality_summary_stress_fullschedule_rerun01.csv",
            "artifacts/streaming/evaluation/load_quality_summary_stress_fullschedule_rerun02.csv",
            "artifacts/streaming/evaluation/load_quality_summary_stress_fullschedule_rerun03.csv",
        ],
        "group_keys": ["load_profile", "rows_per_sec_target", "max_rows"],
        "metrics": [
            "rows_per_sec_actual",
            "source_to_emit_p95_ms",
            "ingest_to_emit_p95_ms",
            "precision",
            "recall",
            "f1",
            "fnr",
            "kafka_lag_records",
        ],
    },
}


def _resolve(path_str: str) -> Path:
    path = Path(path_str)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fmt_num(value: float | None, digits: int = 4) -> str:
    if value is None or math.isnan(value):
        return ""
    return f"{value:.{digits}f}"


def _fmt_mean_std(mean_value: float | None, std_value: float | None, digits: int = 4) -> str:
    if mean_value is None:
        return ""
    if std_value is None:
        std_value = 0.0
    return f"{mean_value:.{digits}f} +/- {std_value:.{digits}f}"


def _load_rows(paths: list[Path]) -> list[dict]:
    rows: list[dict] = []
    for run_index, path in enumerate(paths, start=1):
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                copied = dict(row)
                copied["_source_file"] = path.name
                copied["_source_run_index"] = str(run_index)
                rows.append(copied)
    return rows


def _group_rows(rows: list[dict], keys: list[str]) -> dict[tuple[str, ...], list[dict]]:
    grouped: dict[tuple[str, ...], list[dict]] = {}
    for row in rows:
        key = tuple(str(row.get(name, "") or "").strip() for name in keys)
        grouped.setdefault(key, []).append(row)
    return grouped


def _summarize_metric(values: list[float]) -> dict[str, float | int]:
    mean_value = statistics.mean(values)
    std_value = statistics.stdev(values) if len(values) >= 2 else 0.0
    return {
        "n": len(values),
        "mean": mean_value,
        "std": std_value,
        "min": min(values),
        "max": max(values),
    }


def _build_summary_rows(layer_name: str, spec: dict) -> list[dict]:
    input_paths = [_resolve(item) for item in spec["inputs"]]
    rows = _load_rows(input_paths)
    grouped = _group_rows(rows, spec["group_keys"])

    summary_rows: list[dict] = []
    for group_key, group_rows in sorted(grouped.items()):
        base_row: dict[str, str] = {"layer": layer_name}
        for key_name, key_value in zip(spec["group_keys"], group_key):
            base_row[key_name] = key_value

        source_files = sorted({str(row.get("_source_file", "")).strip() for row in group_rows})
        source_run_indexes = sorted({str(row.get("_source_run_index", "")).strip() for row in group_rows})
        base_row["n_runs"] = str(len(source_files))
        base_row["source_files"] = ";".join(source_files)
        base_row["source_run_indexes"] = ";".join(source_run_indexes)
        base_row["statuses"] = ";".join(sorted({str(row.get("status", "")).strip() for row in group_rows if str(row.get("status", "")).strip()}))

        for metric_name in spec["metrics"]:
            values = []
            for row in group_rows:
                numeric = _safe_float(row.get(metric_name))
                if numeric is not None:
                    values.append(numeric)
            if not values:
                continue
            stats = _summarize_metric(values)
            base_row[f"{metric_name}_mean"] = _fmt_num(stats["mean"])
            base_row[f"{metric_name}_std"] = _fmt_num(stats["std"])
            base_row[f"{metric_name}_min"] = _fmt_num(stats["min"])
            base_row[f"{metric_name}_max"] = _fmt_num(stats["max"])
            base_row[f"{metric_name}_mean_std"] = _fmt_mean_std(stats["mean"], stats["std"])

        summary_rows.append(base_row)
    return summary_rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _metric_alias(metric_name: str) -> str:
    aliases = {
        "rows_per_sec_avg": "throughput_avg",
        "source_to_emit_p95_ms_max": "source_to_emit_p95_max",
        "kafka_lag_records_max": "kafka_lag_max",
        "precision_avg": "precision",
        "recall_avg": "recall",
        "f1_avg": "f1",
        "fnr_avg": "fnr",
        "source_to_emit_p95_ms": "source_to_emit_p95",
        "kafka_lag_records": "kafka_lag",
        "recovery_seconds": "recovery_seconds",
        "rows_per_sec_after_fault": "throughput_after_fault",
        "source_to_emit_p95_ms_after_fault": "source_to_emit_p95_after_fault",
        "ingest_to_emit_p95_ms_after_fault": "ingest_to_emit_p95_after_fault",
        "source_to_emit_p95_ms": "source_to_emit_p95",
        "ingest_to_emit_p95_ms": "ingest_to_emit_p95",
        "late_event_ratio_interpretable": "late_event_ratio",
        "freshness_signal_ratio": "freshness_signal_ratio",
        "rows_per_sec_actual": "throughput_actual",
        "kafka_lag_records": "kafka_lag",
    }
    return aliases.get(metric_name, metric_name)


def _write_markdown(path: Path, layer_rows: dict[str, list[dict]], specs: dict) -> None:
    lines: list[str] = []
    lines.append("# Repeated Run Summary")
    lines.append("")
    lines.append("Các bảng dưới đây tổng hợp 3 lượt chạy cho từng layer, trình bày theo dạng `mean ± std`.")
    lines.append("")

    for layer_name in ["layer_a", "layer_b", "layer_c", "watermark", "load_quality"]:
        rows = layer_rows.get(layer_name) or []
        spec = specs[layer_name]
        lines.append(f"## {layer_name}")
        lines.append("")
        if not rows:
            lines.append("Không có dữ liệu.")
            lines.append("")
            continue

        header_keys = list(spec["group_keys"])
        metric_headers = [_metric_alias(metric) for metric in spec["metrics"]]
        lines.append("| " + " | ".join(header_keys + metric_headers) + " |")
        lines.append("| " + " | ".join(["---"] * (len(header_keys) + len(metric_headers))) + " |")
        for row in rows:
            values = [row.get(key, "") for key in header_keys]
            for metric in spec["metrics"]:
                values.append(row.get(f"{metric}_mean_std", ""))
            lines.append("| " + " | ".join(values) + " |")
        lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build repeated-run mean/std summaries for official matrices")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/streaming/evaluation/repeated_run_stats",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = _resolve(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    layer_rows: dict[str, list[dict]] = {}
    for layer_name, spec in LAYER_SPECS.items():
        rows = _build_summary_rows(layer_name, spec)
        layer_rows[layer_name] = rows
        _write_csv(output_dir / f"{layer_name}_repeated_stats.csv", rows)

    _write_markdown(output_dir / "repeated_run_summary.md", layer_rows, LAYER_SPECS)
    print(f"Saved repeated-run summaries to: {output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
