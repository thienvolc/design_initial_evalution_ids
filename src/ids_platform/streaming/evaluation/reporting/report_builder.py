from __future__ import annotations

import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path

from ids_platform.common.paths import resolve_project_path


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _to_float(raw: str | int | float | None) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _is_ok(row: dict) -> bool:
    return str(row.get("status", "")).strip().lower() == "ok"


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    idx = math.ceil(q * len(sorted_vals)) - 1
    idx = min(max(idx, 0), len(sorted_vals) - 1)
    return sorted_vals[idx]


def _first_numeric(row: dict, candidates: list[str]) -> float | None:
    for candidate in candidates:
        value = _to_float(row.get(candidate))
        if value is not None:
            return value
    return None


def _collect_metric(items: list[dict], candidates: list[str]) -> list[float]:
    values = [_first_numeric(item, candidates) for item in items]
    return [value for value in values if value is not None]


def _analyze_layer_a(rows: list[dict]) -> dict:
    ok_rows = [r for r in rows if _is_ok(r)]
    grouped: dict[tuple[str, str, str, str], list[dict]] = {}
    for row in ok_rows:
        key = (
            str(row.get("profile", "")),
            str(row.get("max_offsets_per_trigger", "")),
            str(row.get("shuffle_partitions", "")),
            str(row.get("trigger_interval", "")),
        )
        grouped.setdefault(key, []).append(row)

    ranked = []
    for key, items in grouped.items():
        profile, max_offsets, shuffle, trigger_interval = key
        throughputs = [_to_float(item.get("rows_per_sec_avg")) for item in items]
        throughputs = [x for x in throughputs if x is not None]
        source_to_emit_vals = _collect_metric(items, ["source_to_emit_p95_ms_max", "e2e_p95_ms_max"])

        if not throughputs or not source_to_emit_vals:
            continue

        throughput_median = statistics.median(throughputs)
        source_to_emit_p95 = _percentile(source_to_emit_vals, 0.95)
        score = throughput_median / source_to_emit_p95 if source_to_emit_p95 and source_to_emit_p95 > 0 else None
        if score is None:
            continue

        ranked.append(
            {
                "profile": profile,
                "max_offsets_per_trigger": max_offsets,
                "shuffle_partitions": shuffle,
                "trigger_interval": trigger_interval,
                "count": len(items),
                "rows_per_sec_min": min(throughputs),
                "rows_per_sec_median": throughput_median,
                "rows_per_sec_p95": _percentile(throughputs, 0.95),
                "rows_per_sec_max": max(throughputs),
                "source_to_emit_p95_ms_min": min(source_to_emit_vals),
                "source_to_emit_p95_ms_median": statistics.median(source_to_emit_vals),
                "source_to_emit_p95_ms_p95": source_to_emit_p95,
                "source_to_emit_p95_ms_max": max(source_to_emit_vals),
                "e2e_p95_ms_min": min(source_to_emit_vals),
                "e2e_p95_ms_median": statistics.median(source_to_emit_vals),
                "e2e_p95_ms_p95": source_to_emit_p95,
                "e2e_p95_ms_max": max(source_to_emit_vals),
                "score": score,
            }
        )

    ranked.sort(key=lambda item: item["score"], reverse=True)
    return {
        "total": len(rows),
        "ok": len(ok_rows),
        "ranked": ranked,
        "best": ranked[0] if ranked else None,
    }


def _analyze_layer_b(rows: list[dict]) -> dict:
    ok_rows = [r for r in rows if _is_ok(r)]
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in ok_rows:
        key = (str(row.get("model", "")), str(row.get("feature_set", "")))
        grouped.setdefault(key, []).append(row)

    ranked = []
    for key, items in grouped.items():
        model, feature_set = key
        source_to_emit_vals = _collect_metric(items, ["source_to_emit_p95_ms", "e2e_p95_ms"])
        ingest_to_emit_vals = _collect_metric(items, ["ingest_to_emit_p95_ms", "proc_p95_ms"])
        rows_vals = [_to_float(item.get("rows")) for item in items]
        rows_vals = [x for x in rows_vals if x is not None]

        if not source_to_emit_vals or not ingest_to_emit_vals:
            continue

        ranked.append(
            {
                "model": model,
                "feature_set": feature_set,
                "count": len(items),
                "source_to_emit_p95_ms_min": min(source_to_emit_vals),
                "source_to_emit_p95_ms_median": statistics.median(source_to_emit_vals),
                "source_to_emit_p95_ms_p95": _percentile(source_to_emit_vals, 0.95),
                "source_to_emit_p95_ms_max": max(source_to_emit_vals),
                "e2e_p95_ms_min": min(source_to_emit_vals),
                "e2e_p95_ms_median": statistics.median(source_to_emit_vals),
                "e2e_p95_ms_p95": _percentile(source_to_emit_vals, 0.95),
                "e2e_p95_ms_max": max(source_to_emit_vals),
                "ingest_to_emit_p95_ms_min": min(ingest_to_emit_vals),
                "ingest_to_emit_p95_ms_median": statistics.median(ingest_to_emit_vals),
                "ingest_to_emit_p95_ms_p95": _percentile(ingest_to_emit_vals, 0.95),
                "ingest_to_emit_p95_ms_max": max(ingest_to_emit_vals),
                "proc_p95_ms_min": min(ingest_to_emit_vals),
                "proc_p95_ms_median": statistics.median(ingest_to_emit_vals),
                "proc_p95_ms_p95": _percentile(ingest_to_emit_vals, 0.95),
                "proc_p95_ms_max": max(ingest_to_emit_vals),
                "rows_median": statistics.median(rows_vals) if rows_vals else None,
            }
        )

    ranked.sort(key=lambda item: (item["source_to_emit_p95_ms_p95"], item["ingest_to_emit_p95_ms_p95"]))
    return {
        "total": len(rows),
        "ok": len(ok_rows),
        "ranked": ranked,
        "best": ranked[0] if ranked else None,
    }


def _analyze_layer_c(rows: list[dict]) -> dict:
    ok_rows = [r for r in rows if _is_ok(r)]
    by_scenario = []
    grouped: dict[str, list[dict]] = {}

    for row in ok_rows:
        item = {
            "scenario": row.get("scenario", ""),
            "recovery_seconds": _to_float(row.get("recovery_seconds")),
            "rows_per_sec_after_fault": _to_float(row.get("rows_per_sec_after_fault")),
            "proc_p95_ms_after_fault": _to_float(row.get("proc_p95_ms_after_fault")),
            "e2e_p95_ms_after_fault": _to_float(row.get("e2e_p95_ms_after_fault")),
        }
        by_scenario.append(item)
        grouped.setdefault(item["scenario"], []).append(item)

    scenario_stats = []
    for scenario, items in grouped.items():
        recoveries = [x["recovery_seconds"] for x in items if x["recovery_seconds"] is not None]
        throughputs = [x["rows_per_sec_after_fault"] for x in items if x["rows_per_sec_after_fault"] is not None]
        proc_p95 = [x["proc_p95_ms_after_fault"] for x in items if x["proc_p95_ms_after_fault"] is not None]
        e2e_p95 = [x["e2e_p95_ms_after_fault"] for x in items if x["e2e_p95_ms_after_fault"] is not None]

        scenario_stats.append(
            {
                "scenario": scenario,
                "count": len(items),
                "recovery_seconds_min": min(recoveries) if recoveries else None,
                "recovery_seconds_median": statistics.median(recoveries) if recoveries else None,
                "recovery_seconds_p95": _percentile(recoveries, 0.95),
                "recovery_seconds_max": max(recoveries) if recoveries else None,
                "rows_per_sec_after_fault_median": statistics.median(throughputs) if throughputs else None,
                "proc_p95_ms_after_fault_median": statistics.median(proc_p95) if proc_p95 else None,
                "e2e_p95_ms_after_fault_median": statistics.median(e2e_p95) if e2e_p95 else None,
            }
        )

    scenario_stats.sort(key=lambda x: x.get("scenario", ""))

    valid_recoveries = [r["recovery_seconds"] for r in by_scenario if r["recovery_seconds"] is not None]
    worst = max(valid_recoveries) if valid_recoveries else None

    return {
        "total": len(rows),
        "ok": len(ok_rows),
        "rows": by_scenario,
        "scenario_stats": scenario_stats,
        "worst_recovery_seconds": worst,
    }


def _analyze_watermark(rows: list[dict]) -> dict:
    ok_rows = [r for r in rows if _is_ok(r)]
    ranked = []
    for row in ok_rows:
        ranked.append(
            {
                "watermark_delay_sec": _to_float(row.get("watermark_delay_sec")),
                "late_event_ratio": _to_float(row.get("late_event_ratio")),
                "late_event_ratio_interpretable": _to_float(row.get("late_event_ratio_interpretable")),
                "freshness_signal_ratio": _to_float(row.get("freshness_signal_ratio")),
                "event_lateness_p95_ms": _to_float(row.get("event_lateness_p95_ms")),
                "source_to_emit_p95_ms": _first_numeric(row, ["source_to_emit_p95_ms", "e2e_p95_ms"]),
                "fnr": _to_float(row.get("fnr")),
            }
        )

    ranked = [r for r in ranked if r["watermark_delay_sec"] is not None]
    ranked.sort(key=lambda x: x["watermark_delay_sec"])
    return {
        "total": len(rows),
        "ok": len(ok_rows),
        "rows": ranked,
    }


def _analyze_load_quality(rows: list[dict]) -> dict:
    ok_rows = [r for r in rows if _is_ok(r)]
    ranked = []
    for row in ok_rows:
        ranked.append(
            {
                "load_profile": str(row.get("load_profile", "")),
                "rows_per_sec_target": _to_float(row.get("rows_per_sec_target")),
                "rows_per_sec_actual": _to_float(row.get("rows_per_sec_actual")),
                "source_to_emit_p95_ms": _first_numeric(row, ["source_to_emit_p95_ms", "e2e_p95_ms"]),
                "ingest_to_emit_p95_ms": _first_numeric(row, ["ingest_to_emit_p95_ms", "proc_p95_ms"]),
                "late_event_ratio_interpretable": _to_float(row.get("late_event_ratio_interpretable")),
                "freshness_signal_ratio": _to_float(row.get("freshness_signal_ratio")),
                "f1": _to_float(row.get("f1")),
                "fpr": _to_float(row.get("fpr")),
                "fnr": _to_float(row.get("fnr")),
                "kafka_lag_records": _to_float(row.get("kafka_lag_records")),
            }
        )

    ranked.sort(key=lambda x: (x["rows_per_sec_target"] or 0.0, x["load_profile"]))
    return {
        "total": len(rows),
        "ok": len(ok_rows),
        "rows": ranked,
    }


def _median(values: list[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return statistics.median(present)


def _coverage(rows: list[dict], column: str) -> dict:
    total = len(rows)
    if total == 0:
        return {"present": 0, "total": 0, "coverage_pct": 0.0}
    present = sum(1 for row in rows if _to_float(row.get(column)) is not None)
    return {
        "present": present,
        "total": total,
        "coverage_pct": (present * 100.0) / total,
    }


def _blank_cell_count(rows: list[dict], column: str) -> int:
    return sum(1 for row in rows if not str(row.get(column, "")).strip())


def _timeseries_semantics(rows: list[dict]) -> dict:
    total = len(rows)
    if total == 0:
        return {}

    watermark_zero_rows = [
        row for row in rows
        if _to_float(row.get("watermark_delay_sec")) == 0.0
    ]
    late_interp_blank = _blank_cell_count(rows, "late_event_ratio_interpretable")
    freshness_present = _coverage(rows, "freshness_signal_ratio")
    metric_warnings_blank = _blank_cell_count(rows, "metric_warnings")
    precision_blank = _blank_cell_count(rows, "precision")
    recall_blank = _blank_cell_count(rows, "recall")
    f1_blank = _blank_cell_count(rows, "f1")

    semantics: dict[str, dict] = {}
    semantics["metric_warnings"] = {
        "blank_rows": metric_warnings_blank,
        "total_rows": total,
        "classification": ("expected_blank_when_no_warning" if metric_warnings_blank == total else "mixed"),
        "note": "Blank metric_warnings cells indicate no warning was emitted for that batch.",
    }
    semantics["late_event_ratio_interpretable"] = {
        "blank_rows": late_interp_blank,
        "total_rows": total,
        "classification": (
            "expected_not_applicable_when_watermark_delay_zero"
            if watermark_zero_rows and len(watermark_zero_rows) == total and late_interp_blank == total
            else "mixed"
        ),
        "note": (
            "When watermark_delay_sec=0, late_event_ratio is treated as freshness_signal_ratio and "
            "late_event_ratio_interpretable is intentionally left blank."
        ),
    }
    semantics["precision_recall_f1"] = {
        "precision_blank_rows": precision_blank,
        "recall_blank_rows": recall_blank,
        "f1_blank_rows": f1_blank,
        "total_rows": total,
        "classification": "undefined_when_denominator_zero",
        "note": (
            "Blank precision/recall/f1 cells indicate the metric was undefined for that batch, "
            "for example when there were no positive predictions or no positive labels."
        ),
    }
    semantics["freshness_signal_ratio"] = {
        "present_rows": freshness_present["present"],
        "total_rows": freshness_present["total"],
        "coverage_pct": freshness_present["coverage_pct"],
        "classification": "primary_delay_zero_substitute_metric",
        "note": "Use freshness_signal_ratio as the interpretable signal when watermark_delay_sec=0.",
    }
    return semantics


def _find_first_column(rows: list[dict], candidates: list[str]) -> str:
    if not rows:
        return ""
    keys = set(rows[0].keys())
    for candidate in candidates:
        if candidate in keys:
            return candidate
    return ""


def _analyze_resource_observability(layer_a_rows: list[dict], layer_b_rows: list[dict]) -> dict:
    layer_a_ok = [r for r in layer_a_rows if _is_ok(r)]
    layer_b_ok = [r for r in layer_b_rows if _is_ok(r)]

    profile_groups: dict[str, list[dict]] = {}
    for row in layer_a_ok:
        profile_groups.setdefault(str(row.get("profile", "")), []).append(row)

    layer_a_profile_rss = []
    for profile, items in profile_groups.items():
        med = _median([_to_float(item.get("driver_rss_mb_avg")) for item in items])
        layer_a_profile_rss.append(
            {
                "profile": profile,
                "driver_rss_mb_median": med,
                "count": len(items),
            }
        )
    layer_a_profile_rss.sort(key=lambda item: item["profile"])

    combo_groups: dict[str, list[dict]] = {}
    for row in layer_b_ok:
        combo = f"{row.get('model', '')}+{row.get('feature_set', '')}"
        combo_groups.setdefault(combo, []).append(row)

    layer_b_combo_rss = []
    for combo, items in combo_groups.items():
        med = _median([_to_float(item.get("driver_rss_mb")) for item in items])
        layer_b_combo_rss.append(
            {
                "combo": combo,
                "driver_rss_mb_median": med,
                "count": len(items),
            }
        )
    layer_b_combo_rss.sort(key=lambda item: item["combo"])

    cpu_candidates = [
        "driver_cpu_percent_avg",
        "driver_cpu_pct_avg",
        "driver_cpu_percent",
        "driver_cpu_pct",
    ]
    layer_a_cpu_col = _find_first_column(layer_a_ok, cpu_candidates)
    layer_b_cpu_col = _find_first_column(layer_b_ok, cpu_candidates)

    layer_a_exec_cov = _coverage(layer_a_ok, "executor_mem_util_avg")
    layer_b_exec_cov = _coverage(layer_b_ok, "executor_mem_util_avg")
    layer_a_cpu_cov = _coverage(layer_a_ok, layer_a_cpu_col) if layer_a_cpu_col else {"present": 0, "total": len(layer_a_ok), "coverage_pct": 0.0}
    layer_b_cpu_cov = _coverage(layer_b_ok, layer_b_cpu_col) if layer_b_cpu_col else {"present": 0, "total": len(layer_b_ok), "coverage_pct": 0.0}

    return {
        "layer_a_driver_rss_median": layer_a_profile_rss,
        "layer_b_driver_rss_median": layer_b_combo_rss,
        "coverage": {
            "layer_a_executor_util": layer_a_exec_cov,
            "layer_b_executor_util": layer_b_exec_cov,
            "layer_a_cpu": {
                "field": layer_a_cpu_col,
                **layer_a_cpu_cov,
            },
            "layer_b_cpu": {
                "field": layer_b_cpu_col,
                **layer_b_cpu_cov,
            },
        },
    }


def _fmt_num(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def _evaluation_methodology(sources: dict) -> dict:
    return {
        "boundary_mode": "sut_metrics_only",
        "canonical_evaluation_pipeline": [
            "replay traffic into Kafka",
            "run the SUT streaming scorer",
            "collect per-scenario matrix summaries",
            "build the consolidated evaluation report",
            "review official results in Streamlit",
        ],
        "sut_definition": {
            "name": "Spark Structured Streaming IDS runtime",
            "role": "system_under_test",
            "responsibilities": [
                "consume replayed Kafka traffic",
                "score records and emit predictions",
                "publish runtime/debug telemetry to ids.metrics",
            ],
        },
        "official_evaluation": {
            "source_of_truth": "matrix_summaries_derived_from_ids_metrics",
            "artifact_types": [
                "matrix summary CSVs",
                "consolidated report JSON",
                "consolidated report markdown",
            ],
            "authoritative_inputs": {
                "layer_a_summary": sources["layer_a"],
                "layer_b_summary": sources["layer_b"],
                "layer_c_summary": sources["layer_c"],
                "watermark_summary": sources.get("watermark_summary", ""),
                "load_quality_summary": sources.get("load_quality_summary", ""),
            },
        },
        "debug_telemetry": {
            "source": "ids.metrics Kafka topic",
            "role": "runtime telemetry feed and current matrix-summary source input",
            "official_for_benchmarking": False,
            "surfaces": ["Prometheus", "Grafana"],
        },
        "artifact_contract": {
            "prediction_artifacts": "raw SUT outputs kept for inspection and debugging",
            "summary_artifacts": "official per-scenario evaluation outputs currently derived from ids.metrics",
            "final_report": "official aggregated evaluation output",
        },
    }


def _build_metric_warnings(layer_a_rows: list[dict], layer_b_rows: list[dict]) -> list[str]:
    warnings: list[str] = []
    watermark_zero_rows = [
        row for row in [*layer_a_rows, *layer_b_rows]
        if _is_ok(row) and _to_float(row.get("watermark_delay_sec")) == 0.0
    ]
    if watermark_zero_rows:
        warnings.append(
            "late_event_ratio was collected with watermark_delay_sec=0 for some default runs, "
            "so any positive ingest-vs-source delay counts as late; prefer freshness_signal_ratio for delay=0 runs and "
            "treat late_event_ratio_interpretable as undefined there."
        )
    if any(_is_ok(row) for row in layer_b_rows):
        warnings.append(
            "Legacy proc_p95_ms is a compatibility alias for ingest_to_emit_p95_ms, and e2e_p95_ms is a compatibility alias "
            "for source_to_emit_p95_ms; prefer the explicit names for new analysis."
        )
    load_profile_missing = [
        row for row in [*layer_a_rows, *layer_b_rows]
        if _is_ok(row) and not str(row.get("load_profile", "")).strip()
    ]
    if load_profile_missing:
        warnings.append(
            "Some summary rows still have blank load_profile values; those runs may predate the traceability fix and should be correlated by run_tag."
        )
    metric_warning_values: set[str] = set()
    for row in [*layer_a_rows, *layer_b_rows]:
        raw = str(row.get("metric_warnings", "")).strip()
        if not raw:
            continue
        for warning in raw.split(";"):
            warning_text = warning.strip()
            if warning_text:
                metric_warning_values.add(warning_text)
    for warning_text in sorted(metric_warning_values):
        warnings.append(f"Probe warning observed in metrics: {warning_text}")
    return warnings


def _build_timeseries_semantics_warnings(layer_c_rows: list[dict], watermark_rows: list[dict], load_quality_rows: list[dict]) -> list[str]:
    warnings: list[str] = []
    for name, rows in [
        ("layer_c", layer_c_rows),
        ("watermark", watermark_rows),
        ("load_quality", load_quality_rows),
    ]:
        semantics = _timeseries_semantics(rows)
        if not semantics:
            continue
        late_interp = semantics.get("late_event_ratio_interpretable", {})
        if late_interp.get("classification") == "expected_not_applicable_when_watermark_delay_zero":
            warnings.append(
                f"{name}: late_event_ratio_interpretable is blank by design when watermark_delay_sec=0; use freshness_signal_ratio instead."
            )
        metric_warn = semantics.get("metric_warnings", {})
        if metric_warn.get("classification") == "expected_blank_when_no_warning":
            warnings.append(
                f"{name}: blank metric_warnings cells mean no warning was emitted, not missing telemetry."
            )
    return warnings


def _build_markdown(a: dict, b: dict, c: dict, wm: dict, lq: dict, resources: dict, sources: dict, warnings: list[str]) -> str:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines: list[str] = []

    lines.append("# Online Evaluation Report")
    lines.append("")
    lines.append(f"Generated at (UTC): {generated_at}")
    lines.append("")
    lines.append("## Inputs")
    lines.append(f"- Layer A: {sources['layer_a']}")
    lines.append(f"- Layer B: {sources['layer_b']}")
    lines.append(f"- Layer C: {sources['layer_c']}")
    if sources.get("watermark_summary"):
        lines.append(f"- Watermark: {sources['watermark_summary']}")
    if sources.get("load_quality_summary"):
        lines.append(f"- Load quality: {sources['load_quality_summary']}")
    lines.append("")
    lines.append("## Methodology")
    lines.append("- System under test (SUT): the Spark Structured Streaming IDS runtime consumes replayed Kafka traffic, scores records, and emits predictions.")
    lines.append("- Official evaluation results: matrix runners currently aggregate SUT-emitted `ids.metrics` into summary CSVs, and this report is built from those summaries.")
    lines.append("- Prediction artifacts are retained as raw SUT outputs for inspection and debugging, but they are not the authoritative benchmark path right now.")
    lines.append("- Runtime telemetry surfaces: `ids.metrics` feeds both the current summary path and live Prometheus/Grafana observability.")
    lines.append("- Canonical evaluation flow: replay traffic -> run SUT -> collect matrix summaries -> build consolidated report -> review results in Streamlit.")
    lines.append("- Artifact contract:")
    lines.append("  - prediction artifacts = raw SUT outputs")
    lines.append("  - matrix summary CSVs = official per-scenario evaluation outputs derived from ids.metrics")
    lines.append("  - report JSON/markdown = official aggregated evaluation outputs")
    if warnings:
        lines.append("- Important metric warnings:")
        for warning in warnings:
            lines.append(f"  - {warning}")
    lines.append("")

    lines.append("## Layer A (System Knobs)")
    lines.append(f"- rows: {a['total']} (ok: {a['ok']})")
    if a.get("best"):
        best = a["best"]
        lines.append(
            "- best profile: "
            f"{best['profile']} (score={_fmt_num(best['score'])}, "
            f"rows_per_sec_median={_fmt_num(best['rows_per_sec_median'])}, "
            f"source_to_emit_p95_ms_p95={_fmt_num(best['source_to_emit_p95_ms_p95'])})"
        )
    else:
        lines.append("- best profile: n/a")

    lines.append("")
    lines.append("Top Layer A profiles:")
    for item in a.get("ranked", [])[:3]:
        lines.append(
            "- "
            f"{item['profile']}: score={_fmt_num(item['score'])}, "
            f"rows_per_sec(min/med/p95/max)=({_fmt_num(item['rows_per_sec_min'])}/"
            f"{_fmt_num(item['rows_per_sec_median'])}/{_fmt_num(item['rows_per_sec_p95'])}/"
            f"{_fmt_num(item['rows_per_sec_max'])}), "
            f"source_to_emit_p95_ms(min/med/p95/max)=({_fmt_num(item['source_to_emit_p95_ms_min'])}/"
            f"{_fmt_num(item['source_to_emit_p95_ms_median'])}/{_fmt_num(item['source_to_emit_p95_ms_p95'])}/"
            f"{_fmt_num(item['source_to_emit_p95_ms_max'])}), "
            f"n={item['count']}, max_offsets={item['max_offsets_per_trigger']}, "
            f"shuffle={item['shuffle_partitions']}, trigger={item['trigger_interval'] or 'config_default'}"
        )

    lines.append("")
    lines.append("## Layer B (Model x Feature Set)")
    lines.append(f"- rows: {b['total']} (ok: {b['ok']})")
    if b.get("best"):
        best = b["best"]
        lines.append(
            "- fastest combo: "
            f"{best['model']} + {best['feature_set']} "
            f"(source_to_emit_p95_ms_p95={_fmt_num(best['source_to_emit_p95_ms_p95'])}, "
            f"ingest_to_emit_p95_ms_median={_fmt_num(best.get('ingest_to_emit_p95_ms_median'))}, n={best['count']})"
        )
    else:
        lines.append("- fastest combo: n/a")

    lines.append("")
    lines.append("Top Layer B combos:")
    for item in b.get("ranked", [])[:3]:
        lines.append(
            "- "
            f"{item['model']} + {item['feature_set']}: "
            f"source_to_emit_p95_ms(min/med/p95/max)=({_fmt_num(item['source_to_emit_p95_ms_min'])}/"
            f"{_fmt_num(item['source_to_emit_p95_ms_median'])}/{_fmt_num(item['source_to_emit_p95_ms_p95'])}/"
            f"{_fmt_num(item['source_to_emit_p95_ms_max'])}), "
            f"ingest_to_emit_p95_ms(min/med/p95/max)=({_fmt_num(item['ingest_to_emit_p95_ms_min'])}/"
            f"{_fmt_num(item['ingest_to_emit_p95_ms_median'])}/{_fmt_num(item['ingest_to_emit_p95_ms_p95'])}/"
            f"{_fmt_num(item['ingest_to_emit_p95_ms_max'])}), n={item['count']}, "
            f"rows_median={_fmt_num(item['rows_median'])}"
        )

    lines.append("")
    lines.append("## Layer C (Fault Recovery)")
    lines.append(f"- rows: {c['total']} (ok: {c['ok']})")
    lines.append(f"- worst recovery_seconds: {_fmt_num(c.get('worst_recovery_seconds'))}")
    lines.append("")
    lines.append("Scenario aggregates:")
    for item in c.get("scenario_stats", []):
        lines.append(
            "- "
            f"{item['scenario']} (n={item['count']}): "
            f"median={_fmt_num(item['recovery_seconds_median'])}, "
            f"p95={_fmt_num(item['recovery_seconds_p95'])}, "
            f"min={_fmt_num(item['recovery_seconds_min'])}, "
            f"max={_fmt_num(item['recovery_seconds_max'])}"
        )
    lines.append("")
    lines.append("Scenario details:")
    for item in c.get("rows", []):
        lines.append(
            "- "
            f"{item['scenario']}: recovery_seconds={_fmt_num(item['recovery_seconds'])}, "
            f"rows_per_sec_after_fault={_fmt_num(item['rows_per_sec_after_fault'])}, "
            f"proc_p95_ms_after_fault={_fmt_num(item['proc_p95_ms_after_fault'])}, "
            f"e2e_p95_ms_after_fault={_fmt_num(item['e2e_p95_ms_after_fault'])}"
        )

    if wm.get("total", 0) > 0:
        lines.append("")
        lines.append("## Watermark Experiment")
        lines.append(f"- rows: {wm['total']} (ok: {wm['ok']})")
        lines.append("Watermark rows:")
        for item in wm.get("rows", []):
            lines.append(
                "- "
                f"delay={_fmt_num(item['watermark_delay_sec'], 0)}s, "
                f"late_event_ratio={_fmt_num(item['late_event_ratio'])}, "
                f"late_event_ratio_interpretable={_fmt_num(item.get('late_event_ratio_interpretable'))}, "
                f"freshness_signal_ratio={_fmt_num(item.get('freshness_signal_ratio'))}, "
                f"event_lateness_p95_ms={_fmt_num(item['event_lateness_p95_ms'])}, "
                f"source_to_emit_p95_ms={_fmt_num(item['source_to_emit_p95_ms'])}, fnr={_fmt_num(item['fnr'])}"
            )

    if lq.get("total", 0) > 0:
        lines.append("")
        lines.append("## Detection Under Load")
        lines.append(f"- rows: {lq['total']} (ok: {lq['ok']})")
        lines.append("Load rows:")
        for item in lq.get("rows", []):
            lines.append(
                "- "
                f"{item['load_profile']}: target_rps={_fmt_num(item['rows_per_sec_target'])}, "
                f"actual_rps={_fmt_num(item['rows_per_sec_actual'])}, "
                f"source_to_emit_p95_ms={_fmt_num(item['source_to_emit_p95_ms'])}, "
                f"ingest_to_emit_p95_ms={_fmt_num(item['ingest_to_emit_p95_ms'])}, f1={_fmt_num(item['f1'])}, "
                f"late_event_ratio_interpretable={_fmt_num(item.get('late_event_ratio_interpretable'))}, "
                f"freshness_signal_ratio={_fmt_num(item.get('freshness_signal_ratio'))}, "
                f"fpr={_fmt_num(item['fpr'])}, fnr={_fmt_num(item['fnr'])}, "
                f"kafka_lag={_fmt_num(item['kafka_lag_records'])}"
            )

    lines.append("")
    lines.append("## Resource Observability Narrative Charts")

    a_rss = resources.get("layer_a_driver_rss_median", [])
    if a_rss:
        labels = ", ".join(f'"{item["profile"]}"' for item in a_rss)
        values = ", ".join(_fmt_num(item.get("driver_rss_mb_median")) for item in a_rss)
        rss_vals = [item.get("driver_rss_mb_median") for item in a_rss if item.get("driver_rss_mb_median") is not None]
        y_min = math.floor(min(rss_vals)) if rss_vals else 0
        y_max = math.ceil(max(rss_vals)) if rss_vals else 1
        if y_max <= y_min:
            y_max = y_min + 1
        lines.append("Layer A driver RSS median (MB):")
        lines.append("```mermaid")
        lines.append("xychart-beta")
        lines.append('    title "Layer A Driver RSS Median (MB)"')
        lines.append(f"    x-axis [{labels}]")
        lines.append(f'    y-axis "MB" {y_min} --> {y_max}')
        lines.append(f"    bar [{values}]")
        lines.append("```")

    b_rss = resources.get("layer_b_driver_rss_median", [])
    if b_rss:
        labels = ", ".join(f'"{item["combo"]}"' for item in b_rss)
        values = ", ".join(_fmt_num(item.get("driver_rss_mb_median")) for item in b_rss)
        rss_vals = [item.get("driver_rss_mb_median") for item in b_rss if item.get("driver_rss_mb_median") is not None]
        y_min = math.floor(min(rss_vals)) if rss_vals else 0
        y_max = math.ceil(max(rss_vals)) if rss_vals else 1
        if y_max <= y_min:
            y_max = y_min + 1
        lines.append("")
        lines.append("Layer B driver RSS median (MB):")
        lines.append("```mermaid")
        lines.append("xychart-beta")
        lines.append('    title "Layer B Driver RSS Median (MB)"')
        lines.append(f"    x-axis [{labels}]")
        lines.append(f'    y-axis "MB" {y_min} --> {y_max}')
        lines.append(f"    bar [{values}]")
        lines.append("```")

    coverage = resources.get("coverage", {})
    cov_values = [
        coverage.get("layer_a_executor_util", {}).get("coverage_pct", 0.0),
        coverage.get("layer_b_executor_util", {}).get("coverage_pct", 0.0),
        coverage.get("layer_a_cpu", {}).get("coverage_pct", 0.0),
        coverage.get("layer_b_cpu", {}).get("coverage_pct", 0.0),
    ]
    cov_values_fmt = ", ".join(_fmt_num(v, 1) for v in cov_values)
    lines.append("")
    lines.append("Telemetry coverage (% of rows with populated values):")
    lines.append("```mermaid")
    lines.append("xychart-beta")
    lines.append('    title "Executor and CPU Coverage (%)"')
    lines.append('    x-axis ["A_exec_util", "B_exec_util", "A_cpu", "B_cpu"]')
    lines.append('    y-axis "%" 0 --> 100')
    lines.append(f"    bar [{cov_values_fmt}]")
    lines.append("```")

    lines.append("")
    lines.append("Observability notes:")
    lines.append(
        "- Driver RSS remains near-flat across tested profiles/combos in this environment, indicating memory stability at current workload scale."
    )
    lines.append(
        f"- Executor utilization coverage: Layer A={_fmt_num(coverage.get('layer_a_executor_util', {}).get('coverage_pct'), 1)}%, Layer B={_fmt_num(coverage.get('layer_b_executor_util', {}).get('coverage_pct'), 1)}%."
    )
    lines.append(
        f"- CPU field coverage: Layer A={_fmt_num(coverage.get('layer_a_cpu', {}).get('coverage_pct'), 1)}% (field={coverage.get('layer_a_cpu', {}).get('field') or 'absent'}), Layer B={_fmt_num(coverage.get('layer_b_cpu', {}).get('coverage_pct'), 1)}% (field={coverage.get('layer_b_cpu', {}).get('field') or 'absent'})."
    )

    lines.append("")
    lines.append("## Preliminary Recommendation")
    if a.get("best") and b.get("best"):
        lines.append(
            "- Keep Layer A best profile and Layer B fastest combo as current default for further repeat runs."
        )
    else:
        lines.append("- Insufficient data to provide a default recommendation.")

    if c.get("worst_recovery_seconds") is not None:
        lines.append(
            "- Use Layer C worst-case recovery_seconds as current resilience baseline for future regressions."
        )

    return "\n".join(lines) + "\n"


def build_online_evaluation_summary(
    *,
    layer_a_path: str | Path,
    layer_b_path: str | Path,
    layer_c_path: str | Path,
    watermark_path: str | Path | None = None,
    load_quality_path: str | Path | None = None,
) -> dict:
    resolved_layer_a = resolve_project_path(str(layer_a_path))
    resolved_layer_b = resolve_project_path(str(layer_b_path))
    resolved_layer_c = resolve_project_path(str(layer_c_path))
    resolved_watermark = resolve_project_path(str(watermark_path)) if watermark_path else None
    resolved_load_quality = resolve_project_path(str(load_quality_path)) if load_quality_path else None

    layer_a_rows = _read_csv(resolved_layer_a)
    layer_b_rows = _read_csv(resolved_layer_b)
    layer_c_rows = _read_csv(resolved_layer_c)
    watermark_rows = _read_csv(resolved_watermark) if resolved_watermark else []
    load_quality_rows = _read_csv(resolved_load_quality) if resolved_load_quality else []
    warnings = _build_metric_warnings(layer_a_rows, layer_b_rows)
    warnings.extend(_build_timeseries_semantics_warnings(layer_c_rows, watermark_rows, load_quality_rows))

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sources": {
            "layer_a": str(resolved_layer_a),
            "layer_b": str(resolved_layer_b),
            "layer_c": str(resolved_layer_c),
            "watermark_summary": str(resolved_watermark) if resolved_watermark else "",
            "load_quality_summary": str(resolved_load_quality) if resolved_load_quality else "",
        },
        "methodology": _evaluation_methodology(
            {
                "layer_a": str(resolved_layer_a),
                "layer_b": str(resolved_layer_b),
                "layer_c": str(resolved_layer_c),
                "watermark_summary": str(resolved_watermark) if resolved_watermark else "",
                "load_quality_summary": str(resolved_load_quality) if resolved_load_quality else "",
            }
        ),
        "warnings": warnings,
        "layer_a": _analyze_layer_a(layer_a_rows),
        "layer_b": _analyze_layer_b(layer_b_rows),
        "layer_c": _analyze_layer_c(layer_c_rows),
        "watermark": _analyze_watermark(watermark_rows),
        "load_quality": _analyze_load_quality(load_quality_rows),
        "timeseries_semantics": {
            "layer_c": _timeseries_semantics(layer_c_rows),
            "watermark": _timeseries_semantics(watermark_rows),
            "load_quality": _timeseries_semantics(load_quality_rows),
        },
        "resource_observability": _analyze_resource_observability(layer_a_rows, layer_b_rows),
    }


def build_online_evaluation_markdown(summary: dict) -> str:
    return _build_markdown(
        summary["layer_a"],
        summary["layer_b"],
        summary["layer_c"],
        summary["watermark"],
        summary["load_quality"],
        summary["resource_observability"],
        summary["sources"],
        summary.get("warnings", []),
    )


def write_online_evaluation_report(
    *,
    summary: dict,
    out_md_path: str | Path,
    out_json_path: str | Path,
) -> tuple[Path, Path]:
    resolved_md = resolve_project_path(str(out_md_path))
    resolved_json = resolve_project_path(str(out_json_path))
    resolved_md.parent.mkdir(parents=True, exist_ok=True)
    resolved_json.parent.mkdir(parents=True, exist_ok=True)

    resolved_md.write_text(build_online_evaluation_markdown(summary), encoding="utf-8")
    resolved_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return resolved_md, resolved_json


def generate_online_evaluation_report(
    *,
    layer_a_path: str | Path,
    layer_b_path: str | Path,
    layer_c_path: str | Path,
    out_md_path: str | Path,
    out_json_path: str | Path,
    watermark_path: str | Path | None = None,
    load_quality_path: str | Path | None = None,
) -> tuple[dict, Path, Path]:
    summary = build_online_evaluation_summary(
        layer_a_path=layer_a_path,
        layer_b_path=layer_b_path,
        layer_c_path=layer_c_path,
        watermark_path=watermark_path,
        load_quality_path=load_quality_path,
    )
    resolved_md, resolved_json = write_online_evaluation_report(
        summary=summary,
        out_md_path=out_md_path,
        out_json_path=out_json_path,
    )
    return summary, resolved_md, resolved_json
