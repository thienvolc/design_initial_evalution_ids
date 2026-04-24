from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd


PLOT_SPECS = [
    ("rows_per_sec", "Throughput Over Time", "Rows / sec", "timeseries_rows_per_sec.png"),
    ("source_to_emit_p95_ms", "Source-to-Emit P95 Latency Over Time", "Latency (ms)", "timeseries_source_to_emit_p95_ms.png"),
    ("ingest_to_emit_p95_ms", "Ingest-to-Emit P95 Latency Over Time", "Latency (ms)", "timeseries_ingest_to_emit_p95_ms.png"),
    ("e2e_p95_ms", "Legacy End-to-End P95 Alias Over Time", "Latency (ms)", "timeseries_e2e_p95_ms.png"),
    (
        "late_event_ratio_interpretable",
        "Interpretable Late-Event Ratio Over Time",
        "Ratio",
        "timeseries_late_event_ratio_interpretable.png",
    ),
    (
        "freshness_signal_ratio",
        "Freshness Signal Ratio Over Time",
        "Ratio",
        "timeseries_freshness_signal_ratio.png",
    ),
    ("kafka_lag_records_total", "Kafka Lag Over Time", "Lag (records)", "timeseries_kafka_lag_records_total.png"),
    ("driver_rss_mb", "Driver RSS Over Time", "Memory (MB)", "timeseries_driver_rss_mb.png"),
    ("driver_cpu_percent", "Driver CPU Percent Over Time", "CPU (%)", "timeseries_driver_cpu_percent.png"),
    ("executor_mem_util_avg", "Executor Memory Utilization Over Time", "Utilization", "timeseries_executor_mem_util_avg.png"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build standard report plots from streaming metrics time series CSVs")
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="One or more CSV files or directories containing per-run metrics time series CSVs",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="artifacts/streaming/plots",
        help="Directory where PNG plots will be written",
    )
    return parser.parse_args()


def _resolve_input_files(raw_inputs: list[str]) -> list[Path]:
    files: list[Path] = []
    seen: set[Path] = set()
    for raw_input in raw_inputs:
        path = Path(raw_input)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if path.is_dir():
            candidates = sorted(path.glob("*.csv"))
        else:
            candidates = [path]
        for candidate in candidates:
            if candidate in seen or not candidate.exists():
                continue
            seen.add(candidate)
            files.append(candidate)
    return files


def _load_timeseries_frame(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if "ts_epoch_ms" not in frame.columns:
        raise ValueError(f"Missing ts_epoch_ms in {path}")

    frame = frame.copy()
    frame["ts_epoch_ms"] = pd.to_numeric(frame["ts_epoch_ms"], errors="coerce")
    frame = frame.dropna(subset=["ts_epoch_ms"])
    if frame.empty:
        raise ValueError(f"No valid rows with ts_epoch_ms in {path}")

    frame = frame.sort_values("ts_epoch_ms")
    frame["relative_seconds"] = (frame["ts_epoch_ms"] - frame["ts_epoch_ms"].min()) / 1000.0
    if "run_tag" not in frame.columns or frame["run_tag"].dropna().empty:
        frame["run_tag"] = path.stem
    return frame


def _explain_empty_series(frame: pd.DataFrame, column_name: str) -> str:
    if column_name == "late_event_ratio_interpretable":
        if "watermark_delay_sec" in frame.columns:
            watermark_delay = pd.to_numeric(frame["watermark_delay_sec"], errors="coerce").dropna()
            if not watermark_delay.empty and (watermark_delay == 0).all():
                return "not_applicable_when_watermark_delay_sec_0"
    if column_name == "metric_warnings":
        return "blank_means_no_warning"
    if column_name in {"precision", "recall", "f1"}:
        return "may_be_undefined_for_some_batches"
    return "no_numeric_data"


def main() -> int:
    args = parse_args()
    input_files = _resolve_input_files(list(args.inputs))
    if not input_files:
        raise FileNotFoundError("No input CSV files found")

    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required to build timeseries plots") from exc

    frames = [_load_timeseries_frame(path) for path in input_files]
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    for column_name, title, y_label, output_name in PLOT_SPECS:
        plt.figure(figsize=(10, 5))
        plotted = False
        skipped_reasons: list[str] = []
        for frame, path in zip(frames, input_files):
            if column_name not in frame.columns:
                continue
            series = pd.to_numeric(frame[column_name], errors="coerce")
            plot_frame = frame.assign(_series=series).dropna(subset=["_series"])
            if plot_frame.empty:
                skipped_reasons.append(f"{path.stem}:{_explain_empty_series(frame, column_name)}")
                continue

            run_tag = str(plot_frame["run_tag"].iloc[0] or path.stem)
            plt.plot(plot_frame["relative_seconds"], plot_frame["_series"], marker="o", linewidth=1.5, label=run_tag)
            plotted = True

        if not plotted:
            if skipped_reasons:
                print(f"Skipped plot {column_name}: " + ", ".join(skipped_reasons))
            plt.close()
            continue

        plt.title(title)
        plt.xlabel("Elapsed time (s)")
        plt.ylabel(y_label)
        plt.grid(True, linestyle="--", alpha=0.4)
        plt.legend()
        plt.tight_layout()
        output_path = output_dir / output_name
        plt.savefig(output_path, dpi=150)
        plt.close()
        print(f"Saved plot: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
