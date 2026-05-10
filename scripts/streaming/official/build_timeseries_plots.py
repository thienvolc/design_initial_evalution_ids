from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd
from matplotlib import rcParams


PLOT_SPECS = [
    ("rows_per_sec", "Throughput Over Time", "Rows / sec", "timeseries_rows_per_sec.png"),
    (
        "source_to_emit_p95_ms",
        "Source-to-Emit P95 Latency Over Time",
        "Latency (ms)",
        "timeseries_source_to_emit_p95_ms.png",
    ),
    (
        "ingest_to_emit_p95_ms",
        "Ingest-to-Emit P95 Latency Over Time",
        "Latency (ms)",
        "timeseries_ingest_to_emit_p95_ms.png",
    ),
    ("e2e_p95_ms", "End-to-End P95 Latency Over Time", "Latency (ms)", "timeseries_e2e_p95_ms.png"),
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
    ("driver_cpu_percent", "Driver CPU Over Time", "CPU (%)", "timeseries_driver_cpu_percent.png"),
    ("executor_mem_util_avg", "Executor Memory Utilization Over Time", "Utilization", "timeseries_executor_mem_util_avg.png"),
]

RUN_GROUP_ORDER = ["layer_a", "layer_b", "layer_c", "watermark", "load", "other"]
RUN_GROUP_TITLES = {
    "layer_a": "Layer A",
    "layer_b": "Layer B",
    "layer_c": "Layer C",
    "watermark": "Watermark",
    "load": "Load Quality",
    "other": "Other Runs",
}

LAYER_A_LABEL_ORDER = ["A_low", "A_mid", "A_high"]


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
    parser.add_argument(
        "--group-by-label",
        action="store_true",
        help="Aggregate runs that share the same short label into one representative time-series curve",
    )
    parser.add_argument(
        "--show-raw-replicates",
        action="store_true",
        help="When grouping by label, draw each repeated run as a faint background trace",
    )
    parser.add_argument(
        "--band-mode",
        type=str,
        choices=["none", "minmax", "std"],
        default="none",
        help="When grouping by label, draw an uncertainty band using min-max or mean +/- std",
    )
    parser.add_argument(
        "--x-max",
        type=float,
        default=None,
        help="Optional maximum x-axis value in elapsed seconds",
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
    run_tag = str(frame["run_tag"].dropna().iloc[0] or path.stem)
    frame["run_group"] = _infer_run_group(run_tag)
    frame["short_label"] = _short_run_label(run_tag)
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


def _infer_run_group(run_tag: str) -> str:
    normalized = run_tag.lower()
    if normalized.startswith("layera_"):
        return "layer_a"
    if normalized.startswith("layerb_"):
        return "layer_b"
    if normalized.startswith("layerc_"):
        return "layer_c"
    if normalized.startswith("watermark_"):
        return "watermark"
    if normalized.startswith("load_"):
        return "load"
    return "other"


def _short_run_label(run_tag: str) -> str:
    normalized = run_tag.lower()
    if normalized.startswith("layera_"):
        match = re.search(r"_(a_(low|mid|high))_", normalized)
        if match:
            return f"A_{match.group(2)}"
        return "Layer A"
    if normalized.startswith("layerb_"):
        match = re.search(
            r"layerb_.*?_(logistic_regression|random_forest|gradient_boosting)_(reduced|full)_\d+$",
            normalized,
        )
        if match:
            model_name = match.group(1)
            feature_set = match.group(2)
            if model_name == "logistic_regression":
                prefix = "LR"
            elif model_name == "random_forest":
                prefix = "RF"
            elif model_name == "gradient_boosting":
                prefix = "GBT"
            else:
                prefix = model_name
            suffix = "F" if feature_set == "full" else "R"
            return f"{prefix}-{suffix}"
        return "Layer B"
    if normalized.startswith("layerc_"):
        match = re.search(r"layerc_\d+_([a-z_]+)_\d+$", normalized)
        if match:
            return match.group(1)
        return "Layer C"
    if normalized.startswith("watermark_"):
        match = re.search(r"watermark_\d+_([0-9]+s)_\d+$", normalized)
        if match:
            return match.group(1)
        return "watermark"
    if normalized.startswith("load_"):
        match = re.search(r"load_\d+_([a-z0-9_]+)_\d+$", normalized)
        if match:
            return match.group(1)
        return "load"
    return run_tag


def _output_name_with_group(output_name: str, group_name: str) -> str:
    stem = Path(output_name).stem
    suffix = Path(output_name).suffix
    return f"{stem}.{group_name}{suffix}"


def _ordered_group_labels(labels: list[str], group_name: str) -> list[str]:
    if group_name == "layer_a":
        remaining = [label for label in labels if label not in LAYER_A_LABEL_ORDER]
        return [label for label in LAYER_A_LABEL_ORDER if label in labels] + sorted(remaining)
    return sorted(labels)


def _group_plot_title(group_name: str, title: str, group_by_label: bool) -> str:
    base_title = f"{RUN_GROUP_TITLES.get(group_name, group_name)}: {title}"
    return base_title


def _plot_metric(
    *,
    plt,
    frames: list[pd.DataFrame],
    paths: list[Path],
    column_name: str,
    title: str,
    y_label: str,
    output_path: Path,
    legend_title: str,
    group_by_label: bool,
    show_raw_replicates: bool,
    band_mode: str,
    x_max: float | None,
) -> bool:
    plt.figure(figsize=(8.4, 5.0))
    plotted = False
    skipped_reasons: list[str] = []
    legend_labels: list[str] = []
    color_cycle = rcParams["axes.prop_cycle"].by_key().get("color", ["#1f77b4", "#ff7f0e", "#2ca02c"])
    label_colors: dict[str, str] = {}
    label_seen_count: dict[str, int] = {}
    if group_by_label:
        grouped_frames: dict[str, list[pd.DataFrame]] = {}
        for frame, path in zip(frames, paths):
            if column_name not in frame.columns:
                continue
            series = pd.to_numeric(frame[column_name], errors="coerce")
            plot_frame = frame.assign(_series=series).dropna(subset=["_series"])
            if plot_frame.empty:
                skipped_reasons.append(f"{path.stem}:{_explain_empty_series(frame, column_name)}")
                continue
            short_label = str(plot_frame["short_label"].iloc[0] or path.stem)
            plot_frame = plot_frame.copy()
            plot_frame["time_bin_seconds"] = plot_frame["relative_seconds"].round().astype(int)
            grouped_frames.setdefault(short_label, []).append(plot_frame)

        if grouped_frames:
            first_group = next(iter(grouped_frames.values()))
            group_name = str(first_group[0]["run_group"].iloc[0])
        else:
            group_name = "other"
        ordered_labels = _ordered_group_labels(list(grouped_frames.keys()), group_name)
        for short_label in ordered_labels:
            frame_list = grouped_frames[short_label]
            color = label_colors.setdefault(short_label, color_cycle[len(label_colors) % len(color_cycle)])
            per_run_series: list[pd.Series] = []
            for raw_frame in frame_list:
                run_series = (
                    raw_frame.groupby("time_bin_seconds", as_index=True)["_series"]
                    .mean()
                    .sort_index()
                )
                per_run_series.append(run_series)
                if show_raw_replicates:
                    plt.plot(
                        run_series.index,
                        run_series.values,
                        linewidth=1.0,
                        alpha=0.12,
                        color=color,
                        label="_nolegend_",
                    )

            combined = pd.concat(per_run_series, axis=1)
            combined.columns = [f"run_{index + 1}" for index in range(len(per_run_series))]
            stats = pd.DataFrame(
                {
                    "time_bin_seconds": combined.index.to_numpy(),
                    "mean": combined.mean(axis=1, skipna=True).to_numpy(),
                    "min": combined.min(axis=1, skipna=True).to_numpy(),
                    "max": combined.max(axis=1, skipna=True).to_numpy(),
                    "std": combined.std(axis=1, skipna=True).fillna(0.0).to_numpy(),
                }
            ).sort_values("time_bin_seconds")

            if band_mode == "minmax":
                plt.fill_between(
                    stats["time_bin_seconds"],
                    stats["min"],
                    stats["max"],
                    color=color,
                    alpha=0.16,
                    linewidth=0,
                )
            elif band_mode == "std":
                lower = stats["mean"] - stats["std"]
                upper = stats["mean"] + stats["std"]
                plt.fill_between(
                    stats["time_bin_seconds"],
                    lower,
                    upper,
                    color=color,
                    alpha=0.16,
                    linewidth=0,
                )

            point_count = len(stats)
            marker = "o" if point_count <= 12 else None
            marker_size = 2.4 if marker else 0
            plt.plot(
                stats["time_bin_seconds"],
                stats["mean"],
                linewidth=1.8,
                alpha=0.9,
                marker=marker,
                markersize=marker_size,
                color=color,
                label=short_label,
            )
            legend_labels.append(short_label)
            plotted = True
    else:
        for frame, path in zip(frames, paths):
            if column_name not in frame.columns:
                continue
            series = pd.to_numeric(frame[column_name], errors="coerce")
            plot_frame = frame.assign(_series=series).dropna(subset=["_series"])
            if plot_frame.empty:
                skipped_reasons.append(f"{path.stem}:{_explain_empty_series(frame, column_name)}")
                continue

            short_label = str(plot_frame["short_label"].iloc[0] or path.stem)
            color = label_colors.setdefault(short_label, color_cycle[len(label_colors) % len(color_cycle)])
            seen_count = label_seen_count.get(short_label, 0)
            label_seen_count[short_label] = seen_count + 1
            point_count = len(plot_frame)
            marker = "o" if point_count <= 12 else None
            marker_size = 2.4 if marker else 0
            plt.plot(
                plot_frame["relative_seconds"],
                plot_frame["_series"],
                linewidth=1.4,
                alpha=0.55 if seen_count > 0 else 0.9,
                marker=marker,
                markersize=marker_size,
                color=color,
                label=short_label if seen_count == 0 else "_nolegend_",
            )
            if seen_count == 0:
                legend_labels.append(short_label)
            plotted = True

    if not plotted:
        if skipped_reasons:
            print(f"Skipped plot {column_name}: " + ", ".join(skipped_reasons))
        plt.close()
        return False

    plt.title(title)
    plt.xlabel("Elapsed time (s)")
    plt.ylabel(y_label)
    if x_max is not None:
        plt.xlim(right=x_max)
    plt.grid(True, linestyle="--", alpha=0.28)
    if len(legend_labels) <= 4:
        plt.legend(frameon=False, fontsize=9, loc="best")
    else:
        plt.legend(
            frameon=False,
            fontsize=8,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.18),
            ncol=min(3, len(legend_labels)),
        )
    plt.tight_layout()
    plt.savefig(output_path, dpi=170)
    plt.close()
    print(f"Saved plot: {output_path}")
    return True


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
        _plot_metric(
            plt=plt,
            frames=frames,
            paths=input_files,
            column_name=column_name,
            title=title,
            y_label=y_label,
            output_path=output_dir / output_name,
            legend_title="All runs",
            group_by_label=args.group_by_label,
            show_raw_replicates=args.show_raw_replicates,
            band_mode=args.band_mode,
            x_max=args.x_max,
        )

        for group_name in RUN_GROUP_ORDER:
            group_pairs = [(frame, path) for frame, path in zip(frames, input_files) if str(frame["run_group"].iloc[0]) == group_name]
            if len(group_pairs) <= 1:
                continue
            group_frames = [frame for frame, _ in group_pairs]
            group_paths = [path for _, path in group_pairs]
            _plot_metric(
                plt=plt,
                frames=group_frames,
                paths=group_paths,
                column_name=column_name,
                title=_group_plot_title(group_name, title, args.group_by_label),
                y_label=y_label,
                output_path=output_dir / _output_name_with_group(output_name, group_name),
                legend_title=RUN_GROUP_TITLES.get(group_name, group_name),
                group_by_label=args.group_by_label,
                show_raw_replicates=args.show_raw_replicates,
                band_mode=args.band_mode,
                x_max=args.x_max,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
