from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd
from matplotlib import rcParams

LAYER_A_LABEL_ORDER = ["A_low", "A_mid", "A_high"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an empirical CDF plot from streaming metrics time series CSVs"
    )
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="CSV files containing per-batch metrics time series",
    )
    parser.add_argument(
        "--metric",
        type=str,
        default="source_to_emit_p95_ms",
        help="Numeric column to visualize as an empirical CDF",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output PNG path",
    )
    parser.add_argument(
        "--title",
        type=str,
        default="Empirical CDF of Source-to-Emit P95 Latency",
        help="Plot title",
    )
    parser.add_argument(
        "--xlabel",
        type=str,
        default="Latency (ms)",
        help="X axis label",
    )
    parser.add_argument(
        "--group-by-label",
        action="store_true",
        help="Pool values from inputs that map to the same short label into one empirical CDF curve",
    )
    parser.add_argument(
        "--show-raw-replicates",
        action="store_true",
        help="When grouping by label, draw each repeated run as a faint background empirical CDF",
    )
    return parser.parse_args()


def _resolve_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _short_label_from_run_tag(run_tag: str) -> str:
    normalized = str(run_tag).lower()
    if normalized.startswith("layera_"):
        match = re.search(r"_(a_(low|mid|high))_", normalized)
        if match:
            return f"A_{match.group(2)}"
    if normalized.startswith("layerb_"):
        match = re.search(
            r"layerb_.*?_(logistic_regression|random_forest|gradient_boosting)_(reduced|full)_",
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
    if normalized.startswith("layerc_"):
        match = re.search(r"layerc_\d+_([a-z_]+)_", normalized)
        if match:
            return match.group(1)
    if normalized.startswith("watermark_"):
        match = re.search(r"watermark_\d+_([0-9]+s)_", normalized)
        if match:
            return match.group(1)
    if normalized.startswith("load_"):
        match = re.search(r"load_\d+_([a-z0-9_]+)_", normalized)
        if match:
            return match.group(1)
    return run_tag


def _load_metric_series(path: Path, metric_name: str) -> tuple[str, pd.Series]:
    frame = pd.read_csv(path)
    if metric_name not in frame.columns:
        raise ValueError(f"Missing metric column {metric_name} in {path}")

    values = pd.to_numeric(frame[metric_name], errors="coerce").dropna().sort_values().reset_index(drop=True)
    if values.empty:
        raise ValueError(f"No numeric values for {metric_name} in {path}")

    if "run_tag" in frame.columns and not frame["run_tag"].dropna().empty:
        run_tag = str(frame["run_tag"].dropna().iloc[0])
    else:
        run_tag = path.stem
    return _short_label_from_run_tag(run_tag), values


def _ordered_labels(labels: list[str]) -> list[str]:
    remaining = [label for label in labels if label not in LAYER_A_LABEL_ORDER]
    return [label for label in LAYER_A_LABEL_ORDER if label in labels] + sorted(remaining)


def main() -> int:
    args = parse_args()
    input_paths = [_resolve_path(item) for item in args.inputs]
    output_path = _resolve_path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("matplotlib is required to build latency CDF plots") from exc

    plt.figure(figsize=(8.4, 5.0))

    if args.group_by_label:
        grouped: dict[str, list[pd.Series]] = {}
        for input_path in input_paths:
            label, values = _load_metric_series(input_path, args.metric)
            grouped.setdefault(label, []).append(values)

        color_cycle = rcParams["axes.prop_cycle"].by_key().get("color", ["#1f77b4", "#ff7f0e", "#2ca02c"])
        for index, label in enumerate(_ordered_labels(list(grouped.keys()))):
            series_list = grouped[label]
            color = color_cycle[index % len(color_cycle)]
            if args.show_raw_replicates:
                for values in series_list:
                    raw_cdf = (values.index + 1) / len(values)
                    plt.plot(
                        values,
                        raw_cdf,
                        linewidth=1.0,
                        alpha=0.12,
                        color=color,
                        label="_nolegend_",
                    )

            values = pd.concat(series_list, ignore_index=True).sort_values().reset_index(drop=True)
            cdf = (values.index + 1) / len(values)
            plt.plot(values, cdf, linewidth=1.8, alpha=0.92, color=color, label=label)
    else:
        color_cycle = rcParams["axes.prop_cycle"].by_key().get("color", ["#1f77b4", "#ff7f0e", "#2ca02c"])
        label_colors: dict[str, str] = {}
        label_seen_count: dict[str, int] = {}
        for input_path in input_paths:
            label, values = _load_metric_series(input_path, args.metric)
            color = label_colors.setdefault(label, color_cycle[len(label_colors) % len(color_cycle)])
            seen_count = label_seen_count.get(label, 0)
            label_seen_count[label] = seen_count + 1
            cdf = (values.index + 1) / len(values)
            plt.plot(
                values,
                cdf,
                linewidth=1.8,
                alpha=0.55 if seen_count > 0 else 0.9,
                color=color,
                label=label if seen_count == 0 else "_nolegend_",
            )

    plt.title(args.title)
    plt.xlabel(args.xlabel)
    plt.ylabel("Empirical CDF")
    plt.grid(True, linestyle="--", alpha=0.28)
    plt.legend(frameon=False)
    plt.tight_layout()
    plt.savefig(output_path, dpi=180)
    plt.close()
    print(f"Saved plot: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
