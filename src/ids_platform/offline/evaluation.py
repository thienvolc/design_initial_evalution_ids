"""Phase 04 – Evaluate ALL trained models on the held-out test split.

Evaluates every model saved by Phase03 (not just best_model), enabling
model-side trade-off analysis (e.g. RF recall vs GBT cost).

Per model:
  artifacts/evaluation/test_metrics_{name}.json
  artifacts/evaluation/test_per_attack_recall_{name}.csv
  artifacts/evaluation/predictions_{name}.parquet

Consolidated:
  artifacts/evaluation/test_summary.csv      – one row per model
  artifacts/evaluation/confusion_matrix.csv  – best model only
  artifacts/evaluation/benchmark_runs.csv    – append-only log
"""

from __future__ import annotations

import csv
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from ids_platform.offline.config import load_json, write_json
from ids_platform.offline.log import get_logger
from ids_platform.offline.metrics import compute_binary_metrics, compute_per_attack_recall
from ids_platform.offline.paths import Paths


# ── benchmark log helper ───────────────────────────────────────────────

_BENCHMARK_LOG_COLUMNS = [
    "run_id", "run_ts", "pipeline", "model_name", "feature_set",
    "n_events", "accuracy", "precision", "recall", "f1",
    "fpr", "fnr", "roc_auc", "predict_latency_ms",
    "throughput_rps", "notes",
]


def _append_benchmark(
    bench_path: Path, *, model_name: str, n_features: int,
    n_events: int, metrics: dict[str, float],
    latency_ms: float, throughput: float,
) -> None:
    now = datetime.now(timezone.utc)
    row = {
        "run_id": f"offline_{now:%Y%m%d_%H%M%S}",
        "run_ts": now.isoformat(timespec="seconds"),
        "pipeline": "offline_binary",
        "model_name": model_name,
        "feature_set": f"n={n_features}",
        "n_events": n_events,
        **{k: f"{v:.6f}" for k, v in metrics.items()},
        "predict_latency_ms": f"{latency_ms:.1f}",
        "throughput_rps": f"{throughput:.0f}",
        "notes": "",
    }

    write_header = not bench_path.exists()
    with bench_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=_BENCHMARK_LOG_COLUMNS,
            extrasaction="ignore",
        )
        if write_header:
            writer.writeheader()
        writer.writerow(row)

# ── multi-threshold evaluation ─────────────────────────────────────────


def _evaluate_operating_points(
    y_test: Series,
    y_score: Series,
    operating_points: dict,
    log,
    model_name: str,
) -> dict:
    """Evaluate test at each FPR budget threshold."""
    result = {}
    for budget_key, point in operating_points.items():
        thr = float(point.get("threshold", 0.5))
        p = pd.Series(np.where(y_score.to_numpy() >= thr, 1, 0), index=y_score.index)
        m, _ = compute_binary_metrics(y_test, p, y_score)
        result[budget_key] = {
            "threshold": thr,
            "recall": round(m["recall"], 6),
            "precision": round(m["precision"], 6),
            "f1": round(m["f1"], 6),
            "fpr": round(m["fpr"], 6),
        }
        log.info("    test@%s: thr=%.3f  recall=%.4f  prec=%.4f  f1=%.4f  fpr=%.4f",
                 budget_key, thr, m["recall"], m["precision"], m["f1"], m["fpr"])
    return result


# ── single model evaluation ────────────────────────────────────────────


def _log_score_distribution(y_test: Series, y_score: Series, log) -> None:
    for class_value, class_name in [(0, "benign"), (1, "attack")]:
        class_scores = y_score[y_test == class_value]
        if len(class_scores) > 0:
            log.info(
                "    scores[%s]: mean=%.4f  median=%.4f  p5=%.4f  p95=%.4f",
                class_name,
                class_scores.mean(),
                class_scores.median(),
                class_scores.quantile(0.05),
                class_scores.quantile(0.95),
            )


def _save_per_attack_recall(
    *,
    model_name: str,
    y_test: Series,
    y_pred: Series,
    labels_test: Optional[Series],
    threshold: float,
    evaluation_dir: Path,
    log,
) -> list[dict]:
    if labels_test is None:
        return []

    per_attack_rows = compute_per_attack_recall(
        y_test,
        y_pred,
        labels_test,
        threshold=threshold,
        model_name=model_name,
    )
    for row in per_attack_rows:
        log.info(
            "      %-35s recall=%.4f (%d/%d)",
            row["attack_type"],
            row["recall"],
            row["detected"],
            row["n_samples"],
        )

    if per_attack_rows:
        pd.DataFrame(per_attack_rows).to_csv(
            evaluation_dir / f"test_per_attack_recall_{model_name}.csv",
            index=False,
        )

    return per_attack_rows


def _save_feature_importance(
    *,
    model,
    feature_names: list[str],
    model_name: str,
    evaluation_dir: Path,
    log,
) -> None:
    inner_model = model.named_steps.get("model")
    if not hasattr(inner_model, "feature_importances_"):
        return

    feature_importance_pairs = sorted(
        zip(feature_names, inner_model.feature_importances_),
        key=lambda item: item[1],
        reverse=True,
    )
    importance_rows = [
        {"feature": feature_name, "importance": round(float(importance), 6)}
        for feature_name, importance in feature_importance_pairs
    ]
    write_json(
        evaluation_dir / f"feature_importance_{model_name}.json",
        {"importances": importance_rows},
    )
    log.info(
        "    top-5 features: %s",
        ", ".join(
            f"{feature_name}={importance:.4f}"
            for feature_name, importance in feature_importance_pairs[:5]
        ),
    )


def _build_test_summary_row(
    *,
    model_name: str,
    opt_threshold: float,
    fpr_budget: float,
    metrics: dict[str, float],
    default_metrics: dict[str, float],
    latency_ms: float,
    throughput_rps: float,
) -> dict:
    return {
        "model": model_name,
        "opt_threshold": opt_threshold,
        "fpr_budget": fpr_budget,
        **metrics,
        **default_metrics,
        "latency_ms": round(latency_ms, 1),
        "throughput_rps": round(throughput_rps, 0),
    }


def _evaluate_one_model(
    model_name: str,
    model_path: Path,
    opt_thr: float,
    fpr_budget: float,
    operating_points: dict,
    x_test: DataFrame,
    y_test: Series,
    labels_test: Optional[Series],
    features: list[str],
    paths: Paths,
    log,
) -> dict:
    """Evaluate a single model. Returns summary row dict."""

    model = joblib.load(model_path)
    log.info("  ── %s (thr=%.4f, FPR budget=%.0f%%) ──",
             model_name, opt_thr, fpr_budget * 100)

    # predict
    t0 = time.perf_counter()
    y_score: Optional[Series] = None
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(x_test)[:, 1]
        y_score = pd.Series(proba, index=y_test.index)
        y_pred = pd.Series(np.where(np.asarray(proba) >= opt_thr, 1, 0), index=y_test.index)
    else:
        y_pred = pd.Series(model.predict(x_test), index=y_test.index)
    elapsed = time.perf_counter() - t0

    metrics, cm = compute_binary_metrics(y_test, y_pred, y_score)
    latency_ms = elapsed * 1000
    throughput = len(y_test) / elapsed if elapsed > 0 else 0.0

    # score distributions
    if y_score is not None:
        _log_score_distribution(y_test, y_score, log)

    pred_atk = int(y_pred.sum())
    log.info("    predictions: pred_attack=%d (%.2f%%)  pred_benign=%d",
             pred_atk, 100 * pred_atk / len(y_pred), len(y_pred) - pred_atk)

    # per-attack recall
    per_attack_rows = _save_per_attack_recall(
        model_name=model_name,
        y_test=y_test,
        y_pred=y_pred,
        labels_test=labels_test,
        threshold=opt_thr,
        evaluation_dir=paths.evaluation_dir,
        log=log,
    )

    # multi-threshold trade-off
    test_op = {}
    if y_score is not None and operating_points:
        test_op = _evaluate_operating_points(
            y_test,
            y_score,
            operating_points,
            log,
            model_name,
        )

    # default 0.5 metrics
    default_metrics = {}
    if y_score is not None:
        dp = pd.Series(np.where(y_score.to_numpy() >= 0.5, 1, 0), index=y_score.index)
        dm, _ = compute_binary_metrics(y_test, dp, y_score)
        default_metrics = {
            "default_recall": dm["recall"], "default_precision": dm["precision"],
            "default_f1": dm["f1"], "default_fpr": dm["fpr"],
        }

    # per-model test_metrics JSON
    write_json(
        paths.evaluation_dir / f"test_metrics_{model_name}.json",
        {
            "dataset": "test", "model": model_name,
            "n": len(y_test), "opt_threshold": opt_thr,
            "fpr_budget": fpr_budget,
            **metrics,
            **default_metrics,
            "operating_points": test_op,
            "per_attack_recall": per_attack_rows,
            "latency_ms": round(latency_ms, 1),
            "throughput_rps": round(throughput, 0),
        },
    )

    # per-model predictions
    pred_df = pd.DataFrame({
        "label_binary": y_test.values,
        "prediction": y_pred.values,
    })
    if y_score is not None:
        pred_df["score"] = y_score.values
    if labels_test is not None:
        pred_df["label"] = labels_test.values
    pred_df.to_parquet(
        paths.evaluation_dir / f"predictions_{model_name}.parquet", index=False,
    )

    # confusion matrix (saved per model)
    cm_df = pd.DataFrame(
        cm, index=["actual_benign", "actual_attack"],
        columns=["pred_benign", "pred_attack"],
    )
    cm_df.to_csv(paths.evaluation_dir / f"confusion_matrix_{model_name}.csv")

    # feature importance
    _save_feature_importance(
        model=model,
        feature_names=features,
        model_name=model_name,
        evaluation_dir=paths.evaluation_dir,
        log=log,
    )

    # benchmark log
    _append_benchmark(
        paths.evaluation_dir / "benchmark_runs.csv",
        model_name=model_name,
        n_features=len(features),
        n_events=len(y_test),
        metrics=metrics,
        latency_ms=latency_ms,
        throughput=throughput,
    )

    log.info(
        "    %-25s  f1=%.4f  recall=%.4f  prec=%.4f  fpr=%.4f  thr=%.4f  "
        "latency=%.0fms  throughput=%.0f rps",
        model_name, metrics["f1"], metrics["recall"], metrics["precision"],
        metrics["fpr"], opt_thr, latency_ms, throughput,
    )

    return _build_test_summary_row(
        model_name=model_name,
        opt_threshold=opt_thr,
        fpr_budget=fpr_budget,
        metrics=metrics,
        default_metrics=default_metrics,
        latency_ms=latency_ms,
        throughput_rps=throughput,
    )


def _load_model_operating_points(paths: Paths, model_name: str, model_meta: dict) -> dict:
    """Load per-model operating points saved by Phase03.

    Fallback to best_model.json only for the selected best model to keep
    compatibility with older artifacts.
    """
    op_path = paths.models_dir / f"operating_points_{model_name}.json"
    if op_path.exists():
        payload = load_json(op_path)
        operating_points = payload.get("operating_points", {})
        if isinstance(operating_points, dict):
            return operating_points

    if str(model_meta.get("best_model", "")) == model_name:
        operating_points = model_meta.get("operating_points", {})
        if isinstance(operating_points, dict):
            return operating_points

    return {}


# ── public API ──────────────────────────────────────────────────────────


def run(paths: Paths) -> None:
    log = get_logger("phase04", paths.log_dir / "phase04_evaluate.log")
    paths.ensure_dirs()

    for dep, desc in [
        (paths.test_path, "test split"),
        (paths.preprocessing_dir / "feature_manifest.json", "feature manifest"),
        (paths.models_dir / "best_model.json", "best model meta"),
    ]:
        if not dep.exists():
            raise FileNotFoundError(f"Missing {desc}: {dep}")

    manifest = load_json(paths.preprocessing_dir / "feature_manifest.json")
    model_meta = load_json(paths.models_dir / "best_model.json")
    features = list(manifest["feature_columns"])
    best_name = str(model_meta["best_model"])
    fpr_budget = float(model_meta.get("fpr_budget", 0.05))

    # ── discover all trained models ──
    valid_metrics_path = paths.models_dir / "valid_metrics.csv"
    if valid_metrics_path.exists():
        valid_metrics_df = pd.read_csv(valid_metrics_path)
        model_entries = []
        for _, row in valid_metrics_df.iterrows():
            name = str(row["model"])
            model_path = paths.models_dir / f"{name}.joblib"
            if model_path.exists():
                model_entries.append({
                    "name": name,
                    "path": model_path,
                    "opt_threshold": float(row.get("opt_threshold", 0.5)),
                    "fpr_budget": fpr_budget,
                })
        log.info("  discovered %d models from valid_metrics.csv", len(model_entries))
    else:
        # fallback: only best model
        model_entries = [{
            "name": best_name,
            "path": Path(model_meta["model_path"]),
            "opt_threshold": float(model_meta.get("opt_threshold", 0.5)),
            "fpr_budget": fpr_budget,
        }]

    # ── load test data once ──
    test_df = pd.read_parquet(paths.test_path)
    missing = [f for f in features if f not in test_df.columns]
    if missing:
        raise ValueError(f"Missing features in test: {missing}")

    x_test = test_df[features].copy()
    y_test = test_df["label_binary"].astype(int)
    labels_test = test_df["label"].copy() if "label" in test_df.columns else None

    n_benign = int(y_test.eq(0).sum())
    n_attack = int(y_test.eq(1).sum())
    log.info("START  n_test=%d  benign=%d (%.1f%%)  attack=%d (%.1f%%)  models=%d",
             len(y_test), n_benign, 100 * n_benign / len(y_test),
             n_attack, 100 * n_attack / len(y_test), len(model_entries))

    if labels_test is not None:
        attack_type_counts = labels_test[y_test == 1].value_counts()
        for attack_type, count in attack_type_counts.items():
            log.info("    %-40s %d", attack_type, count)

    # ── evaluate each model ──
    summary_rows = []
    for entry in model_entries:
        is_best = entry["name"] == best_name
        tag = " [BEST]" if is_best else ""
        log.info("\n  evaluating %s%s ...", entry["name"], tag)

        row = _evaluate_one_model(
            model_name=entry["name"],
            model_path=entry["path"],
            opt_thr=entry["opt_threshold"],
            fpr_budget=entry["fpr_budget"],
            operating_points=_load_model_operating_points(paths, entry["name"], model_meta),
            x_test=x_test,
            y_test=y_test,
            labels_test=labels_test,
            features=features,
            paths=paths,
            log=log,
        )
        row["is_best"] = is_best
        summary_rows.append(row)

    # ── consolidated summary ──
    summary_df = pd.DataFrame(summary_rows).sort_values(
        ["recall", "fpr"], ascending=[False, True],
    )
    summary_df.to_csv(paths.evaluation_dir / "test_summary.csv", index=False)

    log.info("\n  ── TEST SUMMARY ──")
    for _, r in summary_df.iterrows():
        tag = " *" if r.get("is_best") else ""
        log.info(
            "    %-25s  recall=%.4f  prec=%.4f  f1=%.4f  fpr=%.4f  "
            "latency=%.0fms  throughput=%.0f rps%s",
            r["model"], r["recall"], r["precision"], r["f1"], r["fpr"],
            r["latency_ms"], r["throughput_rps"], tag,
        )

    log.info("DONE   %d models evaluated, best=%s", len(summary_rows), best_name)
