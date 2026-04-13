from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import yaml
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline

from _common import PROJECT_ROOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild gradient_boosting model artifact")
    parser.add_argument("--max-train-rows", type=int, default=1_500_000)
    parser.add_argument("--max-valid-rows", type=int, default=400_000)
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--fpr-budget", type=float, default=0.05)
    return parser.parse_args()


def _stratified_sample(df: pd.DataFrame, label_col: str, max_rows: int) -> pd.DataFrame:
    if max_rows <= 0 or len(df) <= max_rows:
        return df

    frac = max_rows / len(df)
    parts: list[pd.DataFrame] = []
    for _, grp in df.groupby(label_col):
        n = max(1, int(len(grp) * frac))
        parts.append(grp.sample(n=min(n, len(grp)), random_state=42))
    return pd.concat(parts, ignore_index=True)


def _read_parquet_limited(path: Path, columns: list[str], max_rows: int, batch_size: int) -> pd.DataFrame:
    dataset = ds.dataset(path, format="parquet")
    chunks: list[pd.DataFrame] = []
    collected = 0

    for batch in dataset.to_batches(columns=columns, batch_size=batch_size):
        chunk = batch.to_pandas()
        chunks.append(chunk)
        collected += len(chunk)
        if max_rows > 0 and collected >= max_rows:
            break

    if not chunks:
        return pd.DataFrame(columns=columns)

    frame = pd.concat(chunks, ignore_index=True)
    if max_rows > 0 and len(frame) > max_rows:
        frame = frame.head(max_rows).copy()
    return frame


def _metrics(y_true: np.ndarray, preds: np.ndarray, proba: np.ndarray) -> dict[str, float]:
    tp = int(((preds == 1) & (y_true == 1)).sum())
    fp = int(((preds == 1) & (y_true == 0)).sum())
    tn = int(((preds == 0) & (y_true == 0)).sum())
    fn = int(((preds == 0) & (y_true == 1)).sum())

    total = tp + fp + tn + fn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    fnr = fn / (fn + tp) if (fn + tp) else 0.0
    acc = (tp + tn) / total if total else 0.0

    return {
        "accuracy": acc,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "fpr": fpr,
        "fnr": fnr,
        "roc_auc": float(roc_auc_score(y_true, proba)),
    }


def _best_threshold(y_true: np.ndarray, proba: np.ndarray, fpr_budget: float) -> float:
    n_neg = int((y_true == 0).sum())
    best_thr = 0.5
    best_recall = -1.0

    for thr in np.linspace(0.001, 0.999, 999):
        preds = (proba >= thr).astype(int)
        fp = int(((preds == 1) & (y_true == 0)).sum())
        tp = int(((preds == 1) & (y_true == 1)).sum())
        fn = int(((preds == 0) & (y_true == 1)).sum())

        fpr = fp / n_neg if n_neg else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0

        if fpr <= fpr_budget and recall > best_recall:
            best_recall = recall
            best_thr = float(thr)

    return best_thr


def main() -> int:
    args = parse_args()

    preprocessing_cfg = PROJECT_ROOT / "configs" / "modeling" / "preprocessing.yaml"
    feature_manifest = PROJECT_ROOT / "artifacts" / "offline" / "preprocessing" / "feature_manifest.json"
    train_path = PROJECT_ROOT / "data" / "gold" / "splits" / "train.parquet"
    valid_path = PROJECT_ROOT / "data" / "gold" / "splits" / "valid.parquet"
    model_path = PROJECT_ROOT / "artifacts" / "offline" / "models" / "gradient_boosting.joblib"
    valid_metrics_path = PROJECT_ROOT / "artifacts" / "offline" / "models" / "valid_metrics.csv"

    payload = yaml.safe_load(preprocessing_cfg.read_text(encoding="utf-8")) or {}
    models_cfg = (payload.get("models") or {}).get("gradient_boosting") or {}
    sampling = payload.get("sampling") or {}

    manifest = json.loads(feature_manifest.read_text(encoding="utf-8"))
    feature_columns = [str(c) for c in manifest.get("feature_columns", [])]
    if not feature_columns:
        raise ValueError("feature_columns missing from feature_manifest")

    cols = [*feature_columns, "label_binary"]

    print(f"Loading train data from {train_path}")
    train_df = _read_parquet_limited(train_path, cols, args.max_train_rows, args.batch_size)
    train_df = _stratified_sample(train_df, "label_binary", args.max_train_rows)
    x_train = train_df[feature_columns]
    y_train = train_df["label_binary"].astype(int).to_numpy()

    print(f"Loading valid data from {valid_path}")
    valid_df = _read_parquet_limited(valid_path, cols, args.max_valid_rows, args.batch_size)
    x_valid = valid_df[feature_columns]
    y_valid = valid_df["label_binary"].astype(int).to_numpy()

    gbt = HistGradientBoostingClassifier(
        max_iter=int(models_cfg.get("n_estimators", 400)),
        max_depth=int(models_cfg.get("max_depth", 8)),
        learning_rate=float(models_cfg.get("learning_rate", 0.05)),
        min_samples_leaf=int(models_cfg.get("min_samples_leaf", 20)),
        random_state=42,
        class_weight="balanced" if str(sampling.get("strategy", "")).strip() == "class_weight" else None,
    )

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", gbt),
    ])

    t_train = time.perf_counter()
    pipe.fit(x_train, y_train)
    train_time_s = time.perf_counter() - t_train

    t_infer = time.perf_counter()
    valid_proba = pipe.predict_proba(x_valid)[:, 1]
    valid_infer_time_s = time.perf_counter() - t_infer

    opt_thr = _best_threshold(y_valid, valid_proba, args.fpr_budget)
    preds_opt = (valid_proba >= opt_thr).astype(int)
    preds_default = (valid_proba >= 0.5).astype(int)

    opt_metrics = _metrics(y_valid, preds_opt, valid_proba)
    default_metrics = _metrics(y_valid, preds_default, valid_proba)

    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_path)
    print(f"Saved model: {model_path}")

    metrics_df = pd.read_csv(valid_metrics_path)
    metrics_df = metrics_df[metrics_df["model"] != "gradient_boosting"]

    row = {
        "model": "gradient_boosting",
        "split": "valid",
        "n": int(len(y_valid)),
        "class_weight": str(sampling.get("strategy", "")) == "class_weight",
        "opt_threshold": float(opt_thr),
        "fpr_budget": float(args.fpr_budget),
        "accuracy": round(opt_metrics["accuracy"], 6),
        "precision": round(opt_metrics["precision"], 6),
        "recall": round(opt_metrics["recall"], 6),
        "f1": round(opt_metrics["f1"], 6),
        "fpr": round(opt_metrics["fpr"], 6),
        "fnr": round(opt_metrics["fnr"], 6),
        "roc_auc": round(opt_metrics["roc_auc"], 6),
        "default_threshold": 0.5,
        "default_recall": round(default_metrics["recall"], 6),
        "default_precision": round(default_metrics["precision"], 6),
        "default_f1": round(default_metrics["f1"], 6),
        "default_fpr": round(default_metrics["fpr"], 6),
        "train_time_s": round(train_time_s, 2),
        "valid_infer_time_s": round(valid_infer_time_s, 2),
        "valid_rows_per_sec": round(len(y_valid) / max(valid_infer_time_s, 1e-9), 1),
    }

    out_df = pd.concat([metrics_df, pd.DataFrame([row])], ignore_index=True)
    out_df.to_csv(valid_metrics_path, index=False)
    print(f"Updated metrics: {valid_metrics_path}")
    print(f"opt_threshold={row['opt_threshold']:.3f} f1={row['f1']:.4f} fpr={row['fpr']:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
