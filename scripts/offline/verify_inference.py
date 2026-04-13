"""Verify the saved best model on valid and test splits using the stored threshold."""

from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd

from _common import project_path
from ids_platform.offline.config import load_json
from ids_platform.offline.metrics import compute_binary_metrics


FEATURE_MANIFEST_PATH = project_path("artifacts", "preprocessing", "feature_manifest.json")
BEST_MODEL_METADATA_PATH = project_path("artifacts", "models", "best_model.json")
VALID_SPLIT_PATH = project_path("data", "gold", "splits", "valid.parquet")
TEST_SPLIT_PATH = project_path("data", "gold", "splits", "test.parquet")


def evaluate_split(split_path: Path, feature_columns: list[str], model, threshold: float) -> tuple[pd.DataFrame, pd.Series, dict[str, float]]:
    split_frame = pd.read_parquet(split_path)
    features = split_frame[feature_columns]
    labels = split_frame["label_binary"].astype(int)

    scores = model.predict_proba(features)[:, 1]
    predicted_labels = pd.Series((scores >= threshold).astype(int), index=labels.index)
    score_series = pd.Series(scores, index=labels.index)
    metrics, _ = compute_binary_metrics(labels, predicted_labels, score_series)
    return split_frame, score_series, metrics


def print_split_metrics(name: str, dataframe: pd.DataFrame, metrics: dict[str, float]) -> None:
    label_series = dataframe["label_binary"].astype(int)
    print(f"\n=== {name} ===")
    print(f"  n={len(label_series)}  attack={int(label_series.sum())} ({100 * label_series.mean():.1f}%)")
    print(
        f"  f1={metrics['f1']:.4f}  recall={metrics['recall']:.4f}  "
        f"prec={metrics['precision']:.4f}  fpr={metrics['fpr']:.4f}"
    )


def print_test_attack_breakdown(test_frame: pd.DataFrame) -> None:
    if "label" not in test_frame.columns:
        return
    for attack_type, count in test_frame.loc[test_frame["label_binary"] == 1, "label"].value_counts().items():
        print(f"    {attack_type:<40} {count}")


def print_score_distribution(score_series: pd.Series, labels: pd.Series) -> None:
    print("\n=== Score distributions (test) ===")
    for class_value, class_name in [(0, "benign"), (1, "attack")]:
        class_scores = score_series[labels == class_value]
        if len(class_scores) == 0:
            continue
        print(
            f"  {class_name:>7}: mean={class_scores.mean():.4f}  median={class_scores.median():.4f}  "
            f"p5={class_scores.quantile(0.05):.4f}  p95={class_scores.quantile(0.95):.4f}"
        )


def print_diagnosis(valid_metrics: dict[str, float], test_metrics: dict[str, float]) -> None:
    print("\n=== Diagnosis ===")
    if valid_metrics["f1"] > 0.95 and test_metrics["f1"] < 0.50:
        print("  ✓ Pipeline correct. Valid=high, Test=low → test contains unseen attack types.")
        print("  → This is concept-drift / zero-day finding, not a bug.")
    elif valid_metrics["f1"] < 0.50:
        print("  ✗ Valid also low → pipeline/model issue, not just test data.")
    else:
        print("  ? Inconclusive — check score distributions above.")


def main() -> None:
    feature_manifest = load_json(FEATURE_MANIFEST_PATH)
    best_model_metadata = load_json(BEST_MODEL_METADATA_PATH)

    feature_columns = list(feature_manifest["feature_columns"])
    model = joblib.load(Path(best_model_metadata["model_path"]))
    threshold = float(best_model_metadata.get("opt_threshold", 0.5))

    print(f"Model: {best_model_metadata['best_model']}  threshold: {threshold}")

    valid_frame, _, valid_metrics = evaluate_split(VALID_SPLIT_PATH, feature_columns, model, threshold)
    print_split_metrics("VALID (should match Phase03)", valid_frame, valid_metrics)

    test_frame, test_scores, test_metrics = evaluate_split(TEST_SPLIT_PATH, feature_columns, model, threshold)
    print_split_metrics("TEST", test_frame, test_metrics)
    print_test_attack_breakdown(test_frame)
    print_score_distribution(test_scores, test_frame["label_binary"].astype(int))
    print_diagnosis(valid_metrics, test_metrics)


if __name__ == "__main__":
    main()
