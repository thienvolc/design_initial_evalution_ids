"""ML evaluation metrics for binary IDS classification."""

from __future__ import annotations

from numpy import ndarray
from pandas import Series
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_recall_fscore_support,
    roc_auc_score,
)


def compute_binary_metrics(
    y_true: Series | ndarray,
    y_pred: Series | ndarray,
    y_score: Series | ndarray | None = None,
) -> tuple[dict[str, float], list[list[int]]]:
    """Return (metrics_dict, confusion_matrix_2x2)."""

    prec, rec, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="binary", zero_division=0,
    )
    acc = accuracy_score(y_true, y_pred)

    cm = confusion_matrix(y_true, y_pred, labels=[0, 1]).tolist()
    tn, fp = cm[0]
    fn, tp = cm[1]

    fpr = fp / (fp + tn) if (fp + tn) else 0.0
    fnr = fn / (fn + tp) if (fn + tp) else 0.0

    result: dict[str, float] = {
        "accuracy": round(float(acc), 6),
        "precision": round(float(prec), 6),
        "recall": round(float(rec), 6),
        "f1": round(float(f1), 6),
        "fpr": round(float(fpr), 6),
        "fnr": round(float(fnr), 6),
    }

    if y_score is not None:
        try:
            result["roc_auc"] = round(float(roc_auc_score(y_true, y_score)), 6)
        except ValueError:
            result["roc_auc"] = 0.0

    return result, cm


def compute_per_attack_recall(
    y_true: Series,
    y_pred: Series,
    labels: Series,
    *,
    threshold: float,
    model_name: str,
) -> list[dict]:
    """Compute recall rows for positive-class samples grouped by attack label."""

    rows: list[dict] = []
    attack_mask = y_true.eq(1)
    for attack_type in sorted(labels[attack_mask].unique()):
        type_mask = labels.eq(attack_type) & attack_mask
        sample_count = int(len(labels[type_mask]))
        detected_count = int(y_pred[type_mask].sum())
        recall = detected_count / sample_count if sample_count else 0
        rows.append(
            {
                "model": model_name,
                "attack_type": attack_type,
                "n_samples": sample_count,
                "detected": detected_count,
                "recall": round(recall, 6),
                "threshold": threshold,
            }
        )
    return rows
