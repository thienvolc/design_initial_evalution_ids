"""Quick ablation: GBT with 4 configs on current train/valid split.

Configs tested:
  1. all features + class_weight=balanced  (current default)
  2. all features + class_weight=None
  3. selected features (top-30 f_classif) + class_weight=balanced
  4. selected features (top-30 f_classif) + class_weight=None
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline

from _common import project_path
from ids_platform.offline.config import load_feature_list
from ids_platform.offline.metrics import compute_binary_metrics

# ── paths ──
TRAIN = project_path("data", "gold", "splits", "train.parquet")
VALID = project_path("data", "gold", "splits", "valid.parquet")
FEATURES = load_feature_list(project_path("configs", "modeling", "feature_registry_full.yaml"))
MAX_ROWS = 4_000_000  # keep ablation fast

# ── load data ──
print(f"Loading train (max {MAX_ROWS:,}) ...")
cols = list(dict.fromkeys([*FEATURES, "label_binary"]))
df_train = pd.read_parquet(TRAIN, columns=cols)
if len(df_train) > MAX_ROWS:
    frac = MAX_ROWS / len(df_train)
    parts = []
    for _, grp in df_train.groupby("label_binary"):
        parts.append(grp.sample(n=max(1, int(len(grp) * frac)), random_state=42))
    df_train = pd.concat(parts, ignore_index=True)

y_train = df_train["label_binary"].astype(int)
x_train_all = df_train[FEATURES].copy()
del df_train

print(f"Loading valid ...")
df_valid = pd.read_parquet(VALID, columns=cols)
y_valid = df_valid["label_binary"].astype(int)
x_valid_all = df_valid[FEATURES].copy()
del df_valid

print(f"Train: {len(y_train):,} rows ({y_train.sum():,} attack)")
print(f"Valid: {len(y_valid):,} rows ({y_valid.sum():,} attack)")

# ── feature selection (for ablation) ──
print("Running feature selection (top-30 f_classif) ...")
imp = SimpleImputer(strategy="median")
x_imp = pd.DataFrame(imp.fit_transform(x_train_all), columns=FEATURES)
sel = SelectKBest(f_classif, k=30)
sel.fit(x_imp, y_train)
selected = [f for f, m in zip(FEATURES, sel.get_support()) if m]
print(f"  Selected: {len(selected)} features")
del x_imp

# ── GBT params ──
gbt_params = dict(max_iter=400, max_depth=8, learning_rate=0.05,
                   min_samples_leaf=20, random_state=42)

# ── configs ──
configs = [
    ("all_feat + balanced",    FEATURES, "balanced"),
    ("all_feat + no_weight",   FEATURES, None),
    ("sel_feat + balanced",    selected, "balanced"),
    ("sel_feat + no_weight",   selected, None),
]

FPR_BUDGETS = [0.01, 0.03, 0.05, 0.10]

results = []
for name, feats, cw in configs:
    print(f"\n{'='*70}")
    print(f"  {name}  ({len(feats)} features, class_weight={cw})")
    print(f"{'='*70}")

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", HistGradientBoostingClassifier(**gbt_params, class_weight=cw)),
    ])

    t0 = time.perf_counter()
    pipe.fit(x_train_all[feats], y_train)
    train_time = time.perf_counter() - t0

    proba = pipe.predict_proba(x_valid_all[feats])[:, 1]
    n_neg = int((y_valid == 0).sum())
    n_pos = int((y_valid == 1).sum())

    # default 0.5
    preds_50 = (proba >= 0.5).astype(int)
    m50, _ = compute_binary_metrics(y_valid, pd.Series(preds_50), pd.Series(proba))
    print(f"  default(0.5): recall={m50['recall']:.4f}  prec={m50['precision']:.4f}  f1={m50['f1']:.4f}  fpr={m50['fpr']:.4f}")

    # FPR budgets
    for budget in FPR_BUDGETS:
        best_r, best_t = 0.0, 0.5
        for thr in np.linspace(0.001, 0.999, 999):
            p = (proba >= thr).astype(int)
            tp = int(((p == 1) & (y_valid.values == 1)).sum())
            fp = int(((p == 1) & (y_valid.values == 0)).sum())
            fn = int(((p == 0) & (y_valid.values == 1)).sum())
            fpr = fp / n_neg if n_neg else 0
            rec = tp / (tp + fn) if (tp + fn) else 0
            if fpr <= budget and rec > best_r:
                best_r, best_t = rec, thr

        preds_b = (proba >= best_t).astype(int)
        mb, _ = compute_binary_metrics(y_valid, pd.Series(preds_b), pd.Series(proba))
        print(f"  FPR<={budget*100:.0f}%: thr={best_t:.3f}  recall={mb['recall']:.4f}  prec={mb['precision']:.4f}  f1={mb['f1']:.4f}")

        results.append({
            "config": name, "n_features": len(feats), "class_weight": str(cw),
            "fpr_budget": budget, "threshold": best_t,
            "recall": mb["recall"], "precision": mb["precision"], "f1": mb["f1"],
            "fpr": mb["fpr"], "train_time_s": round(train_time, 1),
        })

df_results = pd.DataFrame(results)
out = project_path("artifacts", "offline", "evaluation", "ablation_results.csv")
df_results.to_csv(out, index=False)
print(f"\nResults saved to {out}")
print("\n" + df_results.to_string(index=False))
