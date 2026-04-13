"""Feature count sweep + per-attack recall analysis.

Tests: top-10, top-17, top-25, top-40, all-78
Metric: recall @ FPR constraint (1%, 3%, 5%, 10%)
Also logs per-attack-type recall (especially Infiltration).
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

# ── config ──
TRAIN = project_path("data", "gold", "splits", "train.parquet")
VALID = project_path("data", "gold", "splits", "valid.parquet")
TEST = project_path("data", "gold", "splits", "test.parquet")
FEATURES = load_feature_list(project_path("configs", "modeling", "feature_registry_full.yaml"))
MAX_ROWS = 6_000_000
K_VALUES = [10, 17, 25, 40, len(FEATURES)]  # last = all
FPR_BUDGETS = [0.01, 0.03, 0.05, 0.10]
GBT = dict(max_iter=400, max_depth=8, learning_rate=0.05,
           min_samples_leaf=20, random_state=42, class_weight="balanced")

# ── load ──
print("Loading data ...")
cols_all = list(dict.fromkeys([*FEATURES, "label_binary", "label"]))

df_train = pd.read_parquet(TRAIN, columns=cols_all)
if len(df_train) > MAX_ROWS:
    frac = MAX_ROWS / len(df_train)
    parts = [grp.sample(n=max(1, int(len(grp)*frac)), random_state=42)
             for _, grp in df_train.groupby("label_binary")]
    df_train = pd.concat(parts, ignore_index=True)
y_train = df_train["label_binary"].astype(int)
x_train = df_train[FEATURES].copy()
del df_train

df_valid = pd.read_parquet(VALID, columns=cols_all)
y_valid = df_valid["label_binary"].astype(int)
x_valid = df_valid[FEATURES].copy()
labels_valid = df_valid["label"].copy() if "label" in df_valid.columns else None
del df_valid

df_test = pd.read_parquet(TEST, columns=cols_all)
y_test = df_test["label_binary"].astype(int)
x_test = df_test[FEATURES].copy()
labels_test = df_test["label"].copy() if "label" in df_test.columns else None
del df_test

print(f"Train: {len(y_train):,} ({y_train.sum():,} atk)  "
      f"Valid: {len(y_valid):,} ({y_valid.sum():,} atk)  "
      f"Test: {len(y_test):,} ({y_test.sum():,} atk)")

# ── feature ranking (once) ──
print("\nRanking features (f_classif) ...")
imp = SimpleImputer(strategy="median")
x_imp = pd.DataFrame(imp.fit_transform(x_train), columns=FEATURES)
sel_all = SelectKBest(f_classif, k="all")
sel_all.fit(x_imp, y_train)
scores = sel_all.scores_
ranking = sorted(zip(FEATURES, scores), key=lambda t: t[1], reverse=True)
del x_imp

print("\nFeature ranking (top 40):")
for i, (feat, sc) in enumerate(ranking[:40], 1):
    print(f"  {i:>2}. {feat:<40} F={sc:>12.2f}")

# ── helper: best threshold at FPR budget ──
def _best_at_fpr(proba, y_true, budget):
    n_neg = int((y_true == 0).sum())
    best_r, best_t = 0.0, 0.5
    for thr in np.linspace(0.001, 0.999, 999):
        p = (proba >= thr).astype(int)
        tp = int(((p == 1) & (y_true.values == 1)).sum())
        fp = int(((p == 1) & (y_true.values == 0)).sum())
        fn = int(((p == 0) & (y_true.values == 1)).sum())
        fpr = fp / n_neg if n_neg else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        if fpr <= budget and rec > best_r:
            best_r, best_t = rec, thr
    return best_t, best_r

# ── helper: per-attack recall ──
def _per_attack_recall(proba, thr, y_true, labels):
    if labels is None:
        return {}
    preds = (proba >= thr).astype(int)
    result = {}
    for atype in labels[y_true == 1].unique():
        mask = (labels == atype) & (y_true == 1)
        n = int(mask.sum())
        if n == 0:
            continue
        detected = int(preds[mask].sum())
        result[atype] = {"n": n, "detected": detected, "recall": round(detected / n, 4)}
    return result

# ── sweep ──
sweep_results = []
importance_tracker = {}

for k in K_VALUES:
    top_k_feats = [f for f, _ in ranking[:k]]
    label = f"top-{k}" if k < len(FEATURES) else f"all-{len(FEATURES)}"

    print(f"\n{'='*80}")
    print(f"  {label} features")
    print(f"{'='*80}")

    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", HistGradientBoostingClassifier(**GBT)),
    ])

    t0 = time.perf_counter()
    pipe.fit(x_train[top_k_feats], y_train)
    train_s = time.perf_counter() - t0
    print(f"  trained in {train_s:.1f}s")

    # feature importance via permutation (works with any model)
    from sklearn.inspection import permutation_importance
    # subsample valid for speed (permutation is O(n_features * n_samples))
    n_pi = min(50_000, len(y_valid))
    idx_pi = np.random.RandomState(42).choice(len(y_valid), n_pi, replace=False)
    pi = permutation_importance(
        pipe, x_valid[top_k_feats].iloc[idx_pi], y_valid.iloc[idx_pi],
        n_repeats=5, random_state=42, scoring="recall", n_jobs=-1,
    )
    fi = sorted(zip(top_k_feats, pi.importances_mean),
                key=lambda x: x[1], reverse=True)
    importance_tracker[label] = fi
    print(f"  top-5 perm-importance: {', '.join(f'{f}={v:.4f}' for f,v in fi[:5])}")

    # valid scores
    proba_v = pipe.predict_proba(x_valid[top_k_feats])[:, 1]
    # test scores
    proba_t = pipe.predict_proba(x_test[top_k_feats])[:, 1]

    print(f"\n  {'FPR budget':>10} | {'thr':>6} | {'V-recall':>8} {'V-prec':>7} {'V-f1':>6} | {'T-recall':>8} {'T-prec':>7} {'T-f1':>6}")
    print(f"  {'-'*75}")

    # default 0.5
    pv50 = (proba_v >= 0.5).astype(int)
    pt50 = (proba_t >= 0.5).astype(int)
    mv50, _ = compute_binary_metrics(y_valid, pd.Series(pv50), pd.Series(proba_v))
    mt50, _ = compute_binary_metrics(y_test, pd.Series(pt50), pd.Series(proba_t))
    print(f"  {'default':>10} | {0.5:>6.3f} | {mv50['recall']:>8.4f} {mv50['precision']:>7.4f} {mv50['f1']:>6.4f} | {mt50['recall']:>8.4f} {mt50['precision']:>7.4f} {mt50['f1']:>6.4f}")

    for budget in FPR_BUDGETS:
        thr, _ = _best_at_fpr(proba_v, y_valid, budget)
        # valid metrics
        pv = (proba_v >= thr).astype(int)
        mv, _ = compute_binary_metrics(y_valid, pd.Series(pv), pd.Series(proba_v))
        # test metrics
        pt = (proba_t >= thr).astype(int)
        mt, _ = compute_binary_metrics(y_test, pd.Series(pt), pd.Series(proba_t))

        marker = ""
        print(f"  FPR<={budget*100:>4.0f}% | {thr:>6.3f} | {mv['recall']:>8.4f} {mv['precision']:>7.4f} {mv['f1']:>6.4f} | {mt['recall']:>8.4f} {mt['precision']:>7.4f} {mt['f1']:>6.4f}")

        # per-attack on test
        pa = _per_attack_recall(proba_t, thr, y_test, labels_test)
        for atype, info in sorted(pa.items(), key=lambda x: -x[1]["n"]):
            print(f"    {'':>10}   {atype:<35} recall={info['recall']:.4f} ({info['detected']}/{info['n']})")

        sweep_results.append({
            "k": k, "label": label, "fpr_budget": budget, "threshold": thr,
            "valid_recall": mv["recall"], "valid_prec": mv["precision"], "valid_f1": mv["f1"],
            "test_recall": mt["recall"], "test_prec": mt["precision"], "test_f1": mt["f1"],
            "test_fpr": mt["fpr"], "train_time_s": round(train_s, 1),
        })

# ── save ──
df_out = pd.DataFrame(sweep_results)
out_path = project_path("artifacts", "offline", "evaluation", "feature_sweep_results.csv")
df_out.to_csv(out_path, index=False)
print(f"\nResults saved to {out_path}")

# ── stability check ──
print(f"\n{'='*80}")
print("Feature importance stability across k-values")
print(f"{'='*80}")
all_fi = {}
for label, fi in importance_tracker.items():
    for feat, imp in fi:
        all_fi.setdefault(feat, {})[label] = round(imp, 4)

stable = sorted(all_fi.items(),
                key=lambda x: sum(x[1].values()) / len(x[1].values()),
                reverse=True)
print(f"\n  {'Feature':<40} ", end="")
for label in importance_tracker:
    print(f" {label:>10}", end="")
print()
for feat, vals in stable[:25]:
    print(f"  {feat:<40} ", end="")
    for label in importance_tracker:
        v = vals.get(label, None)
        print(f" {v:>10.4f}" if v is not None else f" {'---':>10}", end="")
    print()
