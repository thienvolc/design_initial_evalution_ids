"""Diagnostic checks for split difficulty, class balance, and feature quality."""

from __future__ import annotations

import warnings
from pathlib import Path

import pandas as pd
from sklearn.feature_selection import f_classif
from sklearn.impute import SimpleImputer

from _common import print_section, project_path


warnings.filterwarnings("ignore")

TRAIN_SPLIT_PATH = project_path("data", "gold", "splits", "train.parquet")
VALID_SPLIT_PATH = project_path("data", "gold", "splits", "valid.parquet")
TEST_SPLIT_PATH = project_path("data", "gold", "splits", "test.parquet")
MAX_TRAIN_ROWS = 2_000_000
METADATA_COLUMNS = {"timestamp", "label", "label_norm", "label_binary", "source_file"}
TARGET_ATTACK_LABEL = "Infilteration"


def print_attack_ratio_by_split() -> None:
    print_section("H1: Attack ratio at each stage", width=65)

    for split_name, split_path in [("train (raw)", TRAIN_SPLIT_PATH), ("valid", VALID_SPLIT_PATH), ("test", TEST_SPLIT_PATH)]:
        split_frame = pd.read_parquet(split_path, columns=["label_binary", "label"])
        total_rows = len(split_frame)
        attack_rows = int(split_frame["label_binary"].sum())
        print(f"\n  {split_name}: {total_rows:>9} rows | attack={attack_rows:>7} ({100 * attack_rows / total_rows:.1f}%)")
        for attack_label, count in split_frame[split_frame["label_binary"] == 1]["label"].value_counts().items():
            print(f"    {attack_label:<40} {count:>7} ({100 * count / attack_rows:.1f}%)")


def print_train_subsample_effect() -> None:
    train_labels = pd.read_parquet(TRAIN_SPLIT_PATH, columns=["label_binary", "label"])
    total_rows = len(train_labels)
    sampled_fraction = MAX_TRAIN_ROWS / total_rows

    print(f"\n  train (after {MAX_TRAIN_ROWS / 1e6:.0f}M proportional subsample, frac={sampled_fraction:.3f}):")
    for label_value, count in train_labels["label_binary"].value_counts().items():
        kept_count = int(count * sampled_fraction)
        print(f"    label_binary={label_value}: {count:>9} -> ~{kept_count:>7} ({100 * kept_count / MAX_TRAIN_ROWS:.1f}%)")

    infiltration_rows = int(train_labels[train_labels["label"] == TARGET_ATTACK_LABEL]["label"].count())
    kept_infiltration_rows = int(infiltration_rows * sampled_fraction)
    print(f"    Infiltration specifically: {infiltration_rows} -> ~{kept_infiltration_rows} rows in 2M subsample")


def rank_infiltration_features() -> list[str]:
    print_section("H2: Top features for Infiltration detection specifically", width=65)

    train_frame = pd.read_parquet(TRAIN_SPLIT_PATH, columns=None)
    feature_columns = [column for column in train_frame.columns if column not in METADATA_COLUMNS]

    infiltration_mask = (train_frame["label"] == TARGET_ATTACK_LABEL) | (train_frame["label_binary"] == 0)
    sampled_frame = train_frame.loc[infiltration_mask, feature_columns + ["label_binary"]].sample(
        min(300_000, int(infiltration_mask.sum())),
        random_state=42,
    )

    imputer = SimpleImputer(strategy="median")
    imputed_features = pd.DataFrame(imputer.fit_transform(sampled_frame[feature_columns]), columns=feature_columns)
    imputed_features = imputed_features.loc[:, imputed_features.std() > 0]

    scores, _ = f_classif(imputed_features, sampled_frame["label_binary"].astype(int))
    ranked_features = sorted(zip(imputed_features.columns, scores), key=lambda item: item[1], reverse=True)

    print("\n  Top-20 features for Infiltration vs Benign:")
    for feature_name, score in ranked_features[:20]:
        print(f"    {feature_name:<40} F={score:.1f}")

    print("\n  Bottom-5 features:")
    for feature_name, score in ranked_features[-5:]:
        print(f"    {feature_name:<40} F={score:.1f}")

    return [feature_name for feature_name, _ in ranked_features[:5]]


def print_attack_distribution_shift(top_features: list[str]) -> None:
    print_section("H3: Feature distribution shift train(attack) vs valid(attack)", width=65)

    valid_frame = pd.read_parquet(VALID_SPLIT_PATH, columns=top_features + ["label_binary"])
    train_attack_frame = pd.read_parquet(TRAIN_SPLIT_PATH, columns=top_features + ["label_binary"])
    train_attack_frame = train_attack_frame[train_attack_frame["label_binary"] == 1]
    valid_attack_frame = valid_frame[valid_frame["label_binary"] == 1]

    print("\n  Comparing attack traffic distributions (train vs valid):")
    print(f"  {'Feature':<40} {'Train mean':>12} {'Valid mean':>12} {'ratio':>8}")
    for feature_name in top_features:
        train_median = train_attack_frame[feature_name].median()
        valid_median = valid_attack_frame[feature_name].median()
        median_ratio = valid_median / train_median if train_median != 0 else float("inf")
        print(f"  {feature_name:<40} {train_median:>12.2f} {valid_median:>12.2f} {median_ratio:>8.2f}x")


def main() -> None:
    print_attack_ratio_by_split()
    print_train_subsample_effect()
    top_features = rank_infiltration_features()
    print_attack_distribution_shift(top_features)


if __name__ == "__main__":
    main()
