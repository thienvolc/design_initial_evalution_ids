"""Label distribution by day and attack windows for the silver dataset."""

from __future__ import annotations

from datetime import date

import pandas as pd

from _common import print_section, project_path
from ids_platform.offline.cleaning import parse_timestamps


SILVER_DATASET_PATH = project_path("data", "silver", "ids2018_cleaned.parquet")
TRAIN_DAYS = {
    date(2018, 2, 14),
    date(2018, 2, 15),
    date(2018, 2, 16),
    date(2018, 2, 20),
    date(2018, 2, 21),
    date(2018, 2, 28),
    date(2018, 3, 2),
}
VALID_DAYS = {date(2018, 2, 22), date(2018, 2, 23)}
TEST_DAYS = {date(2018, 3, 1)}


def load_silver_dataframe() -> pd.DataFrame:
    print("Reading silver parquet (timestamp + label columns only) ...")
    dataframe = pd.read_parquet(SILVER_DATASET_PATH, columns=["timestamp", "label", "label_binary"])
    dataframe["timestamp"] = parse_timestamps(dataframe["timestamp"])
    dataframe = dataframe[dataframe["timestamp"].notna()].copy()
    dataframe["day"] = dataframe["timestamp"].dt.date
    return dataframe


def print_day_summary(dataframe: pd.DataFrame) -> None:
    print_section("Per-day summary", width=90)
    print(f"{'Day':<14} {'Total':>9} {'Benign':>9} {'Attack':>9} {'Atk%':>6}  Attack types")
    print("-" * 90)

    for day in sorted(dataframe["day"].unique()):
        day_frame = dataframe[dataframe["day"] == day]
        total_rows = len(day_frame)
        benign_rows = int((day_frame["label_binary"] == 0).sum())
        attack_rows = int((day_frame["label_binary"] == 1).sum())
        attack_types = sorted(day_frame.loc[day_frame["label_binary"] == 1, "label"].dropna().unique())
        print(
            f"{day}   {total_rows:>9,} {benign_rows:>9,} {attack_rows:>9,} "
            f"{100 * attack_rows / total_rows:>5.1f}%  {', '.join(attack_types) or '(benign only)'}"
        )


def print_day_attack_breakdown(dataframe: pd.DataFrame) -> None:
    print_section("Per-day attack type breakdown", width=90)

    for day in sorted(dataframe["day"].unique()):
        day_frame = dataframe[dataframe["day"] == day]
        attack_rows = int((day_frame["label_binary"] == 1).sum())
        if attack_rows == 0:
            continue

        print(f"\n  {day}  ({len(day_frame):,} rows, {attack_rows:,} attack)")
        attack_frame = day_frame.loc[day_frame["label_binary"] == 1]
        for attack_label, count in attack_frame["label"].value_counts().items():
            label_frame = attack_frame.loc[attack_frame["label"] == attack_label, "timestamp"]
            print(
                f"    {attack_label:<40} {count:>8,}  "
                f"[{label_frame.min().strftime('%H:%M')} - {label_frame.max().strftime('%H:%M')}]"
            )


def print_global_attack_summary(dataframe: pd.DataFrame) -> None:
    print_section("Global attack family summary", width=90)

    attack_frame = dataframe[dataframe["label_binary"] == 1]
    total_attack_rows = len(attack_frame)
    for attack_label, count in attack_frame["label"].value_counts().items():
        days_present = sorted(attack_frame.loc[attack_frame["label"] == attack_label, "day"].unique())
        print(
            f"  {attack_label:<40} {count:>9,} ({100 * count / total_attack_rows:>5.1f}%)  "
            f"days: {[str(day) for day in days_present]}"
        )

    total_rows = len(dataframe)
    print(f"\n  Total attack: {total_attack_rows:,}")
    print(f"  Total benign: {total_rows - total_attack_rows:,}")
    print(f"  Total:        {total_rows:,}")


def print_split_summary(dataframe: pd.DataFrame) -> None:
    print_section("Proposed seen-attack baseline split", width=90)

    for split_name, day_set in [("TRAIN", TRAIN_DAYS), ("VALID", VALID_DAYS), ("TEST", TEST_DAYS)]:
        split_frame = dataframe[dataframe["day"].isin(day_set)]
        total_rows = len(split_frame)
        attack_rows = int(split_frame["label_binary"].sum())
        benign_rows = total_rows - attack_rows
        attack_types = sorted(split_frame.loc[split_frame["label_binary"] == 1, "label"].dropna().unique())
        print(
            f"\n  {split_name}: {total_rows:,} rows | benign={benign_rows:,} ({100 * benign_rows / total_rows:.1f}%) | "
            f"attack={attack_rows:,} ({100 * attack_rows / total_rows:.1f}%)"
        )
        print(f"    Days: {sorted(str(day) for day in day_set)}")
        print(f"    Attack types: {attack_types}")
        if attack_rows > 0:
            for attack_label, count in split_frame.loc[split_frame["label_binary"] == 1, "label"].value_counts().items():
                print(f"      {attack_label:<40} {count:>8,}")


def main() -> None:
    silver_dataframe = load_silver_dataframe()
    print(f"Total rows: {len(silver_dataframe):,}\n")
    print_day_summary(silver_dataframe)
    print()
    print_day_attack_breakdown(silver_dataframe)
    print()
    print_global_attack_summary(silver_dataframe)
    print_split_summary(silver_dataframe)


if __name__ == "__main__":
    main()
