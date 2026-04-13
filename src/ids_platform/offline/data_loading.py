"""Phase 01 – Ingest raw CSE-CIC-IDS2018 CSVs → Silver Parquet.

Memory-safe: writes each CSV to a temp parquet immediately, then merges
with pyarrow (batch-streaming, no full-dataset concat in pandas).
Feature selection deferred to Phase 03.
"""

from __future__ import annotations

import gc
import shutil
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pyarrow.parquet as pq
from pandas import DataFrame

from ids_platform.offline.cleaning import (
    TIMESTAMP_FLOOR,
    normalize_labels,
    parse_timestamps,
    replace_inf,
)
from ids_platform.offline.config import load_label_mapping
from ids_platform.offline.log import get_logger
from ids_platform.offline.paths import Paths


# ── helpers ─────────────────────────────────────────────────────────────


_META_LOWER = {"timestamp", "label", "label_norm", "label_binary", "source_file"}


def _read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "latin1"):
        try:
            return pd.read_csv(path, low_memory=False, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode {path.name}")


def _clean_one(
    csv_path: Path,
    label_mapping: dict[str, str],
) -> tuple[DataFrame, dict[str, int]]:
    """Return (cleaned_df, drop_stats)."""

    df = _read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]

    # locate Timestamp & Label (case-insensitive)
    ts_col = next((c for c in df.columns if c.lower() == "timestamp"), None)
    lbl_col = next((c for c in df.columns if c.lower() == "label"), None)
    if ts_col is None or lbl_col is None:
        raise ValueError(f"Missing Timestamp/Label in {csv_path.name}")

    df.rename(columns={ts_col: "timestamp", lbl_col: "label"}, inplace=True)
    n_raw = len(df)

    # ── timestamps ──
    df["timestamp"] = parse_timestamps(df["timestamp"])
    bad_ts = df["timestamp"].isna() | (df["timestamp"] < TIMESTAMP_FLOOR)
    n_bad_ts = int(bad_ts.sum())
    df = df[~bad_ts].copy()

    # ── labels ──
    df["label"] = df["label"].astype("string").str.strip()
    null_label = df["label"].isna() | df["label"].isin(["", "nan", "None", "<NA>"])
    n_null_label = int(null_label.sum())
    df = df[~null_label].copy()

    df["label_norm"], n_unmapped, _ = normalize_labels(df["label"], label_mapping)
    df["label_binary"] = df["label_norm"].ne("benign").astype("int8")

    # ── numeric cleaning ──
    feature_cols = [c for c in df.columns if c.lower() not in _META_LOWER]
    replace_inf(df, feature_cols)

    # ── duplicate check ──
    n_before_dedup = len(df)
    df.drop_duplicates(inplace=True)
    n_dupes = n_before_dedup - len(df)

    df["source_file"] = csv_path.name

    # ── standardise column order (consistent schema across CSVs) ──
    meta_first = ["timestamp", "label", "label_norm", "label_binary"]
    other = sorted(c for c in df.columns if c not in [*meta_first, "source_file"])
    df = df[[*meta_first, *other, "source_file"]]

    stats = {
        "rows_raw": n_raw,
        "rows_kept": len(df),
        "dropped_timestamp": n_bad_ts,
        "dropped_null_label": n_null_label,
        "dropped_duplicates": n_dupes,
        "unmapped_labels": n_unmapped,
    }
    return df, stats


# ── public API ──────────────────────────────────────────────────────────


def run(paths: Paths) -> None:
    log = get_logger("phase01", paths.log_dir / "phase01_ingest.log")
    paths.ensure_dirs()

    label_mapping = load_label_mapping(paths.label_mapping_path)

    csv_files = sorted(paths.raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {paths.raw_dir}")

    log.info("START  files=%d", len(csv_files))

    # ── write one temp parquet per CSV (free memory after each) ──
    temp_dir = paths.silver_path.parent / "_temp_ingest"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True)

    total_stats: dict[str, int] = {}
    total_rows = 0

    for csv_path in csv_files:
        df, stats = _clean_one(csv_path, label_mapping)

        for k, v in stats.items():
            total_stats[k] = total_stats.get(k, 0) + v

        if not df.empty:
            temp_path = temp_dir / f"{csv_path.stem}.parquet"
            df.to_parquet(temp_path, index=False, engine="pyarrow")
            total_rows += len(df)

        log.info(
            "  %-55s  rows_kept=%7d  dropped_ts=%d  dropped_lbl=%d  dupes=%d",
            csv_path.name,
            stats["rows_kept"],
            stats["dropped_timestamp"],
            stats["dropped_null_label"],
            stats["dropped_duplicates"],
        )

        del df
        gc.collect()

    if total_rows == 0:
        shutil.rmtree(temp_dir)
        raise RuntimeError("All CSVs produced zero rows after cleaning")

    # ── merge temp parquets (pyarrow.dataset auto-unifies schemas) ──
    temp_files = sorted(temp_dir.glob("*.parquet"))
    log.info("  merging %d temp parquets → silver ...", len(temp_files))

    import pyarrow.dataset as ds

    dataset = ds.dataset(temp_dir, format="parquet")

    dataset_scanner = cast(Any, dataset).scanner(batch_size=500_000)
    with pq.ParquetWriter(paths.silver_path, dataset.schema) as writer:
        for batch in dataset_scanner.to_batches():
            writer.write_batch(batch)
        del batch
    del dataset_scanner
    del dataset
    gc.collect()

    # ── cleanup ──
    shutil.rmtree(temp_dir)

    log.info("DONE   silver=%s  total_rows=%d", paths.silver_path.name, total_rows)
    for k, v in total_stats.items():
        log.info("  total %-25s = %d", k, v)
