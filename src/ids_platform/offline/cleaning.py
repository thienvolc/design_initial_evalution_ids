"""Data cleaning utilities: timestamps, labels, numeric sanitisation."""

from __future__ import annotations

import re

import numpy as np
import pandas as pd

# Rows with timestamps before this date are treated as corrupt.
TIMESTAMP_FLOOR = pd.Timestamp("2018-01-01")


# ── timestamps ──────────────────────────────────────────────────────────


def parse_timestamps(series: pd.Series) -> pd.Series:
    """Parse the CIC-IDS2018 Timestamp column (dd/mm/yyyy HH:MM:SS)."""

    raw = series.astype(str).str.strip()
    raw = raw.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "NaT": pd.NA})

    parsed = pd.to_datetime(raw, format="%d/%m/%Y %H:%M:%S", errors="coerce")

    mask = parsed.isna() & raw.notna()
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(
            raw[mask], format="%Y-%m-%d %H:%M:%S", errors="coerce",
        )

    mask = parsed.isna() & raw.notna()
    if mask.any():
        parsed.loc[mask] = pd.to_datetime(raw[mask], format="mixed", errors="coerce")

    return parsed


# ── labels ──────────────────────────────────────────────────────────────


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "unknown"


def normalize_labels(
    raw_labels: pd.Series,
    mapping: dict[str, str],
) -> tuple[pd.Series, int, list[str]]:
    """Map raw label strings → normalised slugs using *mapping*.

    Returns (label_norm_series, n_unmapped, list_of_unmapped_values).
    """
    clean = raw_labels.astype("string").str.strip()
    clean = clean.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})

    mapped = clean.map(mapping)
    fallback = clean.fillna("unknown").apply(slugify)
    label_norm = mapped.fillna(fallback)

    unmapped_mask = clean.notna() & mapped.isna()
    n_unmapped = int(unmapped_mask.sum())
    unmapped_values = sorted(clean[unmapped_mask].dropna().unique().tolist())

    return label_norm, n_unmapped, unmapped_values


# ── numeric sanitization ────────────────────────────────────────────────


def replace_inf(df: pd.DataFrame, columns: list[str]) -> None:
    """Coerce columns to numeric and replace ±Inf with NaN *in-place*."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    present = [c for c in columns if c in df.columns]
    if present:
        df[present] = df[present].replace([np.inf, -np.inf], np.nan)
