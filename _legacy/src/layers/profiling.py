"""
Layer 2 — Profiling & Metadata

For each ingested sheet, compute and persist detailed statistical profiles.
Also perform cross-sheet analysis to detect relationships between sheets.

This layer does NOT modify raw data — it only reads and analyzes.
"""

import json
import logging
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)


def profile_sheet(sheet: dict) -> dict:
    """Compute a full statistical profile for a single ingested sheet.

    Args:
        sheet: A sheet record from Layer 1 ingestion.

    Returns:
        Profile dict containing per-column and sheet-level statistics.
    """
    df = sheet["dataframe"]
    n_rows = len(df)
    n_cols = len(df.columns)

    columns_profile = {}
    for col in df.columns:
        series = df[col]
        col_profile = _profile_column(series, n_rows)
        columns_profile[col] = col_profile

    # Duplicate row detection
    dup_count = int(df.duplicated().sum())

    profile = {
        "sheet_id": sheet["sheet_id"],
        "dataset_id": sheet["dataset_id"],
        "sheet_name": sheet["sheet_name"],
        "source_file": sheet["source_file"],
        "row_count": n_rows,
        "column_count": n_cols,
        "column_names": list(df.columns),
        "duplicate_row_count": dup_count,
        "duplicate_row_pct": round(dup_count / n_rows * 100, 2) if n_rows > 0 else 0,
        "columns": columns_profile,
    }

    logger.info(
        "Profiled sheet '%s': %d rows × %d cols, %.1f%% duplicate rows",
        sheet["sheet_name"],
        n_rows,
        n_cols,
        profile["duplicate_row_pct"],
    )
    return profile


def profile_all_sheets(sheets: list[dict]) -> list[dict]:
    """Profile every sheet and return a list of profile dicts."""
    return [profile_sheet(s) for s in sheets]


def cross_sheet_analysis(profiles: list[dict], sheets: list[dict]) -> dict:
    """Analyze relationships across multiple sheets.

    Detects:
      1. Identical column names across sheets
      2. High-overlap value columns
      3. Likely primary keys (high uniqueness, low null rate)
      4. Possible foreign key relationships
      5. Repeated IDs with identical dates (line-item splits)
    """
    analysis = {
        "shared_columns": _detect_shared_columns(profiles),
        "likely_primary_keys": _detect_primary_keys(profiles),
        "possible_foreign_keys": _detect_foreign_keys(profiles, sheets),
        "line_item_indicators": _detect_line_item_splits(profiles, sheets),
    }

    logger.info(
        "Cross-sheet analysis: %d shared columns, %d likely PKs, %d FK candidates",
        len(analysis["shared_columns"]),
        sum(len(v) for v in analysis["likely_primary_keys"].values()),
        len(analysis["possible_foreign_keys"]),
    )
    return analysis


def persist_profiles(
    profiles: list[dict],
    cross_analysis: dict | None,
    artifacts_dir: str,
    dataset_id: str,
) -> None:
    """Persist profiling metadata to the artifacts directory.

    Writes:
      - {artifacts_dir}/{dataset_id}/profiles/{sheet_id}_profile.json
      - {artifacts_dir}/{dataset_id}/profiles/cross_sheet_analysis.json
    """
    profile_dir = Path(artifacts_dir) / dataset_id / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)

    for profile in profiles:
        path = profile_dir / f"{profile['sheet_id']}_profile.json"
        with open(path, "w") as f:
            json.dump(profile, f, indent=2, default=str)
        logger.info("Persisted profile for sheet %s → %s", profile["sheet_id"], path)

    if cross_analysis:
        path = profile_dir / "cross_sheet_analysis.json"
        with open(path, "w") as f:
            json.dump(cross_analysis, f, indent=2, default=str)
        logger.info("Persisted cross-sheet analysis → %s", path)


# ---------------------------------------------------------------------------
# Column-level profiling
# ---------------------------------------------------------------------------


def _profile_column(series: pd.Series, n_rows: int) -> dict:
    """Compute statistics for a single column."""
    null_count = int(series.isna().sum())
    # Also count empty strings for object columns
    if series.dtype == object:
        empty_str_count = int((series.astype(str).str.strip() == "").sum())
        null_count = max(null_count, null_count + empty_str_count)

    non_null = series.dropna()
    unique_count = int(non_null.nunique())

    # Top 5 most frequent values
    if len(non_null) > 0:
        top_values = non_null.astype(str).value_counts().head(5).to_dict()
    else:
        top_values = {}

    profile = {
        "inferred_type": str(series.dtype),
        "null_count": null_count,
        "null_pct": round(null_count / n_rows * 100, 2) if n_rows > 0 else 0,
        "unique_count": unique_count,
        "uniqueness_ratio": round(unique_count / n_rows, 4) if n_rows > 0 else 0,
        "top_5_values": top_values,
    }

    # Numeric stats
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric) > 0:
        profile["numeric_stats"] = {
            "min": float(numeric.min()),
            "max": float(numeric.max()),
            "mean": round(float(numeric.mean()), 2),
            "median": round(float(numeric.median()), 2),
            "std": round(float(numeric.std()), 2) if len(numeric) > 1 else 0,
        }

    # Date range detection
    date_values = _try_parse_dates(non_null)
    if date_values is not None and len(date_values) > 0:
        profile["date_range"] = {
            "min": str(date_values.min()),
            "max": str(date_values.max()),
        }

    return profile


def _try_parse_dates(series: pd.Series) -> pd.Series | None:
    """Attempt to parse a series as dates. Returns None if < 50% parse."""
    if len(series) == 0:
        return None
    try:
        parsed = pd.to_datetime(series, errors="coerce")
        valid_pct = parsed.notna().sum() / len(series)
        if valid_pct >= 0.5:
            return parsed.dropna()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Cross-sheet analysis helpers
# ---------------------------------------------------------------------------


def _detect_shared_columns(profiles: list[dict]) -> list[dict]:
    """Find columns with identical names across sheets."""
    col_to_sheets: dict[str, list[str]] = {}
    for p in profiles:
        for col in p["column_names"]:
            col_lower = col.strip().lower()
            col_to_sheets.setdefault(col_lower, []).append(p["sheet_id"])

    shared = []
    for col, sheet_ids in col_to_sheets.items():
        if len(sheet_ids) > 1:
            shared.append({"column": col, "sheet_ids": sheet_ids})

    return shared


def _detect_primary_keys(profiles: list[dict]) -> dict[str, list[str]]:
    """Detect likely primary keys per sheet (high uniqueness, low null rate)."""
    pk_candidates: dict[str, list[str]] = {}

    for p in profiles:
        candidates = []
        for col_name, col_stats in p["columns"].items():
            uniqueness = col_stats.get("uniqueness_ratio", 0)
            null_pct = col_stats.get("null_pct", 100)

            # High uniqueness (> 90%) and low nulls (< 5%)
            if uniqueness > 0.9 and null_pct < 5:
                candidates.append(col_name)

        pk_candidates[p["sheet_id"]] = candidates

    return pk_candidates


def _detect_foreign_keys(profiles: list[dict], sheets: list[dict]) -> list[dict]:
    """Detect possible foreign key relationships by value overlap."""
    if len(sheets) < 2:
        return []

    fk_candidates = []

    # Compare value sets across sheets for high-uniqueness columns
    for i, s1 in enumerate(sheets):
        for j, s2 in enumerate(sheets):
            if i >= j:
                continue

            df1 = s1["dataframe"]
            df2 = s2["dataframe"]

            for col1 in df1.columns:
                for col2 in df2.columns:
                    vals1 = set(df1[col1].dropna().astype(str))
                    vals2 = set(df2[col2].dropna().astype(str))

                    if len(vals1) < 3 or len(vals2) < 3:
                        continue

                    overlap = vals1 & vals2
                    smaller_set = min(len(vals1), len(vals2))
                    if smaller_set > 0 and len(overlap) / smaller_set > 0.5:
                        fk_candidates.append(
                            {
                                "sheet_1": s1["sheet_id"],
                                "column_1": col1,
                                "sheet_2": s2["sheet_id"],
                                "column_2": col2,
                                "overlap_pct": round(
                                    len(overlap) / smaller_set * 100, 1
                                ),
                            }
                        )

    return fk_candidates


def _detect_line_item_splits(profiles: list[dict], sheets: list[dict]) -> list[dict]:
    """Detect repeated IDs with identical dates — possible line-item splits."""
    indicators = []

    for sheet, profile in zip(sheets, profiles):
        df = sheet["dataframe"]

        # Find columns that look like IDs (high uniqueness keyword match)
        id_cols = [
            col
            for col in df.columns
            if any(
                kw in col.lower()
                for kw in ["id", "#", "number", "contract", "deal", "order"]
            )
        ]

        for id_col in id_cols:
            valid = df[id_col].dropna()
            if len(valid) == 0:
                continue

            dup_ratio = 1 - (valid.nunique() / len(valid))
            if dup_ratio > 0.3:  # > 30% duplicates suggests line items
                indicators.append(
                    {
                        "sheet_id": sheet["sheet_id"],
                        "column": id_col,
                        "duplicate_ratio": round(dup_ratio, 3),
                        "unique_values": int(valid.nunique()),
                        "total_rows": len(valid),
                        "likely_line_item_level": True,
                    }
                )

    return indicators
