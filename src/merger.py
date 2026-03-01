"""
merger.py — Core merge logic for the Contract Data Merger.

Merges DataFrames by a composite key (account_id + close_date),
validates column conflicts, and supports incremental appending.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# The two required key columns after mapping
KEY_COLS = ["account_id", "close_date"]


def apply_column_mapping(
    df: pd.DataFrame, mapping: dict[str, str]
) -> pd.DataFrame:
    """Rename columns according to user-provided mapping.

    Args:
        df: Raw DataFrame.
        mapping: {raw_column_name: target_column_name}.
                 Columns mapped to None or "" are dropped.

    Returns:
        DataFrame with renamed columns; unmapped columns dropped.
    """
    rename_map = {}
    drop_cols = []

    for raw_col, target in mapping.items():
        if raw_col not in df.columns:
            continue
        if target and target.strip():
            rename_map[raw_col] = target.strip()
        else:
            drop_cols.append(raw_col)

    # Keep only columns that were mapped
    keep = set(rename_map.keys())
    all_cols = set(df.columns)
    extra = all_cols - keep - set(drop_cols)
    df = df.drop(columns=list(extra) + drop_cols, errors="ignore")
    df = df.rename(columns=rename_map)

    return df


def merge_sheets(
    dfs: list[pd.DataFrame],
    key_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Merge multiple mapped DataFrames on the composite key.

    Uses an outer join so no rows are lost. For overlapping non-key
    columns, validates that values match for the same key, then
    coalesces into a single column.

    Args:
        dfs: List of DataFrames (already column-mapped).
        key_cols: Composite key columns. Defaults to KEY_COLS.

    Returns:
        Merged master DataFrame.

    Raises:
        ValueError: If overlapping columns have conflicting values.
    """
    if not dfs:
        return pd.DataFrame()

    key_cols = key_cols or KEY_COLS

    # Validate that all dfs have the key columns
    for i, df in enumerate(dfs):
        missing = [k for k in key_cols if k not in df.columns]
        if missing:
            raise ValueError(
                f"Sheet {i + 1} is missing required key column(s): {missing}. "
                f"Please map these columns before merging."
            )

    master = dfs[0].copy()

    for i, new_df in enumerate(dfs[1:], start=2):
        master = _merge_two(master, new_df, key_cols, source_label=f"Sheet {i}")

    # Drop rows where all key columns are null
    key_mask = master[key_cols].notna().any(axis=1)
    master = master[key_mask].reset_index(drop=True)

    logger.info(
        "Merged %d sheets → %d rows, %d columns.",
        len(dfs),
        len(master),
        len(master.columns),
    )
    return master


def append_to_master(
    master: pd.DataFrame,
    new_df: pd.DataFrame,
    key_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Append new columns from new_df to existing master sheet.

    Matches rows by the composite key. New columns are added; existing
    columns are validated for consistency.

    Args:
        master: Existing master DataFrame.
        new_df: New DataFrame with additional column data.
        key_cols: Composite key columns.

    Returns:
        Updated master DataFrame with new columns.

    Raises:
        ValueError: If overlapping columns have conflicting values.
    """
    key_cols = key_cols or KEY_COLS

    missing = [k for k in key_cols if k not in new_df.columns]
    if missing:
        raise ValueError(
            f"New data is missing required key column(s): {missing}. "
            f"Please map these columns before appending."
        )

    return _merge_two(master, new_df, key_cols, source_label="Appended file")


def _merge_two(
    left: pd.DataFrame,
    right: pd.DataFrame,
    key_cols: list[str],
    source_label: str = "new data",
) -> pd.DataFrame:
    """Merge two DataFrames on key_cols with conflict validation.

    For overlapping non-key columns, checks that values are consistent
    before coalescing. Raises ValueError on conflicts.
    """
    # Find overlapping non-key columns
    left_data_cols = set(left.columns) - set(key_cols)
    right_data_cols = set(right.columns) - set(key_cols)
    overlap = left_data_cols & right_data_cols

    if overlap:
        # Check for conflicts on overlapping columns
        _validate_overlap(left, right, key_cols, overlap, source_label)

        # Rename overlapping cols in right to avoid _x/_y suffixes
        # We'll coalesce after merge
        right_renamed = right.copy()
        rename_map = {col: f"{col}__right" for col in overlap}
        right_renamed = right_renamed.rename(columns=rename_map)

        merged = pd.merge(left, right_renamed, on=key_cols, how="outer")

        # Coalesce overlapping columns: prefer left, fill with right
        for col in overlap:
            right_col = f"{col}__right"
            if right_col in merged.columns:
                merged[col] = merged[col].fillna(merged[right_col])
                merged = merged.drop(columns=[right_col])

        return merged
    else:
        return pd.merge(left, right, on=key_cols, how="outer")


def _validate_overlap(
    left: pd.DataFrame,
    right: pd.DataFrame,
    key_cols: list[str],
    overlap: set[str],
    source_label: str,
) -> None:
    """Check that overlapping columns have consistent values for matching keys.

    Raises ValueError with details about conflicts.
    """
    # Inner join on keys to find matching rows
    common = pd.merge(
        left[key_cols + list(overlap)],
        right[key_cols + list(overlap)],
        on=key_cols,
        how="inner",
        suffixes=("_left", "_right"),
    )

    if common.empty:
        return  # No overlapping rows, no conflicts possible

    conflicts = []
    for col in overlap:
        left_col = f"{col}_left"
        right_col = f"{col}_right"

        if left_col not in common.columns or right_col not in common.columns:
            continue

        # Compare only where both values are non-null
        mask = common[left_col].notna() & common[right_col].notna()
        if not mask.any():
            continue

        # Normalize to strings for comparison
        left_vals = common.loc[mask, left_col].astype(str).str.strip()
        right_vals = common.loc[mask, right_col].astype(str).str.strip()
        mismatches = left_vals != right_vals

        if mismatches.any():
            n_conflicts = mismatches.sum()
            # Get a sample of conflicting rows
            sample_idx = common.loc[mask][mismatches].head(3).index
            examples = []
            for idx in sample_idx:
                key_vals = {k: str(common.loc[idx, k]) for k in key_cols}
                examples.append(
                    f"  Key {key_vals}: "
                    f"existing='{common.loc[idx, left_col]}' vs "
                    f"new='{common.loc[idx, right_col]}'"
                )
            conflicts.append(
                f"Column '{col}' has {n_conflicts} conflicting value(s) "
                f"from {source_label}:\n" + "\n".join(examples)
            )

    if conflicts:
        raise ValueError(
            "Column value conflicts detected during merge:\n\n"
            + "\n\n".join(conflicts)
        )


def get_master_summary(df: pd.DataFrame) -> dict:
    """Build a summary dict for the master sheet."""
    if df.empty:
        return {
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "key_coverage": {},
        }

    key_coverage = {}
    for col in KEY_COLS:
        if col in df.columns:
            non_null = int(df[col].notna().sum())
            key_coverage[col] = {
                "non_null": non_null,
                "total": len(df),
                "pct": round(non_null / len(df) * 100, 1) if len(df) > 0 else 0,
            }

    return {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "key_coverage": key_coverage,
    }
