"""
ingest.py — File loading for Excel (multi-tab) and CSV directory modes.

Every row gets `meta_source_tab` and `meta_source_file` columns for auditing.
"""

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def load_excel(filepath: str) -> list[pd.DataFrame]:
    """Load all sheets from a single .xlsx file.

    Returns a list of DataFrames, one per non-empty sheet,
    each annotated with source metadata columns.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Excel file not found: {filepath}")

    frames: list[pd.DataFrame] = []
    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as exc:
        logger.error("Failed to open Excel file %s: %s", filepath, exc)
        return frames

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            df = _clean_frame(df, sheet_name, filepath.name)
            if df.empty:
                logger.warning(
                    "Sheet '%s' in %s is empty after cleanup — skipping.",
                    sheet_name,
                    filepath.name,
                )
                continue
            df["meta_source_tab"] = sheet_name
            df["meta_source_file"] = filepath.name
            frames.append(df)
            logger.info(
                "Loaded %d rows from sheet '%s' (%s).",
                len(df),
                sheet_name,
                filepath.name,
            )
        except Exception as exc:
            logger.error(
                "Error reading sheet '%s' in %s: %s — skipping.",
                sheet_name,
                filepath.name,
                exc,
            )

    return frames


def load_csv_dir(dir_path: str) -> list[pd.DataFrame]:
    """Load all .csv and .xlsx files from a directory.

    Returns a list of DataFrames annotated with source metadata.
    """
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a valid directory: {dir_path}")

    frames: list[pd.DataFrame] = []
    supported = {".csv", ".xlsx"}

    for entry in sorted(dir_path.iterdir()):
        if entry.suffix.lower() not in supported:
            continue

        try:
            if entry.suffix.lower() == ".csv":
                df = pd.read_csv(entry, encoding="utf-8-sig")
                df = _clean_frame(df, "csv", entry.name)
                if df.empty:
                    logger.warning(
                        "CSV file %s is empty after cleanup — skipping.", entry.name
                    )
                    continue
                df["meta_source_tab"] = "csv"
                df["meta_source_file"] = entry.name
                frames.append(df)
                logger.info("Loaded %d rows from %s.", len(df), entry.name)

            elif entry.suffix.lower() == ".xlsx":
                sub_frames = load_excel(str(entry))
                frames.extend(sub_frames)

        except Exception as exc:
            logger.error("Error reading %s: %s — skipping.", entry.name, exc)

    return frames


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
import re

_UNNAMED_RE = re.compile(r"^Unnamed:\s*\d+", re.IGNORECASE)


def _clean_frame(df: pd.DataFrame, sheet: str, filename: str) -> pd.DataFrame:
    """Clean a raw DataFrame immediately after loading.

    1. Drop columns whose header matches 'Unnamed: N' (empty Excel columns).
    2. Drop columns whose header is entirely whitespace or empty.
    3. Strip leading/trailing whitespace from all remaining headers.
    4. Drop rows that are entirely empty (all NaN).
    5. Drop columns that are entirely NaN after row cleanup.
    """
    original_cols = len(df.columns)
    original_rows = len(df)

    # 1. Drop 'Unnamed: N' columns
    unnamed_cols = [c for c in df.columns if _UNNAMED_RE.match(str(c))]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
        logger.info(
            "Dropped %d unnamed column(s) from '%s' in %s.",
            len(unnamed_cols),
            sheet,
            filename,
        )

    # 2. Drop columns with blank/whitespace-only headers
    blank_cols = [c for c in df.columns if not str(c).strip()]
    if blank_cols:
        df = df.drop(columns=blank_cols)

    # 3. Strip whitespace from headers
    df.columns = [str(c).strip() for c in df.columns]

    # 4. Drop entirely empty rows
    df = df.dropna(how="all")

    # 5. Drop columns that are entirely NaN
    all_nan_cols = [c for c in df.columns if df[c].isna().all()]
    if all_nan_cols:
        df = df.drop(columns=all_nan_cols)
        logger.info(
            "Dropped %d all-NaN column(s) from '%s' in %s: %s",
            len(all_nan_cols),
            sheet,
            filename,
            all_nan_cols,
        )

    # 6. Expand composite columns (e.g. "Opportunity : Account Name : Account ID")
    df = _expand_composite_columns(df)

    dropped_cols = original_cols - len(df.columns)
    dropped_rows = original_rows - len(df)
    if dropped_cols or dropped_rows:
        logger.info(
            "Cleanup '%s' (%s): removed %d col(s), %d empty row(s). Remaining: %d rows × %d cols.",
            sheet,
            filename,
            dropped_cols,
            dropped_rows,
            len(df),
            len(df.columns),
        )

    return df


def _expand_composite_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Split composite columns like 'Opportunity : Account Name : Account ID'.

    When a column header contains ' : ' separators, create a new column
    using the last segment as the name, copying the data. This allows
    the schema mapper to match it by its short name (e.g. 'Account ID').
    """
    new_cols = {}
    for col in df.columns:
        if " : " in col:
            # Extract the last segment as the short name
            segments = [s.strip() for s in col.split(" : ")]
            short_name = segments[-1]  # e.g. "Account ID"

            # Only add if the short name doesn't already exist
            if short_name not in df.columns and short_name not in new_cols:
                new_cols[short_name] = df[col].copy()
                logger.info(
                    "Extracted column '%s' from composite '%s'.",
                    short_name,
                    col,
                )

    if new_cols:
        for name, series in new_cols.items():
            df[name] = series
        # Drop original composite columns to avoid duplicates
        composite_cols = [c for c in df.columns if " : " in c]
        df = df.drop(columns=composite_cols)
        logger.info(
            "Dropped %d composite column(s) after extraction.", len(composite_cols)
        )

    return df
