"""
ingestion.py — Parse Excel/CSV files into DataFrames.

Simplified ingestion for the Contract Data Merger.
Accepts .xlsx (multi-sheet) and .csv files, returns structured sheet records.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest_file(filepath: str) -> list[dict]:
    """Ingest an Excel or CSV file and return a list of sheet records.

    Each record contains:
      - sheet_name: str
      - dataframe: pd.DataFrame (raw, all columns as strings)
      - row_count: int
      - column_count: int
      - columns: list[str]
      - sample_values: dict[str, list[str]]  (up to 8 samples per column)
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    suffix = filepath.suffix.lower()
    if suffix == ".csv":
        return _ingest_csv(filepath)
    elif suffix in (".xlsx", ".xls"):
        return _ingest_excel(filepath)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def _ingest_excel(filepath: Path) -> list[dict]:
    """Parse all sheets from an Excel file."""
    sheets: list[dict] = []

    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as exc:
        logger.error("Failed to open Excel file %s: %s", filepath, exc)
        raise

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
            df = df.dropna(how="all")

            if df.empty:
                logger.info("Sheet '%s' is empty — skipping.", sheet_name)
                continue

            # Filter out pivot/junk sheets
            total_cols = len(df.columns)
            unnamed_cols = sum(
                1 for c in df.columns if str(c).startswith("Unnamed")
            )
            named_ratio = (
                (total_cols - unnamed_cols) / total_cols if total_cols > 0 else 0
            )
            if named_ratio < 0.5 or (total_cols - unnamed_cols) < 2:
                logger.info(
                    "Sheet '%s' looks like a pivot table — skipping.", sheet_name
                )
                continue

            # Drop Unnamed columns
            unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
            if unnamed:
                df = df.drop(columns=unnamed)

            sheets.append(_build_record(sheet_name, df))

        except Exception as exc:
            logger.error("Error reading sheet '%s': %s", sheet_name, exc)

    return sheets


def _ingest_csv(filepath: Path) -> list[dict]:
    """Parse a single CSV file."""
    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig", dtype=str)
        df = df.dropna(how="all")
        if df.empty:
            return []
        # Drop Unnamed columns
        unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
        if unnamed:
            df = df.drop(columns=unnamed)
        return [_build_record(filepath.stem, df)]
    except Exception as exc:
        logger.error("Error reading CSV %s: %s", filepath, exc)
        raise


def _build_record(sheet_name: str, df: pd.DataFrame) -> dict:
    """Build a standardized sheet record from a DataFrame."""
    columns = list(df.columns)
    sample_values = {}
    for col in columns:
        vals = df[col].dropna().astype(str).unique()
        sample_values[col] = list(vals[:8])

    return {
        "sheet_name": sheet_name,
        "dataframe": df,
        "row_count": len(df),
        "column_count": len(columns),
        "columns": columns,
        "sample_values": sample_values,
    }
