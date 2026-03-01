"""
merge.py — Concatenation, quote-line aggregation, and deduplication.

Merges all ingested DataFrames into a single master frame, aggregates
quote lines by (Contract_ID, Contract_Close_Date), and deduplicates.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Columns that should never be aggregated
META_COLS = {
    "meta_source_tab",
    "meta_source_file",
    "Vendor_Canonical",
    "Vendor_Cluster_ID",
}


def concat_all(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate a list of DataFrames with outer join (no column loss).

    After concat, drops any columns that are entirely NaN (artifacts of
    outer-joining frames with different schemas).
    """
    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True, sort=False)

    # Drop columns that became entirely NaN after concat
    all_nan = [c for c in combined.columns if combined[c].isna().all()]
    if all_nan:
        combined = combined.drop(columns=all_nan)
        logger.info(
            "Post-concat: dropped %d all-NaN columns: %s", len(all_nan), all_nan
        )

    # Drop any 'Unnamed: N' columns that slipped through
    import re

    unnamed_re = re.compile(r"^Unnamed:\s*\d+", re.IGNORECASE)
    unnamed = [c for c in combined.columns if unnamed_re.match(str(c))]
    if unnamed:
        combined = combined.drop(columns=unnamed)
        logger.info("Post-concat: dropped %d unnamed columns.", len(unnamed))

    logger.info(
        "Concatenated %d frames → %d total rows, %d columns.",
        len(dfs),
        len(combined),
        len(combined.columns),
    )
    return combined


def merge_quote_lines(
    df: pd.DataFrame,
    id_col: str = "Contract_ID",
    date_col: str = "Contract_Close_Date",
) -> pd.DataFrame:
    """Merge multiple quote lines into single contracts.

    Groups by (Contract_ID, Contract_Close_Date) and aggregates:
      - Numeric columns: sum
      - Text / object columns: first non-null (or most frequent)
      - List / comma-delimited: join unique values

    Auto-detects grouping columns if the defaults aren't found.
    """
    # Auto-detect ID column
    id_col = _find_column(df, id_col, _ID_KEYWORDS)
    date_col = _find_column(df, date_col, _DATE_KEYWORDS)

    if id_col is None:
        logger.warning(
            "Cannot merge quote lines — no ID column found (tried Contract_ID and keywords %s). Returning as-is.",
            _ID_KEYWORDS,
        )
        return df

    # Date column is optional — if missing, group by ID alone
    group_cols = [id_col]
    if date_col is not None:
        group_cols.append(date_col)
        has_key = df[id_col].notna() & df[date_col].notna()
    else:
        logger.info("No close-date column found — grouping by '%s' only.", id_col)
        has_key = df[id_col].notna()

    # Separate rows that have a valid contract identity from those that don't
    df_keyed = df[has_key].copy()
    df_orphan = df[~has_key].copy()

    if df_keyed.empty:
        logger.info(
            "No rows with valid grouping key (%s) — skipping merge.", group_cols
        )
        return df

    pre_count = len(df_keyed)

    # Build per-column aggregation rules
    agg_rules: dict[str, str | callable] = {}
    for col in df_keyed.columns:
        if col in set(group_cols):
            continue
        if col in META_COLS:
            # Keep all unique source references
            agg_rules[col] = _join_unique
        elif pd.api.types.is_numeric_dtype(df_keyed[col]):
            agg_rules[col] = "sum"
        elif pd.api.types.is_bool_dtype(df_keyed[col]):
            agg_rules[col] = "first"
        else:
            agg_rules[col] = _first_non_null

    merged = df_keyed.groupby(group_cols, dropna=False).agg(agg_rules).reset_index()

    post_count = len(merged)
    logger.info(
        "Quote-line merge: %d rows → %d contracts (grouped by %s).",
        pre_count,
        post_count,
        group_cols,
    )

    # Re-attach orphan rows
    if not df_orphan.empty:
        logger.info("Appending %d orphan rows (missing contract key).", len(df_orphan))
        merged = pd.concat([merged, df_orphan], ignore_index=True, sort=False)

    return merged


def deduplicate(
    df: pd.DataFrame,
    subset: list[str] | None = None,
) -> tuple[pd.DataFrame, int]:
    """Remove duplicate contracts, keeping the row with the fewest nulls.

    Args:
        df: Merged DataFrame.
        subset: Columns to use for detecting duplicates.
                Defaults to (Vendor_Canonical, Effective_Date, ACV).

    Returns:
        (deduplicated DataFrame, number of duplicates removed)
    """
    if subset is None:
        subset = ["Vendor_Canonical", "Effective_Date", "ACV"]

    # Only use columns that exist
    subset = [c for c in subset if c in df.columns]
    if not subset:
        logger.warning("No dedup columns found — skipping deduplication.")
        return df, 0

    pre_count = len(df)

    # Score each row by number of non-null cells (higher = better)
    df["_completeness"] = df.notna().sum(axis=1)
    df = df.sort_values("_completeness", ascending=False)
    df = df.drop_duplicates(subset=subset, keep="first")
    df = df.drop(columns=["_completeness"])

    removed = pre_count - len(df)
    if removed:
        logger.info("Deduplication: removed %d duplicates (key: %s).", removed, subset)

    return df, removed


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

# Keywords for auto-detecting ID columns (case-insensitive substring match)
_ID_KEYWORDS = [
    "contract_id",
    "contract id",
    "quote_id",
    "quote id",
    "deal_id",
    "deal id",
    "opp_id",
    "opp id",
    "opportunity_id",
    "order_id",
    "order id",
    "agreement_id",
    "po_number",
    "subscription_id",
    "record_id",
    "ref",
    "contract #",
    "quote #",
    "deal #",
    "opp #",
    "order #",
    "contract_number",
    "quote_number",
    "deal_number",
]

# Keywords for auto-detecting close-date columns
_DATE_KEYWORDS = [
    "close_date",
    "close date",
    "closed_date",
    "closed date",
    "booking_date",
    "booking date",
    "win_date",
    "won_date",
    "signed_date",
    "executed_date",
    "creation_date",
    "order_date",
    "purchase_date",
]


def _find_column(df: pd.DataFrame, preferred: str, keywords: list[str]) -> str | None:
    """Find a column by preferred name, or by keyword search in column names.

    Returns the column name if found, or None if not found.
    """
    # 1. Exact match
    if preferred in df.columns:
        return preferred

    # 2. Case-insensitive exact match
    col_lower_map = {c.lower().strip(): c for c in df.columns}
    if preferred.lower() in col_lower_map:
        return col_lower_map[preferred.lower()]

    # 3. Keyword substring search (case-insensitive)
    for kw in keywords:
        for col in df.columns:
            if kw in col.lower():
                logger.info("Auto-detected column '%s' via keyword '%s'.", col, kw)
                return col

    return None


def _first_non_null(series: pd.Series):
    """Return first non-null value in a Series."""
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    return non_null.iloc[0]


def _join_unique(series: pd.Series) -> str:
    """Join unique non-null values with ' | '."""
    vals = series.dropna().astype(str).unique()
    return " | ".join(vals) if len(vals) else ""
