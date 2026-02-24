"""
anomalies.py — Detect suspicious rows and data quality issues.

Flags: negative values, impossibly long terms, date mismatches,
ACV/financial outliers (>3σ), missing critical fields, far-future dates,
high null rates per column, and overall completeness issues.
"""

import logging
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# Columns that should never be empty in a good dataset
_CRITICAL_COLS = ["Contract_ID", "Vendor", "Account_ID"]

# Financial columns where negative values are suspicious
_FINANCIAL_COLS = [
    "ACV", "Total_Value", "Net_ARR", "Quote_ARR",
    "Expiring_ARR", "Growth_ARR", "Customer_Amount", "Unit_Price",
]


def detect_anomalies(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Flag suspicious rows and return a summary of anomalies found.

    Adds an `_anomaly_flags` column (pipe-separated flag labels).

    Returns:
        (df_with_flags, anomaly_summary)
        anomaly_summary is a list of {type, count, severity, description} dicts.
    """
    flags: list[pd.Series] = []
    summary: list[dict] = []

    n = len(df)
    if n == 0:
        df["_anomaly_flags"] = ""
        return df, summary

    # ── 1. Negative financial values ──────────────────────────────
    for col in _FINANCIAL_COLS:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            mask = numeric < 0
            count = int(mask.sum())
            if count > 0:
                flags.append(mask.map({True: f"negative_{col}", False: ""}))
                summary.append({
                    "type": f"Negative {col}",
                    "count": count,
                    "severity": "high",
                    "description": f"{count} rows have negative {col} values.",
                })

    # ── 2. Impossibly long terms (>120 months) ────────────────────
    if "Term_Months" in df.columns:
        numeric = pd.to_numeric(df["Term_Months"], errors="coerce")
        mask = numeric > 120
        count = int(mask.sum())
        if count > 0:
            flags.append(mask.map({True: "long_term", False: ""}))
            summary.append({
                "type": "Long Term (>10yr)",
                "count": count,
                "severity": "medium",
                "description": f"{count} contracts have terms exceeding 120 months.",
            })

    # ── 3. Expiry before effective date ───────────────────────────
    if "Effective_Date" in df.columns and "Expiry_Date" in df.columns:
        eff = pd.to_datetime(df["Effective_Date"], errors="coerce")
        exp = pd.to_datetime(df["Expiry_Date"], errors="coerce")
        mask = (eff.notna() & exp.notna()) & (exp < eff)
        count = int(mask.sum())
        if count > 0:
            flags.append(mask.map({True: "date_mismatch", False: ""}))
            summary.append({
                "type": "Date Mismatch",
                "count": count,
                "severity": "high",
                "description": f"{count} rows where expiry date is before effective date.",
            })

    # ── 4. Financial outliers (>3 standard deviations) ────────────
    for col in _FINANCIAL_COLS:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            valid = numeric.dropna()
            if len(valid) > 10:
                mean = valid.mean()
                std = valid.std()
                if std > 0:
                    mask = ((numeric - mean).abs() > 3 * std) & numeric.notna()
                    count = int(mask.sum())
                    if count > 0:
                        flags.append(mask.map({True: f"outlier_{col}", False: ""}))
                        summary.append({
                            "type": f"{col} Outlier",
                            "count": count,
                            "severity": "medium",
                            "description": f"{count} rows with {col} >3σ from mean (${mean:,.0f} ± ${std:,.0f}).",
                        })

    # ── 5. Missing critical fields ────────────────────────────────
    for col in _CRITICAL_COLS:
        if col in df.columns:
            mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
            count = int(mask.sum())
            if count > 0:
                flags.append(mask.map({True: f"missing_{col}", False: ""}))
                summary.append({
                    "type": f"Missing {col}",
                    "count": count,
                    "severity": "high" if col in ("Contract_ID", "Account_ID") else "medium",
                    "description": f"{count} rows are missing {col}.",
                })

    # ── 6. Empty/null cells across ALL columns ────────────────────
    # Flag columns with significant emptiness (>20% null)
    for col in df.columns:
        if col.startswith("meta_") or col == "_anomaly_flags":
            continue
        null_count = int(df[col].isna().sum())
        # Also count empty strings
        if df[col].dtype == object:
            empty_str = int((df[col].astype(str).str.strip() == "").sum())
            null_count = max(null_count, null_count + empty_str)

        null_pct = null_count / n * 100
        if null_pct > 20 and null_count > 5:
            summary.append({
                "type": f"Empty: {col}",
                "count": null_count,
                "severity": "medium" if null_pct > 50 else "low",
                "description": f"{col} is {null_pct:.0f}% empty ({null_count}/{n} rows missing).",
            })

    # ── 7. Future close dates (>1 year from now) ──────────────────
    if "Contract_Close_Date" in df.columns:
        dates = pd.to_datetime(df["Contract_Close_Date"], errors="coerce")
        future_cutoff = pd.Timestamp.now() + pd.DateOffset(years=1)
        mask = dates > future_cutoff
        count = int(mask.sum())
        if count > 0:
            flags.append(mask.map({True: "future_close", False: ""}))
            summary.append({
                "type": "Far Future Close",
                "count": count,
                "severity": "low",
                "description": f"{count} contracts have close dates >1 year in the future.",
            })

    # ── 8. Completely empty rows (all non-meta cols null) ─────────
    data_cols = [c for c in df.columns if not c.startswith("meta_") and c != "_anomaly_flags"]
    if data_cols:
        mask = df[data_cols].isna().all(axis=1)
        count = int(mask.sum())
        if count > 0:
            flags.append(mask.map({True: "empty_row", False: ""}))
            summary.append({
                "type": "Empty Rows",
                "count": count,
                "severity": "medium",
                "description": f"{count} rows are completely empty across all data fields.",
            })

    # ── 9. Duplicate contract IDs ─────────────────────────────────
    if "Contract_ID" in df.columns:
        valid_ids = df["Contract_ID"].dropna()
        dup_mask = valid_ids.duplicated(keep=False)
        dup_count = int(dup_mask.sum())
        if dup_count > 0:
            summary.append({
                "type": "Duplicate Contract IDs",
                "count": dup_count,
                "severity": "medium",
                "description": f"{dup_count} rows share the same Contract_ID with other rows.",
            })

    # ── Combine flags ─────────────────────────────────────────────
    if flags:
        combined = pd.DataFrame(flags).T
        df["_anomaly_flags"] = combined.apply(
            lambda row: " | ".join(f for f in row if f), axis=1
        )
    else:
        df["_anomaly_flags"] = ""

    total_flagged = int((df["_anomaly_flags"] != "").sum())
    logger.info("Anomaly detection: %d/%d rows flagged, %d issue types.", total_flagged, n, len(summary))

    return df, summary
