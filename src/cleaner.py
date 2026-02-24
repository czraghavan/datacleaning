"""
cleaner.py — SaaS-specific data cleaning logic.

Handles date normalization, financial scrubbing, and boolean standardization.
"""

import re
import logging

import pandas as pd
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# Columns that are known date columns by canonical name
DATE_COLUMNS = {
    "Effective_Date", "Renewal_Date", "Expiry_Date",
    "Contract_Close_Date",
}

# Strings that indicate auto-renewal instead of an actual date
AUTO_RENEW_TOKENS = {
    "auto-renew", "auto renew", "autorenew", "auto renewal",
    "auto-renewal", "evergreen", "perpetual",
}

# Boolean normalization map (lowercase key → bool)
BOOL_MAP = {
    "yes": True, "no": False,
    "y": True, "n": False,
    "true": True, "false": False,
    "1": True, "0": False,
    "checked": True, "unchecked": False,
    "x": True, "": False,
}

# Excel error patterns to sanitize (case-insensitive)
EXCEL_ERROR_PATTERN = re.compile(
    r"^\s*#(REF|ERROR|VALUE|DIV/0|NAME\?|NULL|N/A)!?\s*$",
    re.IGNORECASE,
)


class Cleaner:
    """Applies SaaS-specific cleaning transformations to a DataFrame."""

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the full cleaning pipeline."""
        df = self.sanitize_errors(df)
        df = self.normalize_dates(df)
        df = self.scrub_financials(df)
        df = self.standardize_booleans(df)
        return df

    # ------------------------------------------------------------------
    # Excel error sanitization
    # ------------------------------------------------------------------
    def sanitize_errors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace Excel error values (#REF!, #ERROR!, etc.) with NaN.

        Logs how many error cells were found per column.
        """
        total_errors = 0
        for col in df.columns:
            if col.startswith("meta_"):
                continue
            if df[col].dtype != object:
                continue

            mask = df[col].astype(str).str.match(EXCEL_ERROR_PATTERN, na=False)
            count = mask.sum()
            if count > 0:
                df.loc[mask, col] = pd.NA
                total_errors += count
                logger.warning(
                    "Sanitized %d Excel error(s) in column '%s'.", count, col,
                )

        if total_errors:
            logger.info("Total Excel errors sanitized: %d.", total_errors)
        return df

    # ------------------------------------------------------------------
    # Date normalization
    # ------------------------------------------------------------------
    def normalize_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert date-like columns to ISO 8601 (YYYY-MM-DD).

        Strings like "Auto-renew" or "N/A" are stored in an
        `is_auto_renew` boolean column and the date cell is set to NaT.

        Date ranges like "4/15/2025 to 4/14/2026" are split into start
        and end date columns automatically.
        """
        if "is_auto_renew" not in df.columns:
            df["is_auto_renew"] = False

        # Handle "Contract Date Range" or similar range columns first
        range_cols = [c for c in df.columns if "range" in c.lower() and "date" in c.lower()]
        for col in range_cols:
            df = self._split_date_range_column(df, col)

        # Find date columns: canonical names + any column containing "date"
        date_cols = [
            c for c in df.columns
            if c in DATE_COLUMNS or ("date" in c.lower() and "range" not in c.lower())
        ]

        for col in date_cols:
            df = self._clean_date_column(df, col)

        return df

    def _split_date_range_column(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Split a 'date range' column (e.g. '4/15/2025 to 4/14/2026') into two columns."""
        start_dates = []
        end_dates = []

        for val in df[col]:
            if pd.isna(val):
                start_dates.append(pd.NaT)
                end_dates.append(pd.NaT)
                continue

            val_str = str(val).strip()

            # Try splitting on common range separators
            parts = None
            for sep in [" to ", " - ", " – ", " — ", " thru ", " through "]:
                if sep in val_str.lower():
                    idx = val_str.lower().index(sep)
                    parts = (val_str[:idx].strip(), val_str[idx + len(sep):].strip())
                    break

            if parts:
                try:
                    start_dates.append(dateparser.parse(parts[0], dayfirst=False))
                except (ValueError, OverflowError):
                    start_dates.append(pd.NaT)
                try:
                    end_dates.append(dateparser.parse(parts[1], dayfirst=False))
                except (ValueError, OverflowError):
                    end_dates.append(pd.NaT)
            else:
                # Not a range — try as single date
                try:
                    dt = dateparser.parse(val_str, dayfirst=False)
                    start_dates.append(dt)
                    end_dates.append(pd.NaT)
                except (ValueError, OverflowError):
                    start_dates.append(pd.NaT)
                    end_dates.append(pd.NaT)

        # Create the split columns (only if they don't already exist)
        if "Effective_Date" not in df.columns:
            df["Effective_Date"] = pd.to_datetime(start_dates, errors="coerce")
            df["Effective_Date"] = df["Effective_Date"].dt.strftime("%Y-%m-%d").replace("NaT", pd.NA)
        if "Expiry_Date" not in df.columns:
            df["Expiry_Date"] = pd.to_datetime(end_dates, errors="coerce")
            df["Expiry_Date"] = df["Expiry_Date"].dt.strftime("%Y-%m-%d").replace("NaT", pd.NA)

        # Drop the original range column
        df = df.drop(columns=[col])
        logger.info("Split date range column '%s' into 'Effective_Date' and 'Expiry_Date'.", col)

        return df

    def _clean_date_column(self, df: pd.DataFrame, col: str) -> pd.DataFrame:
        """Parse a single column into datetime, handling auto-renew tokens."""
        parsed = []
        auto_flags = df["is_auto_renew"].copy()

        for idx, val in df[col].items():
            if pd.isna(val):
                parsed.append(pd.NaT)
                continue

            val_str = str(val).strip()

            # Check for auto-renew tokens
            if val_str.lower() in AUTO_RENEW_TOKENS or val_str.upper() == "N/A":
                parsed.append(pd.NaT)
                auto_flags.at[idx] = True
                continue

            # Handle date ranges in non-range columns (fallback)
            for sep in [" to ", " - ", " – ", " — "]:
                if sep in val_str.lower():
                    idx_sep = val_str.lower().index(sep)
                    val_str = val_str[:idx_sep].strip()
                    break

            # Try parsing
            try:
                dt = dateparser.parse(val_str, dayfirst=False)
                parsed.append(dt)
            except (ValueError, OverflowError):
                logger.warning("Unparseable date '%s' in column '%s' (row %s).", val_str, col, idx)
                parsed.append(pd.NaT)

        df[col] = pd.to_datetime(parsed, errors="coerce")
        # Format to YYYY-MM-DD string for clean output, but keep NaT as NaT
        df[col] = df[col].dt.strftime("%Y-%m-%d").replace("NaT", pd.NA)
        df["is_auto_renew"] = auto_flags

        return df

    # ------------------------------------------------------------------
    # Financial scrubbing
    # ------------------------------------------------------------------
    def scrub_financials(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip currency symbols, expand k/M suffixes, compute ACV if possible."""
        # Canonical financial columns (including new separated ARR fields)
        _CANONICAL_FINANCIAL = {
            "ACV", "Total_Value", "Net_ARR", "Quote_ARR", "Expiring_ARR",
            "Growth_ARR", "Customer_Amount", "Unit_Price", "Discount",
        }
        financial_cols = [
            c for c in df.columns
            if c in _CANONICAL_FINANCIAL or any(
                kw in c.lower() for kw in ("value", "cost", "price", "amount", "acv", "tcv", "arr")
            )
        ]
        # Exclude meta / non-financial columns that might match
        financial_cols = [
            c for c in financial_cols
            if c not in {"meta_source_tab", "meta_source_file", "is_auto_renew"}
        ]

        for col in financial_cols:
            df[col] = df[col].apply(self._parse_currency)

        # Compute ACV from Total_Value / Term_Months if ACV is missing or NaN
        if "Total_Value" in df.columns and "Term_Months" in df.columns:
            if "ACV" not in df.columns:
                df["ACV"] = pd.NA

            mask = df["ACV"].isna() & df["Total_Value"].notna() & df["Term_Months"].notna()
            if mask.any():
                term = pd.to_numeric(df.loc[mask, "Term_Months"], errors="coerce")
                total = pd.to_numeric(df.loc[mask, "Total_Value"], errors="coerce")
                df.loc[mask, "ACV"] = (total / term * 12).round(2)
                logger.info("Computed ACV for %d rows from Total_Value / Term_Months.", mask.sum())

        return df

    @staticmethod
    def _parse_currency(val) -> float | None:
        """Convert a messy currency value to a float.

        Handles: "$1,234.56", "£50k", "1.2M", "€1 234", etc.
        """
        if pd.isna(val):
            return None

        val_str = str(val).strip()
        if not val_str or val_str.upper() in {"N/A", "NA", "-", "—", "TBD"}:
            return None

        # Remove currency symbols and whitespace
        val_str = re.sub(r"[£$€¥₹\s]", "", val_str)
        # Remove commas used as thousands separators
        val_str = val_str.replace(",", "")

        # Detect multiplier suffixes
        multiplier = 1.0
        if val_str.lower().endswith("m"):
            multiplier = 1_000_000
            val_str = val_str[:-1]
        elif val_str.lower().endswith("k"):
            multiplier = 1_000
            val_str = val_str[:-1]

        try:
            return round(float(val_str) * multiplier, 2)
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Boolean standardization
    # ------------------------------------------------------------------
    def standardize_booleans(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert Yes/No/1/0/Checked columns to True/False."""
        for col in df.columns:
            if col.startswith("meta_") or col == "is_auto_renew":
                continue

            if df[col].dtype == object:
                unique_lower = {
                    str(v).strip().lower()
                    for v in df[col].dropna().unique()
                }
                # Only convert if ALL non-null unique values are boolean-like
                if unique_lower and unique_lower.issubset(BOOL_MAP.keys()):
                    df[col] = df[col].apply(
                        lambda v: BOOL_MAP.get(str(v).strip().lower(), v) if pd.notna(v) else v
                    )
                    logger.info("Standardized boolean column: '%s'", col)

        return df
