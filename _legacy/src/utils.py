"""
utils.py — Shared helper functions for data parsing and cleaning.

These utilities are used by the transformation layer (Layer 5) during
extraction. They are stateless, deterministic, and have no side effects.
"""

import logging
import re
from typing import Any

import pandas as pd
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Currency / financial parsing
# ---------------------------------------------------------------------------


def parse_currency(val: Any) -> float | None:
    """Convert a messy currency value to a float.

    Handles: "$1,234.56", "£50k", "1.2M", "€1 234", etc.
    Returns None for unparseable values.
    """
    if pd.isna(val):
        return None

    val_str = str(val).strip()
    if not val_str or val_str.upper() in {"N/A", "NA", "-", "—", "TBD", ""}:
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
    elif val_str.lower().endswith("b"):
        multiplier = 1_000_000_000
        val_str = val_str[:-1]

    try:
        return round(float(val_str) * multiplier, 2)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

# Strings that indicate auto-renewal instead of an actual date
AUTO_RENEW_TOKENS = {
    "auto-renew",
    "auto renew",
    "autorenew",
    "auto renewal",
    "auto-renewal",
    "evergreen",
    "perpetual",
}

# Range separators for date range strings
_RANGE_SEPARATORS = [" to ", " - ", " – ", " — ", " thru ", " through "]


def parse_date(val: Any, dayfirst: bool = False) -> pd.Timestamp | None:
    """Parse a single value into a Timestamp.

    Returns None for unparseable values. Detects auto-renew tokens.
    """
    if pd.isna(val):
        return None

    val_str = str(val).strip()
    if not val_str or val_str.upper() == "N/A":
        return None

    # Check for auto-renew tokens
    if val_str.lower() in AUTO_RENEW_TOKENS:
        return None

    # Handle date ranges — take the first date
    for sep in _RANGE_SEPARATORS:
        if sep in val_str.lower():
            idx = val_str.lower().index(sep)
            val_str = val_str[:idx].strip()
            break

    try:
        return dateparser.parse(val_str, dayfirst=dayfirst)
    except (ValueError, OverflowError, TypeError):
        return None


def is_auto_renew_token(val: Any) -> bool:
    """Check if a value is an auto-renew token instead of a date."""
    if pd.isna(val):
        return False
    return str(val).strip().lower() in AUTO_RENEW_TOKENS


def split_date_range(val: str) -> tuple[str | None, str | None]:
    """Split a date range string like '4/15/2025 to 4/14/2026' into (start, end).

    Returns (start_str, end_str) or (val, None) if not a range.
    """
    if pd.isna(val):
        return None, None

    val_str = str(val).strip()
    for sep in _RANGE_SEPARATORS:
        if sep in val_str.lower():
            idx = val_str.lower().index(sep)
            return val_str[:idx].strip(), val_str[idx + len(sep) :].strip()

    return val_str, None


# ---------------------------------------------------------------------------
# Boolean standardization
# ---------------------------------------------------------------------------

BOOL_MAP = {
    "yes": True,
    "no": False,
    "y": True,
    "n": False,
    "true": True,
    "false": False,
    "1": True,
    "0": False,
    "checked": True,
    "unchecked": False,
    "x": True,
    "": False,
}


def parse_boolean(val: Any) -> bool | None:
    """Convert a value to boolean using common conventions.

    Returns None if the value is not a recognized boolean-like string.
    """
    if pd.isna(val):
        return None
    key = str(val).strip().lower()
    return BOOL_MAP.get(key)


def is_boolean_column(series: pd.Series) -> bool:
    """Check if all non-null values in a Series are boolean-like."""
    unique_lower = {str(v).strip().lower() for v in series.dropna().unique()}
    return bool(unique_lower) and unique_lower.issubset(BOOL_MAP.keys())


# ---------------------------------------------------------------------------
# Excel error sanitization
# ---------------------------------------------------------------------------

EXCEL_ERROR_PATTERN = re.compile(
    r"^\s*#(REF|ERROR|VALUE|DIV/0|NAME\?|NULL|N/A)!?\s*$",
    re.IGNORECASE,
)


def sanitize_excel_errors(df: pd.DataFrame) -> pd.DataFrame:
    """Replace Excel error values (#REF!, #ERROR!, etc.) with NaN.

    Only operates on object-type columns. Returns a copy.
    """
    df = df.copy()
    total_errors = 0
    for col in df.columns:
        if df[col].dtype != object:
            continue
        mask = df[col].astype(str).str.match(EXCEL_ERROR_PATTERN, na=False)
        count = mask.sum()
        if count > 0:
            df.loc[mask, col] = pd.NA
            total_errors += count
            logger.debug("Sanitized %d Excel error(s) in column '%s'.", count, col)

    if total_errors:
        logger.info("Total Excel errors sanitized: %d.", total_errors)
    return df


# ---------------------------------------------------------------------------
# Content-type heuristics (for schema mapping suggestions)
# ---------------------------------------------------------------------------

_CURRENCY_RE = re.compile(r"^[\s$£€¥₹]*[\d,.]+[kKmMbB]?\s*$")
_ID_RE = re.compile(r"^[A-Z0-9]{2,}[-_]?\d+", re.IGNORECASE)
_DATE_RE = re.compile(
    r"\d{1,4}[/\-\.]\d{1,2}[/\-\.]\d{1,4}"
    r"|[A-Za-z]{3,}\s+\d{1,2},?\s+\d{4}"
    r"|\d{1,2}\s+[A-Za-z]{3,}\s+\d{4}"
)


def sample_values(series: pd.Series, n: int = 50) -> list[str]:
    """Get a sample of non-null string values from a Series."""
    non_null = series.dropna().astype(str).str.strip()
    non_null = non_null[non_null != ""]
    if len(non_null) == 0:
        return []
    return non_null.sample(min(n, len(non_null)), random_state=42).tolist()


def detect_value_type(samples: list[str]) -> str | None:
    """Detect the dominant data pattern in a list of sample strings.

    Returns: 'date', 'currency', 'id', or None.
    """
    if not samples:
        return None

    total = len(samples)
    date_hits = sum(1 for v in samples if _DATE_RE.search(v))
    currency_hits = sum(1 for v in samples if _CURRENCY_RE.match(v))
    id_hits = sum(1 for v in samples if _ID_RE.match(v))

    if date_hits / total > 0.6:
        return "date"
    if currency_hits / total > 0.6:
        return "currency"
    if id_hits / total > 0.6:
        return "id"
    return None


# ---------------------------------------------------------------------------
# Vendor abbreviation expansion
# ---------------------------------------------------------------------------

VENDOR_ABBREVIATIONS: dict[str, str] = {
    "aws": "amazon web services",
    "gcp": "google cloud platform",
    "msft": "microsoft",
    "ms": "microsoft",
    "sf": "salesforce",
    "sfdc": "salesforce",
    "gh": "github",
    "o365": "office 365",
    "k8s": "kubernetes",
}


def expand_vendor_abbreviation(name: str) -> str:
    """Expand known vendor abbreviations for better fuzzy matching."""
    return VENDOR_ABBREVIATIONS.get(name.lower().strip(), name.lower().strip())
