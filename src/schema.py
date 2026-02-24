"""
schema.py — Fuzzy header matching & canonical schema mapping.

Maps messy column names (e.g. "Annual_Cost", "Contract_Value") to a
standardized set of canonical column names using:
  1. Exact alias lookup
  2. RapidFuzz fuzzy header matching
  3. Content-based heuristic detection (inspects actual cell values)
"""

import re
import logging

import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical schema: canonical_name -> list of known aliases (lowercase)
# ---------------------------------------------------------------------------
CANONICAL_SCHEMA: dict[str, list[str]] = {
    # ── Identity ──────────────────────────────────────────────────────
    "Contract_ID": [
        "contract_id", "contract id", "contractid", "contract #", "contract#",
        "contract number", "contract_number", "contract no", "contract_no",
        "quote_line_id", "quote line id", "quotelineid",
        "quote_id", "quote id", "quoteid", "quote #", "quote#",
        "quote number", "quote_number", "quote no",
        "deal_id", "deal id", "dealid", "deal #", "deal#",
        "deal number", "deal_number",
        "opportunity_id", "opportunity id", "opp_id", "opp id",
        "opp #", "opp#", "opportunity #", "opportunity#",
        "opportunity number", "opportunity_number",
        "opportunity name", "opportunity_name",
        "order_id", "order id", "order #", "order#",
        "order number", "order_number",
        "po_number", "po number", "po #", "po#",
        "purchase_order", "purchase order",
        "subscription_id", "subscription id",
        "agreement_id", "agreement id", "agreement #",
        "reference", "ref", "ref #", "ref#",
        "record_id", "record id",
    ],
    "Account_ID": [
        "account_id", "account id", "accountid",
        "account #", "account#", "account number", "account_number",
        "customer_id", "customer id", "customerid",
        "client_id", "client id",
        "org_id", "org id",
    ],
    "Contract_Close_Date": [
        "contract_close_date", "contract close date",
        "close_date", "close date", "closedate", "closed_date", "closed date",
        "booking_date", "booking date", "booked_date", "booked date",
        "win_date", "win date", "won_date", "won date",
        "created_date", "created date", "creation_date", "creation date",
        "signed_date", "signed date", "signature_date", "signature date",
        "executed_date", "executed date", "execution_date", "execution date",
        "order_date", "order date", "purchase_date", "purchase date",
    ],

    # ── Vendor / Customer ─────────────────────────────────────────────
    "Vendor": [
        "vendor", "vendor_name", "vendor name",
        "supplier", "supplier_name", "supplier name",
        "company", "company_name", "company name",
        "provider", "provider_name", "provider name",
        "partner", "partner_name", "partner name",
        "account name", "account_name",
        "manufacturer", "manufacturer_name",
        "publisher", "publisher_name",
        "reseller", "reseller_name",
        "customer", "customer_name", "customer name",
        "client", "client_name", "client name",
        "entity", "entity_name",
        "org", "organization", "organisation",
    ],

    # ── Financials ────────────────────────────────────────────────────
    "ACV": [
        "acv", "annual_contract_value", "annual contract value",
        "annual_cost", "annual cost", "annual_spend", "annual spend",
        "annual_value", "annual value",
        "contract_value", "contract value",
        "arr", "annual_recurring_revenue", "annual recurring revenue",
        "subscription_value", "subscription value",
        "annual_fee", "annual fee",
        "yearly_cost", "yearly cost", "yearly_value", "yearly value",
        "booking", "bookings",
    ],
    "Net_ARR": [
        "net arr", "net_arr", "net_price", "net price",
        "net_amount", "net amount",
    ],
    "Quote_ARR": [
        "quote arr", "quote_arr",
        "quote line arr", "quote_line_arr",
    ],
    "Expiring_ARR": [
        "expiring arr", "expiring_arr",
        "expiring_revenue", "expiring revenue",
    ],
    "Growth_ARR": [
        "growth arr", "growth_arr",
        "growth_revenue", "growth revenue",
        "expansion arr", "expansion_arr",
    ],
    "Customer_Amount": [
        "customer amount", "customer_amount",
        "customer total", "customer_total",
    ],
    "Total_Value": [
        "total_value", "total value", "totalvalue",
        "total_contract_value", "total contract value",
        "tcv", "total_cost", "total cost",
        "deal_value", "deal value",
        "contract_amount", "contract amount",
        "total_amount", "total amount",
        "total_price", "total price",
        "grand_total", "grand total",
        "lifetime_value", "lifetime value",
        "ltv", "total_revenue", "total revenue",
        "amount", "value", "cost", "price",
        "revenue", "spend", "total_spend",
        "extended_price", "extended price",
        "line_amount", "line amount",
        "sell_price", "sell price",
        "list_price", "list price", "unit_price", "unit price",
    ],
    "Term_Months": [
        "term_months", "term months", "term (months)",
        "contract_term", "contract term",
        "duration_months", "duration months",
        "length_months", "length months",
        "term", "duration", "contract_length", "contract length",
        "subscription_term", "subscription term",
        "period", "contract_period", "contract period",
        "months", "# months", "num_months",
    ],
    "Currency": [
        "currency", "currency_code", "currency code",
        "curr", "ccy", "iso_currency",
    ],
    "Quantity": [
        "quantity", "qty", "units", "seats", "licenses",
        "licence_count", "license_count",
        "user_count", "user count", "users",
        "count", "num_licenses", "num_seats",
    ],
    "Unit_Price": [
        "customer unit price", "customer_unit_price",
        "unit price", "unit_price", "price per unit",
        "ppu", "price_per_unit",
    ],
    "Discount": [
        "discount", "discount_pct", "discount %", "discount_percent",
        "rebate", "markdown",
    ],

    # ── Dates ─────────────────────────────────────────────────────────
    "Effective_Date": [
        "effective_date", "effective date", "effectivedate",
        "start_date", "start date", "startdate",
        "contract_start", "contract start",
        "commencement_date", "commencement date",
        "begin_date", "begin date", "from_date", "from date",
        "service_start", "service start",
        "subscription_start", "subscription start",
        "activation_date", "activation date",
    ],
    "Renewal_Date": [
        "renewal_date", "renewal date", "renewaldate",
        "next_renewal", "next renewal",
        "renewal_due", "renewal due",
        "auto_renewal_date", "auto renewal date",
        "renewal", "next_renewal_date",
    ],
    "Expiry_Date": [
        "expiry_date", "expiry date", "expirydate",
        "expiration_date", "expiration date",
        "end_date", "end date", "enddate",
        "contract_end", "contract end",
        "termination_date", "termination date",
        "to_date", "to date", "thru_date", "thru date",
        "service_end", "service end",
        "subscription_end", "subscription end",
        "cancellation_date", "cancellation date",
    ],

    # ── Booleans ──────────────────────────────────────────────────────
    "Auto_Renew": [
        "auto_renew", "auto renew", "autorenew",
        "auto_renewal", "auto renewal",
        "is_auto_renew", "is auto renew",
        "auto-renew", "auto-renewal",
    ],
    "Close_Year": [
        "close year", "close_year", "fiscal year", "fiscal_year",
        "year", "contract year",
    ],
    "Fiscal_Period": [
        "fiscal period", "fiscal_period", "period", "fiscal quarter",
        "fiscal_quarter", "quarter",
    ],
    "Account_Segment": [
        "account segment", "account_segment", "segment",
        "market segment", "customer segment",
    ],

    # ── Descriptive ───────────────────────────────────────────────────
    "Product": [
        "product", "product_name", "product name",
        "product family", "product_family",
        "service", "service_name", "service name",
        "sku", "sku_name", "sku name",
        "line_item", "line item", "item", "item_name",
        "offering", "solution", "software",
        "product_description", "product description",
        "product_line", "product line",
        "module", "component", "feature",
        "plan", "edition",
    ],
    "Department": [
        "department", "dept", "business_unit", "business unit",
        "cost_center", "cost center", "cost_centre",
        "team", "group", "division", "segment",
        "bu", "unit", "org_unit",
    ],
    "Owner": [
        "owner", "contract_owner", "contract owner",
        "opportunity owner", "opportunity_owner",
        "account_manager", "account manager",
        "buyer", "purchaser", "rep",
        "contact", "primary_contact", "primary contact",
        "sales_rep", "sales rep", "salesperson",
        "account_executive", "account executive",
        "ae", "csm", "customer_success",
        "assigned_to", "assigned to",
        "requestor", "requester",
    ],
    "Status": [
        "status", "contract_status", "contract status",
        "state", "stage", "deal_stage", "deal stage",
        "lifecycle", "phase",
        "active", "is_active",
    ],
    "Opportunity_Type": [
        "opportunity type", "opportunity_type",
        "type", "deal type", "deal_type",
        "transaction type", "transaction_type",
    ],
    "Notes": [
        "notes", "comments", "remarks", "description",
        "memo", "additional_info", "additional info",
        "internal_notes", "internal notes",
    ],
    "Category": [
        "category", "type", "contract_type", "contract type",
        "classification", "class", "tier",
        "subcategory", "sub_category",
        "license_type", "license type",
        "agreement_type", "agreement type",
    ],
}

# Build a flat lookup: alias -> canonical_name
_ALIAS_LOOKUP: dict[str, str] = {}
_ALL_ALIASES: list[str] = []
for canonical, aliases in CANONICAL_SCHEMA.items():
    for alias in aliases:
        _ALIAS_LOOKUP[alias] = canonical
    _ALL_ALIASES.extend(aliases)


# ---------------------------------------------------------------------------
# Content-based heuristic patterns
# ---------------------------------------------------------------------------
_CURRENCY_RE = re.compile(r"^[\s$£€¥₹]*[\d,.]+[kKmMbB]?\s*$")
_ID_RE = re.compile(r"^[A-Z0-9]{2,}[-_]?\d+", re.IGNORECASE)
_DATE_RE = re.compile(
    r"\d{1,4}[/\-\.]\d{1,2}[/\-\.]\d{1,4}"        # 01/15/2024, 2024-01-15
    r"|[A-Za-z]{3,}\s+\d{1,2},?\s+\d{4}"           # Jan 15, 2024
    r"|\d{1,2}\s+[A-Za-z]{3,}\s+\d{4}"             # 15 Jan 2024
)


def _sample_values(series: pd.Series, n: int = 50) -> list[str]:
    """Get a sample of non-null string values from a Series."""
    non_null = series.dropna().astype(str).str.strip()
    non_null = non_null[non_null != ""]
    if len(non_null) == 0:
        return []
    return non_null.sample(min(n, len(non_null)), random_state=42).tolist()


def _detect_column_type(series: pd.Series, col_name: str) -> str | None:
    """Inspect cell values to guess what canonical type a column might be.

    Returns a canonical name if confident, or None if unsure.
    """
    samples = _sample_values(series)
    if not samples:
        return None

    col_lower = col_name.strip().lower()

    # Count pattern matches
    date_hits = sum(1 for v in samples if _DATE_RE.search(v))
    currency_hits = sum(1 for v in samples if _CURRENCY_RE.match(v))
    id_hits = sum(1 for v in samples if _ID_RE.match(v))
    total = len(samples)

    if total == 0:
        return None

    # Strong date signal (>60% of values look like dates)
    if date_hits / total > 0.6:
        # Try to determine which date column
        if any(kw in col_lower for kw in ["close", "win", "won", "book", "sign", "exec", "creat", "order"]):
            return "Contract_Close_Date"
        elif any(kw in col_lower for kw in ["start", "begin", "effect", "from", "commence", "activ"]):
            return "Effective_Date"
        elif any(kw in col_lower for kw in ["end", "expir", "termin", "thru", "through", "to "]):
            return "Expiry_Date"
        elif any(kw in col_lower for kw in ["renew"]):
            return "Renewal_Date"
        # Generic date — skip, already handled by date normalization
        return None

    # Strong currency/financial signal (>60% look like money)
    if currency_hits / total > 0.6:
        if any(kw in col_lower for kw in ["total", "tcv", "grand", "lifetime", "ltv"]):
            return "Total_Value"
        return "ACV"

    # Strong ID signal (>60% look like IDs) and column name hints
    if id_hits / total > 0.6:
        if any(kw in col_lower for kw in ["id", "#", "number", "num", "no", "ref", "code",
                                           "opp", "deal", "quote", "order", "contract",
                                           "agreement", "po", "subscription"]):
            return "Contract_ID"

    return None


# ---------------------------------------------------------------------------
# Fuzzy match validation (reject semantic mismatches)
# ---------------------------------------------------------------------------
# Words that indicate financial data — should NOT map to IDs
_FINANCIAL_WORDS = {"arr", "amount", "cost", "price", "revenue", "total", "fee",
                    "spend", "value", "discount", "margin", "ppu", "rate"}
# Words that indicate IDs — should NOT map to financials
_ID_WORDS = {"id", "number", "no", "#", "name", "code", "ref", "reference"}


def _validate_fuzzy_match(col_lower: str, alias: str, canonical: str, score: int) -> bool:
    """Validate that a fuzzy match is semantically reasonable.

    Prevents nonsensical matches like 'Quote Line ARR' → Contract_ID
    by checking that financial keywords don't map to ID categories.
    """
    col_words = set(col_lower.replace("_", " ").split())

    # Block financial words from mapping to ID columns
    if canonical in {"Contract_ID"}:
        if col_words & _FINANCIAL_WORDS:
            return False

    # Block ID words from mapping to financial columns
    if canonical in {"ACV", "Total_Value"}:
        if col_words & _ID_WORDS and not col_words & _FINANCIAL_WORDS:
            return False

    # For low-confidence matches (80-89), require at least one meaningful word overlap
    if score < 90:
        alias_words = set(alias.replace("_", " ").split())
        overlap = col_words & alias_words
        # Filter out very short words (noise)
        meaningful_overlap = {w for w in overlap if len(w) > 2}
        if not meaningful_overlap:
            return False

    return True


# ---------------------------------------------------------------------------
# Main mapping function
# ---------------------------------------------------------------------------
def fuzzy_map_headers(
    df: pd.DataFrame,
    threshold: int = 80,
) -> tuple[pd.DataFrame, list[str]]:
    """Rename DataFrame columns to canonical names using fuzzy matching.

    Uses a three-step approach:
      1. Exact alias lookup
      2. Fuzzy matching against all aliases (RapidFuzz)
      3. Content-based detection for still-unmapped columns

    Args:
        df: Input DataFrame with raw column names.
        threshold: Minimum fuzzy score (0-100) to accept a match.

    Returns:
        (renamed DataFrame, list of unmapped column names)
    """
    rename_map: dict[str, str] = {}
    unmapped: list[str] = []
    meta_cols = {"meta_source_tab", "meta_source_file"}
    mapped_canonicals: set[str] = set()  # Track what's been mapped

    # --- Pass 1 & 2: Header-name-based matching ---
    still_unmapped: list[str] = []

    for col in df.columns:
        if col in meta_cols:
            continue

        col_lower = col.strip().lower()

        # 1. Try exact alias lookup first
        if col_lower in _ALIAS_LOOKUP:
            canonical = _ALIAS_LOOKUP[col_lower]
            rename_map[col] = canonical
            mapped_canonicals.add(canonical)
            logger.debug("Exact match: '%s' → '%s'", col, canonical)
            continue

        # 2. Fuzzy match against all aliases
        match = process.extractOne(
            col_lower,
            _ALL_ALIASES,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if match:
            matched_alias, score, _ = match
            canonical = _ALIAS_LOOKUP[matched_alias]
            # Validate: reject if the match is semantically wrong
            if _validate_fuzzy_match(col_lower, matched_alias, canonical, score):
                rename_map[col] = canonical
                mapped_canonicals.add(canonical)
                logger.info("Fuzzy match: '%s' → '%s' (score=%d via alias '%s')", col, canonical, score, matched_alias)
            else:
                logger.info("Rejected fuzzy match: '%s' → '%s' (score=%d) — semantics mismatch.", col, canonical, score)
                still_unmapped.append(col)
        else:
            still_unmapped.append(col)

    # --- Pass 3: Content-based detection for remaining unmapped columns ---
    for col in still_unmapped:
        detected = _detect_column_type(df[col], col)
        if detected and detected not in mapped_canonicals:
            rename_map[col] = detected
            mapped_canonicals.add(detected)
            logger.info("Content-detected: '%s' → '%s' (based on cell values)", col, detected)
        else:
            unmapped.append(col)
            logger.warning("Unmapped column: '%s' — no header or content match.", col)

    # Handle duplicate canonical names after rename (e.g. two cols both map to ACV)
    seen: dict[str, int] = {}
    for orig, canon in list(rename_map.items()):
        if canon in seen:
            seen[canon] += 1
            new_name = f"{canon}_{seen[canon]}"
            rename_map[orig] = new_name
            logger.warning("Duplicate canonical '%s' — renaming '%s' to '%s'.", canon, orig, new_name)
        else:
            seen[canon] = 0

    df = df.rename(columns=rename_map)
    return df, unmapped
