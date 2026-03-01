"""
Layer 5 — Transformation Engine

Consumes raw data, mapping config, and transformation config.
Sub-stages:
  5A — Extraction: Extract mapped columns, rename to canonical, attach lineage
  5B — Row Classification: Determine dataset structure (contract/line-item/mixed)
  5C — Grouping & Aggregation: Config-driven aggregation by contract_id
  5D — Derived Fields: Compute fields from versioned rules
  5E — Transformation Logging: Persist full audit trail

All transformation logic is configuration-driven, not hardcoded.
"""

import json
import logging
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from src.layers.canonical_schema import CanonicalSchema
from src.layers.schema_mapping import MappingConfig
from src.utils import (expand_vendor_abbreviation, is_auto_renew_token,
                       is_boolean_column, parse_boolean, parse_currency,
                       parse_date, sanitize_excel_errors)

logger = logging.getLogger(__name__)


# =====================================================================
# 5A — Extraction
# =====================================================================


def extract_mapped_columns(
    sheets: list[dict],
    mapping_config: MappingConfig,
    schema: CanonicalSchema,
) -> dict[str, pd.DataFrame]:
    """Extract mapped columns from raw sheets and rename to canonical names.

    Does NOT mutate raw data. Produces a dictionary of DataFrames (one per sheet) with:
      - Only mapped columns, renamed to canonical names
      - row_lineage_id for each row
      - _source_sheet_id and _source_row_idx for traceability

    Args:
        sheets: List of sheet records from Layer 1.
        mapping_config: Confirmed mapping configuration from Layer 4.
        schema: Canonical schema from Layer 3.

    Returns:
        Dictionary mapping sheet_id to its extracted DataFrame.
    """
    sheet_lookup = {s["sheet_id"]: s for s in sheets}
    extracted_sheets: dict[str, pd.DataFrame] = {}
    sheet_mappings = mapping_config.get_sheet_mappings()

    # For each sheet, extract all mapped columns that reference it
    for sheet in sheets:
        df = sheet["dataframe"].copy()
        sheet_id = sheet["sheet_id"]
        rows: list[dict] = []

        relevant_mappings = sheet_mappings.get(sheet_id, {})

        if not relevant_mappings:
            continue

        # Sanitize Excel errors first
        df = sanitize_excel_errors(df)

        # Build extracted rows
        for idx in range(len(df)):
            row: dict = {
                "row_lineage_id": str(uuid.uuid4()),
                "_source_sheet_id": sheet_id,
                "_source_row_idx": idx,
            }

            for canonical, raw_col in relevant_mappings.items():
                if raw_col not in df.columns:
                    row[canonical] = None
                    continue

                raw_value = df.iloc[idx][raw_col]

                # Apply type-specific parsing based on schema
                field_type = schema.get_field_type(canonical)
                row[canonical] = _parse_value(raw_value, field_type, canonical)

            rows.append(row)

        if not rows:
            logger.warning("No rows extracted for sheet %s", sheet_id)
            continue

        result = pd.DataFrame(rows)

        # Apply vendor resolution if vendor column exists
        if "vendor" in result.columns:
            result = _resolve_vendors(result)

        extracted_sheets[sheet_id] = result

    total_rows = sum(len(df) for df in extracted_sheets.values())
    if not extracted_sheets:
        logger.warning(
            "No rows extracted across any sheets — check mapping configuration."
        )
        return {}

    logger.info(
        "5A Extraction: %d total rows extracted across %d sheet(s).",
        total_rows,
        len(extracted_sheets),
    )
    return extracted_sheets


def _parse_value(value, field_type: str | None, field_name: str):
    """Parse a raw value based on the expected field type."""
    if pd.isna(value):
        return None

    if field_type == "numeric":
        return parse_currency(value)
    elif field_type == "date":
        dt = parse_date(value)
        return dt.strftime("%Y-%m-%d") if dt else None
    elif field_type == "boolean":
        return parse_boolean(value)
    elif field_type == "string":
        s = str(value).strip()
        return s if s else None
    else:
        return str(value).strip() if not pd.isna(value) else None


def _resolve_vendors(df: pd.DataFrame, threshold: int = 85) -> pd.DataFrame:
    """Cluster similar vendor names and add canonical vendor column."""
    if "vendor" not in df.columns:
        return df

    unique_names = df["vendor"].dropna().unique().tolist()
    unique_names = [str(n).strip() for n in unique_names if str(n).strip()]

    if not unique_names:
        df["vendor_canonical"] = pd.NA
        return df

    clusters: list[list[str]] = []
    assigned: set[str] = set()

    for name in unique_names:
        if name in assigned:
            continue

        cluster = [name]
        assigned.add(name)
        name_exp = expand_vendor_abbreviation(name)

        for other in unique_names:
            if other in assigned:
                continue
            other_exp = expand_vendor_abbreviation(other)
            score = max(
                fuzz.token_sort_ratio(name_exp, other_exp),
                fuzz.partial_ratio(name_exp, other_exp),
            )
            if score >= threshold:
                cluster.append(other)
                assigned.add(other)

        clusters.append(cluster)

    name_to_canonical: dict[str, str] = {}
    for cluster in clusters:
        counts = Counter()
        for member in cluster:
            counts[member] = int(df["vendor"].astype(str).str.strip().eq(member).sum())
        canonical = counts.most_common(1)[0][0]
        for member in cluster:
            name_to_canonical[member] = canonical

    df["vendor_canonical"] = df["vendor"].astype(str).str.strip().map(name_to_canonical)
    return df


# =====================================================================
# 5B — Row Classification
# =====================================================================


class DatasetClassification:
    """Result of row classification analysis."""

    CONTRACT_LEVEL = "contract_level"
    LINE_ITEM_LEVEL = "line_item_level"
    INVOICE_LEVEL = "invoice_level"
    MIXED = "mixed"

    def __init__(self, level: str, details: dict):
        self.level = level
        self.details = details

    def to_dict(self) -> dict:
        return {"level": self.level, "details": self.details}


def classify_rows(
    extracted_sheets: dict[str, pd.DataFrame],
) -> dict[str, DatasetClassification]:
    """Determine if each sheet is contract-level, line-item-level, or mixed.

    Heuristics:
      - High duplicate contract_ids → likely line-item-level
      - High uniqueness of contract_ids → likely contract-level
    """
    classifications = {}

    for sheet_id, df in extracted_sheets.items():
        if "contract_id" not in df.columns or df["contract_id"].isna().all():
            classifications[sheet_id] = DatasetClassification(
                DatasetClassification.MIXED,
                {"reason": "No contract_id column found", "uniqueness_ratio": None},
            )
            continue

        valid_ids = df["contract_id"].dropna()
        if len(valid_ids) == 0:
            classifications[sheet_id] = DatasetClassification(
                DatasetClassification.MIXED,
                {"reason": "All contract_ids are null", "uniqueness_ratio": 0},
            )
            continue

        unique_ratio = valid_ids.nunique() / len(valid_ids)

        details = {
            "total_rows": len(valid_ids),
            "unique_ids": int(valid_ids.nunique()),
            "uniqueness_ratio": round(unique_ratio, 4),
        }

        if unique_ratio > 0.9:
            level = DatasetClassification.CONTRACT_LEVEL
            details["reason"] = f"High uniqueness ({unique_ratio:.1%})"
        elif unique_ratio < 0.5:
            level = DatasetClassification.LINE_ITEM_LEVEL
            details["reason"] = f"High duplication ({1-unique_ratio:.1%})"
        else:
            level = DatasetClassification.MIXED
            details["reason"] = f"Moderate uniqueness ({unique_ratio:.1%})"

        logger.info(
            "5B Classification [%s]: %s (%s)", sheet_id, level, details["reason"]
        )
        classifications[sheet_id] = DatasetClassification(level, details)

    return classifications


# =====================================================================
# 5C — Grouping & Aggregation
# =====================================================================


def aggregate(
    extracted_sheets: dict[str, pd.DataFrame],
    classifications: dict[str, DatasetClassification],
    aggregation_config: dict,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Group and aggregate data based on classification and config for each sheet.

    Returns:
        (aggregated_sheets, original_line_items)
    """
    aggregated_sheets: dict[str, pd.DataFrame] = {}
    original_line_items: dict[str, pd.DataFrame] = {}

    group_by = aggregation_config.get("group_by", ["contract_id"])
    rules = aggregation_config.get("rules", {})
    count_field = aggregation_config.get("line_item_count_field", "line_item_count")

    for sheet_id, df in extracted_sheets.items():
        classification = classifications[sheet_id]

        # Contract-level: validate uniqueness, no grouping needed
        if classification.level == DatasetClassification.CONTRACT_LEVEL:
            if "contract_id" in df.columns:
                dups = df["contract_id"].dropna().duplicated()
                if dups.any():
                    logger.warning(
                        "Sheet %s classified as contract-level but has %d duplicate contract_ids.",
                        sheet_id,
                        dups.sum(),
                    )
            logger.info(
                "5C Aggregation [%s]: contract-level, no grouping needed (%d rows).",
                sheet_id,
                len(df),
            )
            df_copy = df.copy()
            df_copy[count_field] = 1
            aggregated_sheets[sheet_id] = df_copy
            continue

        # Line-item or mixed: group and aggregate
        # Ensure group_by columns exist
        valid_group_by = [c for c in group_by if c in df.columns]
        if not valid_group_by:
            logger.warning(
                "No group-by columns found (%s) in sheet %s — returning as-is.",
                group_by,
                sheet_id,
            )
            df_copy = df.copy()
            df_copy[count_field] = 1
            aggregated_sheets[sheet_id] = df_copy
            continue

        # Preserve line items before aggregation
        original_line_items[sheet_id] = df.copy()

        # Filter to rows with valid group keys
        has_key = df[valid_group_by[0]].notna()
        for col in valid_group_by[1:]:
            has_key = has_key & df[col].notna()

        df_keyed = df[has_key].copy()
        df_orphan = df[~has_key].copy()

        if df_keyed.empty:
            logger.info(
                "5C Aggregation [%s]: no rows with valid group key — skipping.",
                sheet_id,
            )
            df_copy = df.copy()
            df_copy[count_field] = 1
            aggregated_sheets[sheet_id] = df_copy
            continue

        pre_count = len(df_keyed)

        # Build pandas-compatible aggregation rules
        agg_rules_pd: dict = {}
        meta_cols = {"row_lineage_id", "_source_sheet_id", "_source_row_idx"}

        for col in df_keyed.columns:
            if col in set(valid_group_by) or col in meta_cols:
                continue

            if col in rules:
                method = rules[col]["method"]
                agg_rules_pd[col] = _get_agg_func(method)
            elif pd.api.types.is_numeric_dtype(df_keyed[col]):
                agg_rules_pd[col] = "sum"
            else:
                agg_rules_pd[col] = "first"

        # Add line item count
        agg_rules_pd[count_field] = "size"
        df_keyed[count_field] = 1

        aggregated = (
            df_keyed.groupby(valid_group_by, dropna=False)
            .agg(agg_rules_pd)
            .reset_index()
        )

        # Re-attach orphan rows
        if not df_orphan.empty:
            df_orphan[count_field] = 1
            aggregated = pd.concat(
                [aggregated, df_orphan], ignore_index=True, sort=False
            )

        logger.info(
            "5C Aggregation [%s]: %d rows → %d contracts (grouped by %s).",
            sheet_id,
            pre_count,
            len(aggregated),
            valid_group_by,
        )
        aggregated_sheets[sheet_id] = aggregated

    return aggregated_sheets, original_line_items


def _get_agg_func(method: str):
    """Convert a config method name to a pandas aggregation function."""
    if method == "sum":
        return "sum"
    elif method == "min":
        return "min"
    elif method == "max":
        return "max"
    elif method == "mean":
        return "mean"
    elif method == "first":
        return "first"
    elif method == "count":
        return "size"
    elif method == "collect_unique":
        return lambda x: " | ".join(
            str(v) for v in x.dropna().unique() if str(v).strip()
        )
    else:
        logger.warning("Unknown aggregation method '%s' — using 'first'.", method)
        return "first"


def join_entities(
    aggregated_sheets: dict[str, pd.DataFrame],
    primary_sheet_id: str | None = None,
    preferred_join_keys: list[str] = ["account_id", "contract_id"],
) -> pd.DataFrame:
    """Join multiple processed sheets into a single flat DataFrame.

    If there's only one sheet, returns it.
    If multiple sheets, uses the primary_sheet_id as the base table, and left-joins
    other sheets onto it using the first available shared key from preferred_join_keys.
    """
    if not aggregated_sheets:
        return pd.DataFrame()

    sheet_ids = list(aggregated_sheets.keys())
    if len(sheet_ids) == 1:
        return list(aggregated_sheets.values())[0]

    # Determine base table
    if primary_sheet_id and primary_sheet_id in aggregated_sheets:
        base_id = primary_sheet_id
    else:
        # Default to the largest sheet as the primary if none specified
        base_id = max(aggregated_sheets.items(), key=lambda item: len(item[1]))[0]

    base_df = aggregated_sheets[base_id].copy()
    logger.info(
        "5C Join: Using sheet '%s' as primary base table (%d rows).",
        base_id,
        len(base_df),
    )

    # Left join other sheets onto the base
    for sheet_id, df in aggregated_sheets.items():
        if sheet_id == base_id:
            continue

        join_key = next(
            (
                k
                for k in preferred_join_keys
                if k in base_df.columns and k in df.columns
            ),
            None,
        )

        if not join_key:
            logger.warning(
                "5C Join: No shared keys %s found to join sheet '%s' to base table. Concatenating instead.",
                preferred_join_keys,
                sheet_id,
            )
            base_df = pd.concat([base_df, df], ignore_index=True, sort=False)
            continue

        logger.info(
            "5C Join: Left-joining sheet '%s' onto base using key '%s'.",
            sheet_id,
            join_key,
        )

        # Drop metadata columns from the joined sheet to avoid duplicating traceability
        cols_to_drop = {"row_lineage_id", "_source_sheet_id", "_source_row_idx"}
        df_to_join = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

        # Suffix matching columns that aren't the join key
        overlapping_cols = set(base_df.columns) & set(df_to_join.columns) - {join_key}
        if overlapping_cols:
            suffix = f"_{sheet_id[:4]}"
            df_to_join = df_to_join.rename(
                columns={c: f"{c}{suffix}" for c in overlapping_cols}
            )

        base_df = pd.merge(base_df, df_to_join, on=join_key, how="left")

    return base_df


# =====================================================================
# 5D — Derived Fields
# =====================================================================


def compute_derived_fields(df: pd.DataFrame, derived_config: dict) -> pd.DataFrame:
    """Compute derived fields based on versioned configuration rules.

    All derived rules are deterministic and testable.
    """
    df = df.copy()
    fields = derived_config.get("fields", {})

    for field_name, rule in fields.items():
        op = rule.get("operation")

        try:
            if op == "date_diff_days":
                df[field_name] = _derive_date_diff(df, rule)
            elif op == "divide":
                df[field_name] = _derive_divide(df, rule)
            elif op == "safe_divide":
                df[field_name] = _derive_safe_divide(df, rule)
            elif op == "boolean_expression":
                df[field_name] = _derive_boolean_expr(df, rule)
            else:
                logger.warning(
                    "Unknown derived field operation '%s' for '%s'.", op, field_name
                )
        except Exception as exc:
            logger.warning("Failed to compute derived field '%s': %s", field_name, exc)
            df[field_name] = pd.NA

    computed = [f for f in fields if f in df.columns]
    logger.info("5D Derived fields: computed %d fields: %s", len(computed), computed)
    return df


def _derive_date_diff(df: pd.DataFrame, rule: dict) -> pd.Series:
    """Compute difference in days between two date columns."""
    start_col = rule["start"]
    end_col = rule["end"]

    if start_col not in df.columns or end_col not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index)

    start = pd.to_datetime(df[start_col], errors="coerce")
    end = pd.to_datetime(df[end_col], errors="coerce")
    return (end - start).dt.days


def _derive_divide(df: pd.DataFrame, rule: dict) -> pd.Series:
    """Divide numerator by denominator (can be a column name or constant)."""
    numerator = rule["numerator"]
    denominator = rule["denominator"]

    num = (
        df[numerator]
        if isinstance(numerator, str) and numerator in df.columns
        else numerator
    )
    den = (
        df[denominator]
        if isinstance(denominator, str) and denominator in df.columns
        else denominator
    )

    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce") if isinstance(den, pd.Series) else den

    with np.errstate(divide="ignore", invalid="ignore"):
        result = num / den
    return result.round(4)


def _derive_safe_divide(df: pd.DataFrame, rule: dict) -> pd.Series:
    """Divide with fallback for zero/null denominators."""
    result = _derive_divide(df, rule)
    fallback = rule.get("fallback")
    result = result.replace([np.inf, -np.inf], fallback)
    return result


def _derive_boolean_expr(df: pd.DataFrame, rule: dict) -> pd.Series:
    """Evaluate a simple boolean expression."""
    expr = rule.get("expression", "")

    if "end_date < today AND auto_renew != true" in expr:
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        end = pd.to_datetime(df.get("end_date"), errors="coerce")
        auto = df.get("auto_renew", pd.Series([False] * len(df)))
        return (end < today) & (auto != True)

    if "auto_renew == true OR renewal_date IS NOT NULL" in expr:
        auto = df.get("auto_renew", pd.Series([False] * len(df)))
        renewal = df.get("renewal_date")
        has_renewal = (
            renewal.notna() if renewal is not None else pd.Series([False] * len(df))
        )
        return (auto == True) | has_renewal

    logger.warning("Unsupported boolean expression: %s", expr)
    return pd.Series([pd.NA] * len(df), index=df.index)


# =====================================================================
# 5E — Transformation Logging
# =====================================================================


def log_transformation(
    dataset_id: str,
    mapping_version: str,
    schema_version: str,
    aggregation_config_version: str,
    derived_config_version: str,
    classification: DatasetClassification,
    rows_before: int,
    rows_after: int,
    validation_status: str = "pending",
    artifacts_dir: str = "artifacts",
) -> dict:
    """Create and persist a transformation log entry.

    Returns the log dict.
    """
    log = {
        "dataset_id": dataset_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": str(uuid.uuid4()),
        "versions": {
            "schema": schema_version,
            "mapping": mapping_version,
            "aggregation_rules": aggregation_config_version,
            "derived_fields": derived_config_version,
        },
        "classification": classification.to_dict(),
        "row_counts": {
            "before_aggregation": rows_before,
            "after_aggregation": rows_after,
        },
        "validation_status": validation_status,
    }

    # Persist
    log_dir = Path(artifacts_dir) / dataset_id
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "transform_log.json"

    # Append to existing log or create new
    logs = []
    if log_path.exists():
        with open(log_path) as f:
            logs = json.load(f)
            if not isinstance(logs, list):
                logs = [logs]

    logs.append(log)
    with open(log_path, "w") as f:
        json.dump(logs, f, indent=2, default=str)

    logger.info(
        "5E Transform log: run=%s, %d→%d rows, classification=%s",
        log["run_id"][:8],
        rows_before,
        rows_after,
        classification.level,
    )
    return log
