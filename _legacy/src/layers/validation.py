"""
Layer 6 — Validation & Integrity

Enforce structural, logical, and statistical checks before ML consumption.
All checks are configuration-driven via validation_rules_v{N}.json.

If validation fails, the dataset is flagged and ML ingestion is blocked.
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class ValidationResult:
    """The outcome of a full validation run."""

    def __init__(self):
        self.passed = True
        self.errors: list[dict] = []
        self.warnings: list[dict] = []
        self.info: list[dict] = []

    def add_issue(
        self, severity: str, check_type: str, message: str, details: dict | None = None
    ):
        issue = {"type": check_type, "message": message, "details": details or {}}
        if severity == "error":
            self.errors.append(issue)
            self.passed = False
        elif severity == "warning":
            self.warnings.append(issue)
        else:
            self.info.append(issue)

    @property
    def total_issues(self) -> int:
        return len(self.errors) + len(self.warnings) + len(self.info)

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "summary": {
                "error_count": len(self.errors),
                "warning_count": len(self.warnings),
                "info_count": len(self.info),
            },
        }


def validate(
    df: pd.DataFrame,
    validation_config: dict,
    line_items_df: pd.DataFrame | None = None,
) -> ValidationResult:
    """Run all configured validation checks.

    Args:
        df: The transformed/aggregated DataFrame.
        validation_config: Loaded from validation_rules_v{N}.json.
        line_items_df: Optional original line items for aggregation checksums.

    Returns:
        ValidationResult with all issues found.
    """
    result = ValidationResult()

    # Structural checks
    structural = validation_config.get("structural", {})
    _check_required_fields(df, structural, result)
    _check_nulls_in_required(df, structural, result)
    _check_duplicate_contract_id(df, structural, result)
    _check_invalid_numeric(df, structural, result)

    # Logical checks
    logical = validation_config.get("logical", {})
    _check_end_gte_start(df, logical, result)
    _check_aggregation_totals(df, line_items_df, logical, result)
    _check_zero_duration(df, logical, result)
    _check_negative_financials(df, logical, result)

    # Statistical checks
    statistical = validation_config.get("statistical", {})
    _check_outliers(df, statistical, result)
    _check_extreme_duration(df, statistical, result)
    _check_future_dates(df, statistical, result)

    logger.info(
        "Layer 6 Validation: %s (%d errors, %d warnings, %d info)",
        "PASSED" if result.passed else "FAILED",
        len(result.errors),
        len(result.warnings),
        len(result.info),
    )
    return result


def persist_validation_result(
    result: ValidationResult,
    artifacts_dir: str,
    dataset_id: str,
) -> None:
    """Persist validation results to the artifacts directory."""
    val_dir = Path(artifacts_dir) / dataset_id
    val_dir.mkdir(parents=True, exist_ok=True)

    path = val_dir / "validation_result.json"
    with open(path, "w") as f:
        json.dump(result.to_dict(), f, indent=2, default=str)

    logger.info("Persisted validation result → %s", path)


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------


def _check_required_fields(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Check that all required canonical fields are present as columns."""
    check = config.get("required_fields_present", {})
    if not check.get("enabled", True):
        return

    # Required fields based on schema (hardcoded minimum set)
    required = {"contract_id", "total_value", "start_date", "end_date"}
    missing = required - set(df.columns)

    if missing:
        result.add_issue(
            check.get("severity", "error"),
            "required_fields_missing",
            f"Missing required columns: {sorted(missing)}",
            {"missing": sorted(missing)},
        )


def _check_nulls_in_required(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Check for null values in required fields."""
    check = config.get("no_nulls_in_required", {})
    if not check.get("enabled", True):
        return

    fields = check.get(
        "fields", ["contract_id", "total_value", "start_date", "end_date"]
    )
    for field in fields:
        if field in df.columns:
            null_count = int(df[field].isna().sum())
            if null_count > 0:
                result.add_issue(
                    check.get("severity", "error"),
                    "null_in_required",
                    f"'{field}' has {null_count} null values ({null_count}/{len(df)} rows)",
                    {"field": field, "null_count": null_count, "total_rows": len(df)},
                )


def _check_duplicate_contract_id(
    df: pd.DataFrame, config: dict, result: ValidationResult
):
    """Check for duplicate contract_id after aggregation."""
    check = config.get("no_duplicate_contract_id", {})
    if not check.get("enabled", True):
        return

    if "contract_id" not in df.columns:
        return

    valid_ids = df["contract_id"].dropna()
    dup_count = int(valid_ids.duplicated().sum())

    if dup_count > 0:
        result.add_issue(
            check.get("severity", "error"),
            "duplicate_contract_id",
            f"{dup_count} duplicate contract_id values found after aggregation",
            {"duplicate_count": dup_count},
        )


def _check_invalid_numeric(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Check that numeric fields contain valid numeric values."""
    check = config.get("no_invalid_numeric", {})
    if not check.get("enabled", True):
        return

    numeric_fields = [
        "total_value",
        "acv",
        "net_arr",
        "unit_price",
        "quantity",
        "discount",
        "term_months",
    ]

    for field in numeric_fields:
        if field in df.columns:
            non_null = df[field].dropna()
            if len(non_null) > 0:
                numeric = pd.to_numeric(non_null, errors="coerce")
                invalid = numeric.isna().sum() - non_null.isna().sum()
                if invalid > 0:
                    result.add_issue(
                        check.get("severity", "warning"),
                        "invalid_numeric",
                        f"'{field}' has {invalid} non-numeric values",
                        {"field": field, "invalid_count": int(invalid)},
                    )


# ---------------------------------------------------------------------------
# Logical checks
# ---------------------------------------------------------------------------


def _check_end_gte_start(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Check that end_date >= start_date."""
    check = config.get("end_gte_start", {})
    if not check.get("enabled", True):
        return

    start_col = check.get("start_field", "start_date")
    end_col = check.get("end_field", "end_date")

    if start_col not in df.columns or end_col not in df.columns:
        return

    start = pd.to_datetime(df[start_col], errors="coerce")
    end = pd.to_datetime(df[end_col], errors="coerce")
    mask = (start.notna() & end.notna()) & (end < start)
    count = int(mask.sum())

    if count > 0:
        result.add_issue(
            check.get("severity", "error"),
            "end_before_start",
            f"{count} rows have end_date before start_date",
            {"count": count},
        )


def _check_aggregation_totals(
    df: pd.DataFrame,
    line_items: pd.DataFrame | None,
    config: dict,
    result: ValidationResult,
):
    """Verify that aggregated totals match sum of source line items."""
    check = config.get("aggregation_totals_match", {})
    if not check.get("enabled", True) or line_items is None:
        return

    if "contract_id" not in df.columns or "total_value" not in df.columns:
        return
    if (
        "contract_id" not in line_items.columns
        or "total_value" not in line_items.columns
    ):
        return

    # Sum line items per contract
    li_totals = line_items.groupby("contract_id")["total_value"].apply(
        lambda x: pd.to_numeric(x, errors="coerce").sum()
    )

    mismatches = 0
    for cid in df["contract_id"].dropna().unique():
        agg_val = pd.to_numeric(
            df.loc[df["contract_id"] == cid, "total_value"], errors="coerce"
        ).sum()
        li_val = li_totals.get(cid, 0)
        if abs(agg_val - li_val) > 0.01:
            mismatches += 1

    if mismatches > 0:
        result.add_issue(
            check.get("severity", "warning"),
            "aggregation_mismatch",
            f"{mismatches} contracts have aggregated totals that don't match line-item sums",
            {"mismatch_count": mismatches},
        )


def _check_zero_duration(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Check for zero-duration contracts."""
    check = config.get("no_zero_duration", {})
    if not check.get("enabled", True):
        return

    if "contract_duration_days" in df.columns:
        zeros = int(
            (pd.to_numeric(df["contract_duration_days"], errors="coerce") == 0).sum()
        )
    elif "start_date" in df.columns and "end_date" in df.columns:
        start = pd.to_datetime(df["start_date"], errors="coerce")
        end = pd.to_datetime(df["end_date"], errors="coerce")
        zeros = int(((end - start).dt.days == 0).sum())
    else:
        return

    allow_zero = check.get("allow_zero", False)
    if zeros > 0 and not allow_zero:
        result.add_issue(
            check.get("severity", "warning"),
            "zero_duration",
            f"{zeros} contracts have zero duration",
            {"count": zeros},
        )


def _check_negative_financials(
    df: pd.DataFrame, config: dict, result: ValidationResult
):
    """Check for negative financial values."""
    check = config.get("no_negative_financials", {})
    if not check.get("enabled", True):
        return

    fields = check.get("fields", ["total_value", "acv", "net_arr", "unit_price"])
    for field in fields:
        if field in df.columns:
            numeric = pd.to_numeric(df[field], errors="coerce")
            neg_count = int((numeric < 0).sum())
            if neg_count > 0:
                result.add_issue(
                    check.get("severity", "warning"),
                    "negative_financial",
                    f"'{field}' has {neg_count} negative values",
                    {"field": field, "count": neg_count},
                )


# ---------------------------------------------------------------------------
# Statistical checks
# ---------------------------------------------------------------------------


def _check_outliers(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Detect statistical outliers in financial fields."""
    check = config.get("outlier_detection", {})
    if not check.get("enabled", True):
        return

    std_threshold = check.get("std_threshold", 3.0)
    min_samples = check.get("min_sample_size", 10)
    fields = check.get("fields", ["total_value", "acv", "net_arr"])

    for field in fields:
        if field not in df.columns:
            continue

        numeric = pd.to_numeric(df[field], errors="coerce").dropna()
        if len(numeric) < min_samples:
            continue

        mean = numeric.mean()
        std = numeric.std()
        if std == 0:
            continue

        outlier_mask = (numeric - mean).abs() > std_threshold * std
        count = int(outlier_mask.sum())

        if count > 0:
            result.add_issue(
                check.get("severity", "warning"),
                "outlier",
                f"'{field}': {count} values > {std_threshold}σ from mean (μ={mean:,.0f}, σ={std:,.0f})",
                {
                    "field": field,
                    "count": count,
                    "mean": float(mean),
                    "std": float(std),
                },
            )


def _check_extreme_duration(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Detect contracts with extreme durations."""
    check = config.get("extreme_duration", {})
    if not check.get("enabled", True):
        return

    max_months = check.get("max_months", 120)

    if "term_months" in df.columns:
        numeric = pd.to_numeric(df["term_months"], errors="coerce")
        count = int((numeric > max_months).sum())
    elif "contract_duration_days" in df.columns:
        days = pd.to_numeric(df["contract_duration_days"], errors="coerce")
        count = int((days > max_months * 30).sum())
    else:
        return

    if count > 0:
        result.add_issue(
            check.get("severity", "warning"),
            "extreme_duration",
            f"{count} contracts exceed {max_months} months",
            {"count": count, "max_months": max_months},
        )


def _check_future_dates(df: pd.DataFrame, config: dict, result: ValidationResult):
    """Detect dates far in the future."""
    check = config.get("future_date_detection", {})
    if not check.get("enabled", True):
        return

    max_years = check.get("max_years_ahead", 1)
    fields = check.get("fields", ["close_date"])
    cutoff = pd.Timestamp.now() + pd.DateOffset(years=max_years)

    for field in fields:
        if field not in df.columns:
            continue

        dates = pd.to_datetime(df[field], errors="coerce")
        count = int((dates > cutoff).sum())

        if count > 0:
            result.add_issue(
                check.get("severity", "info"),
                "future_date",
                f"'{field}': {count} values more than {max_years} year(s) in the future",
                {"field": field, "count": count},
            )
