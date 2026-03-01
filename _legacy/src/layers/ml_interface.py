"""
Layer 8 — ML Interface Contract

Define and enforce the explicit ML input contract.
ML must never access raw or partially transformed tables.

Responsibilities:
  - Define required columns, expected dtypes, null handling, scaling assumptions
  - Enforce schema validation before ML execution
  - Provide a clean DataFrame for ML consumption
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ML Input Contract Definition
# ---------------------------------------------------------------------------

ML_CONTRACT = {
    "required_columns": [
        "contract_id",
        "total_value",
        "start_date",
        "end_date",
    ],
    "expected_dtypes": {
        "contract_id": "string",
        "total_value": "float64",
        "acv": "float64",
        "start_date": "datetime64",
        "end_date": "datetime64",
        "term_months": "float64",
        "quantity": "float64",
        "contract_duration_days": "float64",
        "annualized_value": "float64",
        "line_item_count": "int64",
    },
    "null_handling": {
        "total_value": "reject",  # Null total_value = reject row
        "contract_id": "reject",  # Null contract_id = reject row
        "start_date": "reject",  # Null start_date = reject row
        "end_date": "reject",  # Null end_date = reject row
        "acv": "fill_zero",  # Fill missing ACV with 0
        "term_months": "fill_median",  # Fill missing term with median
        "quantity": "fill_one",  # Fill missing quantity with 1
        "discount": "fill_zero",  # Fill missing discount with 0
    },
    "scaling_assumptions": {
        "total_value": "log_transform_recommended",
        "acv": "log_transform_recommended",
        "term_months": "standard_scale",
        "quantity": "standard_scale",
    },
}


class MLContractViolation(Exception):
    """Raised when the ML input contract is violated."""

    pass


def validate_ml_contract(
    df: pd.DataFrame,
    contract: dict | None = None,
) -> tuple[bool, list[str]]:
    """Validate a DataFrame against the ML input contract.

    Args:
        df: Canonical output DataFrame.
        contract: Optional custom ML contract. Uses ML_CONTRACT if None.

    Returns:
        (is_valid, list of violation messages)
    """
    contract = contract or ML_CONTRACT
    violations: list[str] = []

    # Check required columns
    required = contract.get("required_columns", [])
    missing = [c for c in required if c not in df.columns]
    if missing:
        violations.append(f"Missing required ML columns: {missing}")

    # Check for nulls in reject-policy columns
    null_handling = contract.get("null_handling", {})
    for col, policy in null_handling.items():
        if col in df.columns and policy == "reject":
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                violations.append(f"'{col}' has {null_count} nulls (policy: reject)")

    is_valid = len(violations) == 0

    if is_valid:
        logger.info("ML contract validation: PASSED")
    else:
        logger.warning(
            "ML contract validation: FAILED with %d violations", len(violations)
        )
        for v in violations:
            logger.warning("  - %s", v)

    return is_valid, violations


def prepare_for_ml(
    df: pd.DataFrame,
    contract: dict | None = None,
) -> pd.DataFrame:
    """Prepare a canonical DataFrame for ML consumption.

    Applies:
      1. ML contract validation (raises on failure)
      2. Dtype coercion
      3. Null handling per policy
      4. Drops non-ML columns

    Args:
        df: Canonical output DataFrame.
        contract: Optional custom ML contract.

    Returns:
        ML-ready DataFrame.

    Raises:
        MLContractViolation: If the contract validation fails.
    """
    contract = contract or ML_CONTRACT
    df = df.copy()

    # Step 1: Validate
    is_valid, violations = validate_ml_contract(df, contract)
    if not is_valid:
        raise MLContractViolation(
            f"ML contract violated with {len(violations)} issues:\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    # Step 2: Apply null handling
    null_handling = contract.get("null_handling", {})
    for col, policy in null_handling.items():
        if col not in df.columns:
            continue

        if policy == "fill_zero":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        elif policy == "fill_one":
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(1)
        elif policy == "fill_median":
            numeric = pd.to_numeric(df[col], errors="coerce")
            median = numeric.median()
            df[col] = numeric.fillna(median if not pd.isna(median) else 0)
        elif policy == "reject":
            df = df[df[col].notna()]

    # Step 3: Coerce dtypes
    expected_dtypes = contract.get("expected_dtypes", {})
    for col, dtype in expected_dtypes.items():
        if col not in df.columns:
            continue

        try:
            if dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            elif dtype == "datetime64":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype == "string":
                df[col] = df[col].astype(str)
        except Exception as exc:
            logger.warning("Failed to coerce '%s' to %s: %s", col, dtype, exc)

    # Step 4: Drop internal/meta columns
    drop_cols = [c for c in df.columns if c.startswith("_") or c.startswith("meta_")]
    df = df.drop(columns=drop_cols, errors="ignore")

    logger.info(
        "ML preparation: %d rows, %d columns ready for consumption.",
        len(df),
        len(df.columns),
    )
    return df


def get_ml_contract() -> dict:
    """Return the current ML input contract definition."""
    return dict(ML_CONTRACT)
