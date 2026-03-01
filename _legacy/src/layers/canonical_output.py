"""
Layer 7 — Canonical Output

Produce final canonical contract-level and line-item-level tables.
Link output to schema version, mapping version, and transformation version.

Only canonical outputs may be used downstream.
"""

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def produce_canonical_output(
    contracts_df: pd.DataFrame,
    line_items_df: pd.DataFrame | None,
    schema_version: str,
    mapping_version: str,
    transform_version: str,
    dataset_id: str,
) -> dict:
    """Produce final canonical tables with version metadata.

    Args:
        contracts_df: Aggregated contract-level DataFrame.
        line_items_df: Optional line-item-level DataFrame.
        schema_version: Version of the canonical schema used.
        mapping_version: Version of the mapping config used.
        transform_version: Version of the transformation rules used.
        dataset_id: Dataset identifier.

    Returns:
        Dict with:
          - contracts: canonical contract-level DataFrame
          - line_items: optional line-item DataFrame
          - metadata: version and summary info
    """
    # Clean up internal columns from contract output
    internal_cols = [c for c in contracts_df.columns if c.startswith("_")]
    contracts_clean = contracts_df.drop(columns=internal_cols, errors="ignore")

    # Churn model format: account_id and contract_id first, then key fields for ML/reporting
    CHURN_MODEL_ORDER = [
        "account_id",
        "contract_id",
        "vendor",
        "vendor_canonical",
        "total_value",
        "acv",
        "start_date",
        "end_date",
        "term_months",
        "contract_duration_days",
        "duration_years",
        "annualized_value",
        "monthly_value",
        "churn_flag",
        "renewal_flag",
        "close_date",
        "renewal_date",
        "line_item_count",
        "row_lineage_id",
    ]
    cols = list(contracts_clean.columns)
    ordered_cols = [c for c in CHURN_MODEL_ORDER if c in cols]
    remaining = [c for c in cols if c not in CHURN_MODEL_ORDER]
    ordered_cols.extend(remaining)
    contracts_clean = contracts_clean[ordered_cols]

    # Build metadata summary
    metadata = {
        "dataset_id": dataset_id,
        "schema_version": schema_version,
        "mapping_version": mapping_version,
        "transform_version": transform_version,
        "contract_count": len(contracts_clean),
        "contract_columns": list(contracts_clean.columns),
    }

    result = {
        "contracts": contracts_clean,
        "line_items": None,
        "metadata": metadata,
    }

    if line_items_df is not None:
        li_clean = line_items_df.drop(
            columns=[c for c in line_items_df.columns if c.startswith("_")],
            errors="ignore",
        )
        result["line_items"] = li_clean
        metadata["line_item_count"] = len(li_clean)
        metadata["line_item_columns"] = list(li_clean.columns)

    logger.info(
        "Layer 7 Canonical Output: %d contracts, %d line items, versions=[%s, %s, %s]",
        metadata["contract_count"],
        metadata.get("line_item_count", 0),
        schema_version,
        mapping_version,
        transform_version,
    )

    return result


def persist_canonical_output(
    output: dict,
    artifacts_dir: str,
) -> dict[str, str]:
    """Persist canonical output to the artifacts directory.

    Writes:
      - contracts.parquet
      - contracts.csv
      - line_items.parquet (if applicable)
      - output_metadata.json

    Returns dict of file paths.
    """
    dataset_id = output["metadata"]["dataset_id"]
    out_dir = Path(artifacts_dir) / dataset_id / "canonical"
    out_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, str] = {}

    # Contracts
    contracts_parquet = out_dir / "contracts.parquet"
    output["contracts"].to_parquet(contracts_parquet, index=False)
    paths["contracts_parquet"] = str(contracts_parquet)

    contracts_csv = out_dir / "contracts.csv"
    output["contracts"].to_csv(contracts_csv, index=False, encoding="utf-8")
    paths["contracts_csv"] = str(contracts_csv)

    # Line items
    if output["line_items"] is not None:
        li_parquet = out_dir / "line_items.parquet"
        output["line_items"].to_parquet(li_parquet, index=False)
        paths["line_items_parquet"] = str(li_parquet)

    # Metadata
    meta_path = out_dir / "output_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(output["metadata"], f, indent=2, default=str)
    paths["metadata"] = str(meta_path)

    logger.info("Persisted canonical output → %s", out_dir)
    return paths
