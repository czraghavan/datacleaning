"""
Layer 1 — Raw Ingestion

Accept multiple sheets per dataset upload. Store each sheet independently
and immutably. No cleaning, merging, renaming, or transforming at this stage.

Responsibilities:
  - Assign dataset_id, company_id, sheet_id, upload_timestamp
  - Preserve original column names, inferred types, raw values, row order
  - Generate content hash (SHA-256) per sheet
  - Persist immutable raw tables as parquet
"""

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def ingest_excel(
    filepath: str,
    company_id: str | None = None,
    dataset_id: str | None = None,
) -> list[dict]:
    """Ingest all sheets from a single Excel file.

    Args:
        filepath: Path to .xlsx file.
        company_id: Optional company identifier. Auto-generated if not provided.
        dataset_id: Optional dataset identifier. Auto-generated if not provided.

    Returns:
        List of sheet records, each containing:
          - dataset_id, company_id, sheet_id, upload_timestamp
          - sheet_name, source_file
          - dataframe (raw, immutable)
          - content_hash (SHA-256)
          - original_columns, inferred_types, row_count
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"Excel file not found: {filepath}")

    dataset_id = dataset_id or str(uuid.uuid4())
    company_id = company_id or "unknown"
    upload_ts = datetime.now(timezone.utc).isoformat()

    sheets: list[dict] = []

    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as exc:
        logger.error("Failed to open Excel file %s: %s", filepath, exc)
        return sheets

    for sheet_name in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)

            # Drop entirely empty rows
            df = df.dropna(how="all")
            if df.empty:
                logger.warning(
                    "Sheet '%s' in %s is empty — skipping.", sheet_name, filepath.name
                )
                continue

            sheet_id = str(uuid.uuid4())

            # Compute content hash of raw data
            content_hash = _compute_hash(df)

            # Capture original metadata before any modification
            original_columns = list(df.columns)
            inferred_types = {col: str(df[col].dtype) for col in df.columns}

            sheet_record = {
                "dataset_id": dataset_id,
                "company_id": company_id,
                "sheet_id": sheet_id,
                "upload_timestamp": upload_ts,
                "sheet_name": sheet_name,
                "source_file": filepath.name,
                "dataframe": df,
                "content_hash": content_hash,
                "original_columns": original_columns,
                "inferred_types": inferred_types,
                "row_count": len(df),
            }

            sheets.append(sheet_record)
            logger.info(
                "Ingested sheet '%s' from %s: %d rows, %d cols, hash=%s",
                sheet_name,
                filepath.name,
                len(df),
                len(df.columns),
                content_hash[:12],
            )

        except Exception as exc:
            logger.error(
                "Error reading sheet '%s' in %s: %s", sheet_name, filepath.name, exc
            )

    logger.info(
        "Layer 1 complete: ingested %d sheet(s) from %s, dataset_id=%s",
        len(sheets),
        filepath.name,
        dataset_id,
    )
    return sheets


def ingest_csv(
    filepath: str,
    company_id: str | None = None,
    dataset_id: str | None = None,
) -> list[dict]:
    """Ingest a single CSV file as one 'sheet'.

    Args:
        filepath: Path to .csv file.
        company_id: Optional company identifier.
        dataset_id: Optional dataset identifier.

    Returns:
        List with one sheet record (same structure as ingest_excel).
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    dataset_id = dataset_id or str(uuid.uuid4())
    company_id = company_id or "unknown"
    upload_ts = datetime.now(timezone.utc).isoformat()

    try:
        df = pd.read_csv(filepath, encoding="utf-8-sig", dtype=str)
        df = df.dropna(how="all")
        if df.empty:
            logger.warning("CSV file %s is empty — skipping.", filepath.name)
            return []

        sheet_id = str(uuid.uuid4())
        content_hash = _compute_hash(df)

        record = {
            "dataset_id": dataset_id,
            "company_id": company_id,
            "sheet_id": sheet_id,
            "upload_timestamp": upload_ts,
            "sheet_name": "csv",
            "source_file": filepath.name,
            "dataframe": df,
            "content_hash": content_hash,
            "original_columns": list(df.columns),
            "inferred_types": {col: str(df[col].dtype) for col in df.columns},
            "row_count": len(df),
        }

        logger.info(
            "Ingested CSV %s: %d rows, %d cols, hash=%s",
            filepath.name,
            len(df),
            len(df.columns),
            content_hash[:12],
        )
        return [record]

    except Exception as exc:
        logger.error("Error reading CSV %s: %s", filepath.name, exc)
        return []


def ingest_directory(
    dir_path: str,
    company_id: str | None = None,
    dataset_id: str | None = None,
) -> list[dict]:
    """Ingest all .csv and .xlsx files from a directory.

    All files share the same dataset_id. Each file/sheet gets its own sheet_id.
    """
    dir_path = Path(dir_path)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a valid directory: {dir_path}")

    dataset_id = dataset_id or str(uuid.uuid4())
    all_sheets: list[dict] = []
    supported = {".csv", ".xlsx"}

    for entry in sorted(dir_path.iterdir()):
        if entry.suffix.lower() not in supported:
            continue

        if entry.suffix.lower() == ".csv":
            sheets = ingest_csv(
                str(entry), company_id=company_id, dataset_id=dataset_id
            )
        else:
            sheets = ingest_excel(
                str(entry), company_id=company_id, dataset_id=dataset_id
            )

        all_sheets.extend(sheets)

    logger.info(
        "Directory ingestion complete: %d sheet(s) from %s", len(all_sheets), dir_path
    )
    return all_sheets


def persist_raw(sheets: list[dict], artifacts_dir: str) -> list[dict]:
    """Persist raw ingested sheets as parquet files.

    Writes to: {artifacts_dir}/{dataset_id}/raw/{sheet_id}.parquet

    Returns the sheet records with an added 'raw_path' field.
    """
    for sheet in sheets:
        raw_dir = Path(artifacts_dir) / sheet["dataset_id"] / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        path = raw_dir / f"{sheet['sheet_id']}.parquet"
        sheet["dataframe"].to_parquet(path, index=False)
        sheet["raw_path"] = str(path)

        # Also persist metadata
        meta = {k: v for k, v in sheet.items() if k != "dataframe"}
        import json

        meta_path = raw_dir / f"{sheet['sheet_id']}_meta.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2, default=str)

        logger.info("Persisted raw sheet %s → %s", sheet["sheet_id"], path)

    return sheets


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_hash(df: pd.DataFrame) -> str:
    """Compute SHA-256 hash of DataFrame contents for integrity checking."""
    # Use CSV serialization for deterministic hashing
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()
