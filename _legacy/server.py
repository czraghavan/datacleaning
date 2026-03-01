"""
server.py — FastAPI web server for the 10-layer data transformation pipeline.

Run with:  python3 server.py
Then open: http://localhost:8000

Three-phase processing flow:
  1. POST /api/analyze  — Layers 1-3: ingest, profile, suggest mappings
  2. POST /api/confirm  — Layers 4-7: confirm mappings, transform, validate, output
  3. GET  /api/download  — Download canonical output files

All intermediate artifacts are persisted to the artifacts/ directory.
"""

import json
import logging
import shutil
import tempfile
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.ai_assist import suggest_column_mappings
from src.export import export_rich_excel
from src.layers.canonical_output import (persist_canonical_output,
                                         produce_canonical_output)
from src.layers.canonical_schema import load_latest_schema
from src.layers.ingestion import ingest_excel, persist_raw
from src.layers.ml_interface import validate_ml_contract
from src.layers.profiling import (cross_sheet_analysis, persist_profiles,
                                  profile_all_sheets)
from src.layers.schema_mapping import (MappingConfig, persist_mapping_config,
                                       suggest_mappings)
from src.layers.transformation import (aggregate, classify_rows,
                                       compute_derived_fields,
                                       extract_mapped_columns, join_entities,
                                       log_transformation)
from src.layers.validation import persist_validation_result, validate
from src.layers.versioning import create_manifest, persist_manifest
from src.utils import sample_values

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DataOrgModel.server")

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="DataOrgModel Pipeline", version="2.0.0")

STATIC_DIR = Path(__file__).parent / "frontend"
CONFIGS_DIR = Path(__file__).parent / "configs"
ARTIFACTS_DIR = Path(__file__).parent / "artifacts"
TEMPLATES_DIR = Path(__file__).parent / "templates"

TEMPLATES_DIR.mkdir(exist_ok=True)
ARTIFACTS_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory store for pending jobs
_pending_jobs: dict[str, dict] = {}


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return (STATIC_DIR / "index.html").read_text()


# =====================================================================
# Templates — save / load mapping profiles
# =====================================================================


class TemplateRequest(BaseModel):
    name: str
    mappings: dict[str, str | None]


@app.get("/api/templates")
async def list_templates():
    """List all saved mapping templates."""
    templates = []
    for f in sorted(TEMPLATES_DIR.glob("*.json")):
        try:
            data = json.loads(f.read_text())
            templates.append(
                {
                    "id": f.stem,
                    "name": data.get("name", f.stem),
                    "field_count": len(data.get("mappings", {})),
                }
            )
        except Exception:
            pass
    return {"templates": templates}


@app.post("/api/templates")
async def save_template(req: TemplateRequest):
    """Save a column mapping template."""
    template_id = req.name.lower().replace(" ", "_")
    path = TEMPLATES_DIR / f"{template_id}.json"
    data = {"name": req.name, "mappings": req.mappings}
    path.write_text(json.dumps(data, indent=2))
    return {"id": template_id, "name": req.name}


@app.get("/api/templates/{template_id}")
async def get_template(template_id: str):
    """Load a specific template."""
    path = TEMPLATES_DIR / f"{template_id}.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Template not found")
    return json.loads(path.read_text())


@app.delete("/api/templates/{template_id}")
async def delete_template(template_id: str):
    """Delete a template."""
    path = TEMPLATES_DIR / f"{template_id}.json"
    if path.exists():
        path.unlink()
    return {"deleted": template_id}


# =====================================================================
# Phase 1: Analyze (Layers 1-3)
# =====================================================================


@app.post("/api/analyze")
async def analyze_file(
    file: UploadFile = File(...),
    header_threshold: int = Form(80),
):
    """Phase 1: Ingest, profile, and suggest mappings.

    Executes Layers 1-3 and returns:
      - Profiling summary per sheet
      - Suggested column mappings
      - Cross-sheet analysis
    """
    job_id = str(uuid.uuid4())[:8]

    # Save uploaded file
    suffix = Path(file.filename).suffix
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    contents = await file.read()
    tmp.write(contents)
    tmp.close()

    try:
        # ── Layer 1: Ingest ──────────────────────────────────────
        sheets = ingest_excel(tmp.name, company_id=job_id)
        if not sheets:
            return {"error": "No data found in file.", "job_id": job_id}

        dataset_id = sheets[0]["dataset_id"]

        # ── Smart sheet filtering ────────────────────────────────
        # Skip sheets that are pivot tables (mostly unnamed cols), empty, or metadata
        processed_sheets = []
        skipped_sheets = []
        for sheet in sheets:
            df = sheet["dataframe"]
            total_cols = len(df.columns)
            if total_cols == 0 or len(df) == 0:
                skipped_sheets.append(
                    {"name": sheet["sheet_name"], "reason": "Empty sheet"}
                )
                continue
            unnamed_cols = sum(1 for c in df.columns if str(c).startswith("Unnamed"))
            named_ratio = (
                (total_cols - unnamed_cols) / total_cols if total_cols > 0 else 0
            )
            named_count = total_cols - unnamed_cols
            if named_ratio < 0.5 or named_count < 2:
                skipped_sheets.append(
                    {
                        "name": sheet["sheet_name"],
                        "reason": f"Pivot table or lookup ({unnamed_cols}/{total_cols} unnamed columns)",
                    }
                )
                continue
            processed_sheets.append(sheet)

        if not processed_sheets:
            return {
                "error": "No sheets with structured data found. All sheets appear to be pivot tables or empty.",
                "job_id": job_id,
            }

        sheets = processed_sheets
        sheets = persist_raw(sheets, str(ARTIFACTS_DIR))

        # ── Layer 2: Profile ─────────────────────────────────────
        profiles = profile_all_sheets(sheets)
        cross = cross_sheet_analysis(profiles, sheets) if len(sheets) > 1 else None
        persist_profiles(profiles, cross, str(ARTIFACTS_DIR), dataset_id)

        # ── Layer 3: Schema ──────────────────────────────────────
        schema = load_latest_schema(str(CONFIGS_DIR))

        # ── Layer 4 (suggestions only): Mapping ──────────────────
        suggestions = suggest_mappings(sheets, schema, threshold=header_threshold)

        # Try AI suggestions for unmapped columns
        all_unmapped = []
        sample_vals = {}
        for sid, s_data in suggestions.items():
            for col in s_data["unmapped"]:
                all_unmapped.append(col)
                for sheet in sheets:
                    if sheet["sheet_id"] == sid and col in sheet["dataframe"].columns:
                        sample_vals[col] = sample_values(sheet["dataframe"][col], 5)

        ai_suggestions = suggest_column_mappings(all_unmapped, sample_vals)

        # Build response
        proposed = {}
        all_unmapped_final = []
        confidence_map = {}

        for sid, s_data in suggestions.items():
            for raw_col, canonical in s_data["suggested"].items():
                proposed[raw_col] = canonical
                confidence_map[raw_col] = s_data["confidence"].get(raw_col, {})

            for raw_col in s_data["unmapped"]:
                if raw_col in ai_suggestions:
                    proposed[raw_col] = ai_suggestions[raw_col]
                    confidence_map[raw_col] = {
                        "canonical": ai_suggestions[raw_col],
                        "score": 60,
                        "method": "ai_suggestion",
                    }
                else:
                    all_unmapped_final.append(raw_col)

        # Build a lookup from column name → raw sample values
        col_samples_map: dict[str, list[str]] = {}
        for sheet in sheets:
            df = sheet["dataframe"]
            for col in df.columns:
                if col not in col_samples_map:
                    col_samples_map[col] = sample_values(df[col], 30)

        # Build profiling summary for the frontend (include sheet_id for primary-sheet resolution)
        profiling_summary = []
        for p in profiles:
            sheet_summary = {
                "sheet_id": p["sheet_id"],
                "sheet_name": p["sheet_name"],
                "row_count": p["row_count"],
                "column_count": p["column_count"],
                "duplicate_row_pct": p["duplicate_row_pct"],
                "columns": {},
            }
            for col_name, col_data in p["columns"].items():
                sheet_summary["columns"][col_name] = {
                    "type": col_data["inferred_type"],
                    "null_pct": col_data["null_pct"],
                    "unique_count": col_data["unique_count"],
                    "top_values": col_samples_map.get(col_name, []),
                }
                if "numeric_stats" in col_data:
                    sheet_summary["columns"][col_name]["numeric_stats"] = col_data[
                        "numeric_stats"
                    ]
                if "date_range" in col_data:
                    sheet_summary["columns"][col_name]["date_range"] = col_data[
                        "date_range"
                    ]

            profiling_summary.append(sheet_summary)

        # Store job state for phase 2
        _pending_jobs[job_id] = {
            "dataset_id": dataset_id,
            "sheets": sheets,
            "profiles": profiles,
            "schema": schema,
            "suggestions": suggestions,
            "file_path": tmp.name,
        }

        # Build canonical fields list for the frontend
        canonical_fields = [
            {
                "name": name,
                "type": f.get("type"),
                "required": f.get("required", False),
                "category": f.get("category"),
                "description": f.get("description", ""),
            }
            for name, f in schema.fields.items()
        ]

        return {
            "job_id": job_id,
            "dataset_id": dataset_id,
            "filename": file.filename,
            "total_rows": sum(s["row_count"] for s in sheets),
            "sheets_processed": len(sheets),
            "sheets_skipped": skipped_sheets,
            "proposed_mappings": proposed,
            "unmapped_columns": all_unmapped_final,
            "confidence": confidence_map,
            "canonical_fields": canonical_fields,
            "profiling": profiling_summary,
            "cross_sheet_analysis": cross,
        }

    except Exception as exc:
        logger.error("Analysis failed: %s", exc, exc_info=True)
        return {"error": str(exc), "job_id": job_id}


# =====================================================================
# Phase 2: Confirm (Layers 4-9)
# =====================================================================


class ConfirmRequest(BaseModel):
    job_id: str
    mappings: dict[str, str | None]
    selected_sheets: list[str] | None = None
    primary_sheet_id: str | None = None
    vendor_threshold: int = 85
    header_threshold: int = 80


@app.post("/api/confirm")
async def confirm_mappings(req: ConfirmRequest):
    """Phase 2: Confirm mappings and run Layers 4-9.

    Executes: mapping confirmation → extraction → classification →
    aggregation → derived fields → validation → canonical output.
    """
    job = _pending_jobs.get(req.job_id)
    if not job:
        return {"error": f"Job '{req.job_id}' not found or expired."}

    try:
        sheets = job["sheets"]
        schema = job["schema"]
        dataset_id = job["dataset_id"]

        # ── Filter sheets by user selection ───────────────────────
        if req.selected_sheets is not None:
            sheets = [s for s in sheets if s["sheet_name"] in req.selected_sheets]
            logger.info(
                "Sheet selection: using %d of %d sheets: %s",
                len(sheets),
                len(job["sheets"]),
                [s["sheet_name"] for s in sheets],
            )
            if not sheets:
                return {
                    "error": "No sheets selected. Please select at least one sheet."
                }

        # Resolve primary_sheet_id: frontend may send sheet name; backend needs sheet_id
        primary_sheet_id_resolved = None
        if req.primary_sheet_id:
            name_to_id = {s["sheet_name"]: s["sheet_id"] for s in sheets}
            primary_sheet_id_resolved = name_to_id.get(req.primary_sheet_id) or (
                req.primary_sheet_id if req.primary_sheet_id in {s["sheet_id"] for s in sheets} else None
            )
            logger.info(
                "Primary sheet: %s → %s",
                req.primary_sheet_id,
                primary_sheet_id_resolved or "auto",
            )

        # ── Layer 4: Build confirmed mapping config ──────────────
        mapping_config = MappingConfig(dataset_id=dataset_id)
        active_mappings = {
            raw: canonical
            for raw, canonical in req.mappings.items()
            if canonical is not None
        }

        # Map each raw column to the sheet that has it; allow same canonical from multiple sheets (e.g. contract_id from CRM and Line Items)
        for raw_col, canonical in active_mappings.items():
            for sheet in sheets:
                if raw_col in sheet["dataframe"].columns:
                    mapping_config.set_mapping(canonical, sheet["sheet_id"], raw_col)
                    break

        persist_mapping_config(mapping_config, str(ARTIFACTS_DIR))

        # ── Layer 5: Transformation Engine ────────────────────────
        # Extract returns dict[sheet_id, pd.DataFrame]
        extracted_sheets = extract_mapped_columns(sheets, mapping_config, schema)
        rows_before = sum(len(df) for df in extracted_sheets.values())

        # Classify returns dict[sheet_id, DatasetClassification]
        classifications = classify_rows(extracted_sheets)

        agg_config = _load_config("aggregation_rules")
        # Aggregate returns (dict[sheet_id, pd.DataFrame], dict[sheet_id, pd.DataFrame])
        aggregated_sheets, original_line_items_dict = aggregate(
            extracted_sheets, classifications, agg_config
        )

        # Join into a single flat DataFrame
        aggregated = join_entities(
            aggregated_sheets, primary_sheet_id=primary_sheet_id_resolved
        )
        rows_after = len(aggregated)

        # Merge original line items into a single DF for downstream if present
        line_items = None
        if original_line_items_dict:
            line_items = pd.concat(original_line_items_dict.values(), ignore_index=True)

        derived_config = _load_config("derived_fields")
        aggregated = compute_derived_fields(aggregated, derived_config)

        # Log highest level classification as the primary for auditing
        primary_class = (
            next(iter(classifications.values()))
            if classifications
            else DatasetClassification("mixed", {})
        )
        if primary_sheet_id_resolved and primary_sheet_id_resolved in classifications:
            primary_class = classifications[primary_sheet_id_resolved]

        transform_log = log_transformation(
            dataset_id=dataset_id,
            mapping_version=mapping_config.version,
            schema_version=schema.version,
            aggregation_config_version=agg_config.get("version", "v1"),
            derived_config_version=derived_config.get("version", "v1"),
            classification=primary_class,
            rows_before=rows_before,
            rows_after=rows_after,
            artifacts_dir=str(ARTIFACTS_DIR),
        )

        # ── Layer 6: Validation ──────────────────────────────────
        val_config = _load_config("validation_rules")
        validation_result = validate(aggregated, val_config, line_items)
        persist_validation_result(validation_result, str(ARTIFACTS_DIR), dataset_id)

        # ── Layer 7: Canonical Output ────────────────────────────
        canonical_output = produce_canonical_output(
            contracts_df=aggregated,
            line_items_df=line_items,
            schema_version=schema.version,
            mapping_version=mapping_config.version,
            transform_version=agg_config.get("version", "v1"),
            dataset_id=dataset_id,
        )
        output_paths = persist_canonical_output(canonical_output, str(ARTIFACTS_DIR))

        # ── Layer 8: ML Contract ─────────────────────────────────
        ml_valid, ml_violations = validate_ml_contract(canonical_output["contracts"])

        # ── Layer 9: Versioning ──────────────────────────────────
        manifest = create_manifest(
            dataset_id=dataset_id,
            schema_version=schema.version,
            mapping_version=mapping_config.version,
            aggregation_version=agg_config.get("version", "v1"),
            derived_fields_version=derived_config.get("version", "v1"),
            validation_version=val_config.get("version", "v1"),
            configs_dir=str(CONFIGS_DIR),
        )
        persist_manifest(manifest, str(ARTIFACTS_DIR))

        # ── Generate rich Excel export ───────────────────────────
        contracts_df = canonical_output["contracts"]

        # Build audit data
        audit_data = {
            "dataset_id": dataset_id,
            "total_rows_ingested": sum(s["row_count"] for s in sheets),
            "final_rows": len(contracts_df),
            "classification": primary_class.to_dict(),
            "rows_before_aggregation": rows_before,
            "rows_after_aggregation": rows_after,
            "schema_version": schema.version,
            "mapping_version": mapping_config.version,
        }

        mapping_log = [
            {"raw_column": raw, "canonical_field": canonical, "method": "confirmed"}
            for canonical, (sid, raw) in mapping_config.mappings.items()
        ]

        # Export rich Excel
        job_dir = Path(ARTIFACTS_DIR) / dataset_id
        try:
            excel_path = export_rich_excel(
                master_df=contracts_df,
                audit_data=audit_data,
                vendor_clusters=[],
                mapping_log=mapping_log,
                job_dir=job_dir,
                job_id=dataset_id,
            )
            output_paths["excel"] = str(excel_path)
        except Exception as exc:
            logger.warning("Rich Excel export failed: %s", exc)

        # Build quality metrics for frontend
        quality = _compute_quality_metrics(contracts_df)
        timeline = _compute_timeline(contracts_df)

        # Build results response
        results = {
            "job_id": req.job_id,
            "dataset_id": dataset_id,
            "status": "complete",
            "classification": primary_class.to_dict(),
            "rows_before": rows_before,
            "rows_after": rows_after,
            "columns": list(contracts_df.columns),
            "preview": contracts_df.head(50)
            .replace({np.nan: None})
            .to_dict(orient="records"),
            "validation": validation_result.to_dict(),
            "ml_ready": ml_valid and validation_result.passed,
            "ml_violations": ml_violations,
            "quality": quality,
            "timeline": timeline,
            "versions": manifest.to_dict()["versions"],
            "downloads": {},
        }

        # Build download links (include aliases for frontend)
        for key, path in output_paths.items():
            filename = Path(path).name
            results["downloads"][key] = f"/api/download/{dataset_id}/{filename}"
        if "contracts_csv" in output_paths:
            results["downloads"]["csv"] = results["downloads"]["contracts_csv"]
        if "contracts_parquet" in output_paths:
            results["downloads"]["parquet"] = results["downloads"]["contracts_parquet"]

        # Cleanup
        _pending_jobs.pop(req.job_id, None)

        return results

    except Exception as exc:
        logger.error("Processing failed: %s", exc, exc_info=True)
        return {"error": str(exc), "job_id": req.job_id}


# =====================================================================
# Download
# =====================================================================


@app.get("/api/download/{dataset_id}/{filename}")
async def download_file(dataset_id: str, filename: str):
    path = ARTIFACTS_DIR / dataset_id / "canonical" / filename
    if not path.exists():
        # Try other directories
        for subdir in ["", "raw", "profiles", "mappings"]:
            alt = (
                ARTIFACTS_DIR / dataset_id / subdir / filename
                if subdir
                else ARTIFACTS_DIR / dataset_id / filename
            )
            if alt.exists():
                path = alt
                break

    if not path.exists():
        return {"error": f"File not found: {filename}"}

    return FileResponse(
        path=str(path),
        filename=filename,
        media_type="application/octet-stream",
    )


# =====================================================================
# Helpers
# =====================================================================


def _load_config(component: str) -> dict:
    """Load the latest version of a config component."""
    files = sorted(CONFIGS_DIR.glob(f"{component}_v*.json"))
    if not files:
        raise FileNotFoundError(f"No {component} config found in {CONFIGS_DIR}")
    with open(files[-1]) as f:
        return json.load(f)


def _compute_quality_metrics(df: pd.DataFrame) -> dict:
    """Compute per-column completeness and overall quality score."""
    n = len(df)
    if n == 0:
        return {"overall_score": 0, "columns": {}}

    columns = {}
    total_completeness = 0
    data_cols = [c for c in df.columns if not c.startswith("_")]

    for col in data_cols:
        null_count = int(df[col].isna().sum())
        completeness = round((1 - null_count / n) * 100, 1) if n > 0 else 0
        columns[col] = {
            "completeness": completeness,
            "null_count": null_count,
        }
        total_completeness += completeness

    overall = round(total_completeness / len(data_cols), 1) if data_cols else 0

    return {"overall_score": overall, "columns": columns}


def _compute_timeline(df: pd.DataFrame) -> list[dict]:
    """Build timeline entries for contracts with date data."""
    timeline = []
    if "contract_id" not in df.columns:
        return timeline

    for _, row in df.head(100).iterrows():
        entry = {"id": str(row.get("contract_id", ""))}

        if "vendor" in df.columns:
            entry["vendor"] = (
                str(row.get("vendor", "")) if pd.notna(row.get("vendor")) else ""
            )
        if "vendor_canonical" in df.columns:
            entry["vendor"] = (
                str(row.get("vendor_canonical", ""))
                if pd.notna(row.get("vendor_canonical"))
                else ""
            )

        if "start_date" in df.columns and pd.notna(row.get("start_date")):
            entry["start"] = str(row["start_date"])
        if "end_date" in df.columns and pd.notna(row.get("end_date")):
            entry["end"] = str(row["end_date"])
        if "total_value" in df.columns and pd.notna(row.get("total_value")):
            entry["value"] = float(row["total_value"])

        if "start" in entry or "end" in entry:
            timeline.append(entry)

    return timeline


# ---------------------------------------------------------------------------
# Run server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    print("\n  🚀  DataOrgModel Pipeline running at http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
