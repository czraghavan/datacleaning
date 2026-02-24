"""
server.py — FastAPI web server for the SaaS Contract Data Organizer.

Run with:  python3 server.py
Then open: http://localhost:8000

Two-phase processing flow:
  1. POST /api/analyze  — ingest + header mapping → proposed mappings for review
  2. POST /api/confirm  — accept overrides, run pipeline → results + exports

Plus:
  - GET/POST /api/templates — save/load column mapping profiles
  - Anomaly detection, data quality metrics, timeline data in results
"""

import os
import json
import uuid
import shutil
import logging
import tempfile
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.ingest import load_excel, load_csv_dir, _clean_frame
from src.schema import fuzzy_map_headers, CANONICAL_SCHEMA
from src.cleaner import Cleaner
from src.vendor import resolve_vendors
from src.merge import concat_all, merge_quote_lines, deduplicate
from src.llm_mapper import llm_detect_columns
from src.export import export_rich_excel
from src.anomalies import detect_anomalies

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
app = FastAPI(title="SaaS Contract Data Organizer")

WORK_DIR = Path(tempfile.gettempdir()) / "dataorgmodel"
WORK_DIR.mkdir(exist_ok=True)

STATIC_DIR = Path(__file__).parent / "frontend"
TEMPLATES_DIR = Path(__file__).parent / "templates"
TEMPLATES_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory store for pending jobs
_pending_jobs: dict[str, dict] = {}


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


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
            data = json.loads(f.read_text(encoding="utf-8"))
            templates.append({
                "id": f.stem,
                "name": data.get("name", f.stem),
                "column_count": len(data.get("mappings", {})),
                "created": data.get("created", ""),
            })
        except Exception:
            pass
    return {"templates": templates}


@app.post("/api/templates/save")
async def save_template(req: TemplateRequest):
    """Save a column mapping template."""
    template_id = req.name.lower().replace(" ", "_")[:40]
    path = TEMPLATES_DIR / f"{template_id}.json"
    data = {
        "name": req.name,
        "mappings": req.mappings,
        "created": datetime.now().isoformat(),
    }
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    logger.info("Saved template: '%s' (%d mappings)", req.name, len(req.mappings))
    return {"id": template_id, "name": req.name}


@app.get("/api/templates/{template_id}")
async def get_template(template_id: str):
    """Load a specific template."""
    path = TEMPLATES_DIR / f"{template_id}.json"
    if not path.exists():
        raise HTTPException(404, "Template not found.")
    data = json.loads(path.read_text(encoding="utf-8"))
    return data


@app.delete("/api/templates/{template_id}")
async def delete_template(template_id: str):
    """Delete a template."""
    path = TEMPLATES_DIR / f"{template_id}.json"
    if path.exists():
        path.unlink()
    return {"deleted": template_id}


# =====================================================================
# Phase 1: Analyze
# =====================================================================
@app.post("/api/analyze")
async def analyze_file(
    file: UploadFile = File(...),
    header_threshold: int = 80,
):
    suffix = Path(file.filename).suffix.lower()
    if suffix not in {".xlsx", ".csv"}:
        raise HTTPException(400, f"Unsupported file type: {suffix}")

    job_id = str(uuid.uuid4())[:8]
    job_dir = WORK_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    input_path = job_dir / file.filename
    with open(input_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        if suffix == ".xlsx":
            frames = load_excel(str(input_path))
        else:
            frames = [_load_single_csv(str(input_path))]

        if not frames:
            raise HTTPException(400, "No data found in the uploaded file.")

        combined_raw = pd.concat(frames, ignore_index=True, sort=False)
        mapped_df, unmapped = fuzzy_map_headers(combined_raw.copy(), threshold=header_threshold)

        # Sample values for all columns
        sample_values = {}
        for col in list(combined_raw.columns):
            non_null = combined_raw[col].dropna().astype(str).str.strip()
            non_null = non_null[non_null != ""]
            sample_values[col] = non_null.head(5).tolist() if len(non_null) > 0 else []

        # LLM detection for unmapped
        llm_results = llm_detect_columns(unmapped, sample_values)

        # Build mapping log
        mapping_log = []
        mapped_cols = set()

        for orig_col in combined_raw.columns:
            if orig_col in {"meta_source_tab", "meta_source_file"}:
                continue

            if orig_col in mapped_df.columns:
                if orig_col in CANONICAL_SCHEMA:
                    mapping_log.append({
                        "original": orig_col, "canonical": orig_col,
                        "confidence": "high", "source": "exact",
                        "samples": sample_values.get(orig_col, []),
                    })
                    mapped_cols.add(orig_col)
                elif orig_col in unmapped:
                    if orig_col in llm_results:
                        mapping_log.append({
                            "original": orig_col, "canonical": llm_results[orig_col],
                            "confidence": "medium", "source": "llm",
                            "samples": sample_values.get(orig_col, []),
                        })
                    else:
                        mapping_log.append({
                            "original": orig_col, "canonical": None,
                            "confidence": "none", "source": "unmapped",
                            "samples": sample_values.get(orig_col, []),
                        })
                else:
                    mapping_log.append({
                        "original": orig_col, "canonical": orig_col,
                        "confidence": "high", "source": "passthrough",
                        "samples": sample_values.get(orig_col, []),
                    })
            else:
                for new_col in mapped_df.columns:
                    if new_col not in combined_raw.columns and new_col not in mapped_cols:
                        mapping_log.append({
                            "original": orig_col, "canonical": new_col,
                            "confidence": "high", "source": "fuzzy",
                            "samples": sample_values.get(orig_col, []),
                        })
                        mapped_cols.add(new_col)
                        break

        _pending_jobs[job_id] = {
            "frames": frames,
            "input_path": str(input_path),
            "job_dir": str(job_dir),
        }

        categories = sorted(CANONICAL_SCHEMA.keys())

        return {
            "job_id": job_id,
            "filename": file.filename,
            "total_rows": len(combined_raw),
            "total_sheets": len(frames),
            "mappings": mapping_log,
            "categories": categories,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Analyze error for job %s", job_id)
        raise HTTPException(500, f"Analysis error: {exc}")


# =====================================================================
# Phase 2: Confirm
# =====================================================================
class ConfirmRequest(BaseModel):
    job_id: str
    mappings: dict[str, str | None]
    vendor_threshold: int = 85
    header_threshold: int = 80


@app.post("/api/confirm")
async def confirm_mappings(req: ConfirmRequest):
    job_id = req.job_id
    if job_id not in _pending_jobs:
        raise HTTPException(404, f"Job {job_id} not found or expired.")

    job_state = _pending_jobs.pop(job_id)
    frames = job_state["frames"]
    job_dir = Path(job_state["job_dir"])

    try:
        total_rows_ingested = sum(len(f) for f in frames)
        tab_count = len(frames)

        # ── 1. Schema mapping with overrides ─────────────────────────
        user_rename_map = {o: c for o, c in req.mappings.items() if c and c != o}

        mapped_frames = []
        all_unmapped = []
        mapping_log = []

        for df in frames:
            df_mapped, unmapped = fuzzy_map_headers(df, threshold=req.header_threshold)
            override_rename = {o: c for o, c in user_rename_map.items() if o in df_mapped.columns}
            if override_rename:
                df_mapped = df_mapped.rename(columns=override_rename)
            final_unmapped = [c for c in unmapped if c not in user_rename_map]
            all_unmapped.extend(final_unmapped)
            mapped_frames.append(df_mapped)

        all_unmapped = sorted(set(all_unmapped))

        for orig, canon in req.mappings.items():
            if canon:
                source = "user_override" if orig in user_rename_map else "auto"
                mapping_log.append({"original": orig, "canonical": canon, "confidence": "high", "source": source})
            else:
                mapping_log.append({"original": orig, "canonical": None, "confidence": "none", "source": "skipped"})

        # ── 2. Concat ────────────────────────────────────────────────
        master = concat_all(mapped_frames)

        # ── 3. Clean ─────────────────────────────────────────────────
        cleaner = Cleaner()
        master = cleaner.clean(master)

        # ── 4. Vendor resolution ─────────────────────────────────────
        master = resolve_vendors(master, threshold=req.vendor_threshold)
        unique_vendors = int(master["Vendor_Canonical"].nunique()) if "Vendor_Canonical" in master.columns else 0

        vendor_clusters = []
        if "Vendor_Canonical" in master.columns and "Vendor" in master.columns:
            cluster_groups = master.groupby("Vendor_Canonical")["Vendor"].apply(
                lambda x: sorted(set(x.dropna().astype(str)))
            ).to_dict()
            vendor_clusters = [
                {"canonical": k, "variants": v}
                for k, v in cluster_groups.items()
                if len(v) > 1
            ]

        # ── 5. Merge quote lines ─────────────────────────────────────
        pre_merge_count = len(master)
        master = merge_quote_lines(master)
        lines_merged = pre_merge_count - len(master)

        # ── 6. Dedup ──────────────────────────────────────────────────
        master, dupes_removed = deduplicate(master)

        # ── 7. Anomaly detection ──────────────────────────────────────
        master, anomaly_summary = detect_anomalies(master)

        # ── 8. Data quality metrics ───────────────────────────────────
        quality_metrics = _compute_quality_metrics(master)

        # ── 9. Timeline data ──────────────────────────────────────────
        timeline_data = _compute_timeline(master)

        # ── 10. Export CSV ────────────────────────────────────────────
        csv_name = f"master_{job_id}.csv"
        csv_path = job_dir / csv_name
        master.to_csv(csv_path, index=False, encoding="utf-8")

        # ── 11. Export Excel ──────────────────────────────────────────
        audit_data = {
            "total_rows_ingested": total_rows_ingested,
            "tabs_processed": tab_count,
            "final_rows": len(master),
            "unique_vendors": unique_vendors,
            "quote_lines_merged": lines_merged,
            "duplicates_removed": dupes_removed,
            "unmapped_columns": all_unmapped,
            "columns": list(master.columns),
        }

        xlsx_path = export_rich_excel(
            master_df=master, audit_data=audit_data,
            vendor_clusters=vendor_clusters, mapping_log=mapping_log,
            job_dir=job_dir, job_id=job_id,
        )

        # ── 12. Preview ──────────────────────────────────────────────
        preview = master.head(50).fillna("").to_dict(orient="records")
        columns = [c for c in master.columns if c != "_anomaly_flags"]

        # Count flagged rows
        flagged_count = int((master["_anomaly_flags"] != "").sum()) if "_anomaly_flags" in master.columns else 0

        return {
            "job_id": job_id,
            "csv_filename": csv_name,
            "xlsx_filename": xlsx_path.name,
            "audit": {**audit_data, "anomalies_flagged": flagged_count},
            "vendor_clusters": vendor_clusters,
            "anomalies": anomaly_summary,
            "quality": quality_metrics,
            "timeline": timeline_data,
            "preview": preview,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Pipeline error for job %s", job_id)
        raise HTTPException(500, f"Pipeline error: {exc}")


# =====================================================================
# Download
# =====================================================================
@app.get("/api/download/{job_id}/{filename}")
async def download_file(job_id: str, filename: str):
    file_path = WORK_DIR / job_id / filename
    if not file_path.exists():
        raise HTTPException(404, "File not found.")
    suffix = Path(filename).suffix.lower()
    media_type = {
        ".csv": "text/csv",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }.get(suffix, "application/octet-stream")
    return FileResponse(path=str(file_path), filename=filename, media_type=media_type)


# =====================================================================
# Helpers
# =====================================================================
def _load_single_csv(path: str):
    df = pd.read_csv(path, encoding="utf-8-sig")
    df = _clean_frame(df, "csv", Path(path).name)
    df["meta_source_tab"] = "csv"
    df["meta_source_file"] = Path(path).name
    return df


def _compute_quality_metrics(df: pd.DataFrame) -> dict:
    """Compute per-column completeness and overall quality score."""
    total = len(df)
    if total == 0:
        return {"overall_score": 0, "columns": [], "total_rows": 0}

    cols = []
    for col in df.columns:
        if col.startswith("meta_") or col == "_anomaly_flags":
            continue
        non_null = int(df[col].notna().sum())
        pct = round(non_null / total * 100, 1)
        cols.append({
            "name": col,
            "non_null": non_null,
            "total": total,
            "completeness": pct,
            "rating": "excellent" if pct >= 90 else "fair" if pct >= 60 else "poor",
        })

    cols.sort(key=lambda x: x["completeness"])
    overall = round(sum(c["completeness"] for c in cols) / len(cols), 1) if cols else 0

    return {
        "overall_score": overall,
        "total_rows": total,
        "total_columns": len(cols),
        "columns": cols,
    }


def _compute_timeline(df: pd.DataFrame) -> list[dict]:
    """Build timeline entries for contracts with date data."""
    entries = []

    # Need at least some date column + vendor
    vendor_col = "Vendor_Canonical" if "Vendor_Canonical" in df.columns else "Vendor" if "Vendor" in df.columns else None
    start_col = "Effective_Date" if "Effective_Date" in df.columns else "Contract_Close_Date" if "Contract_Close_Date" in df.columns else None
    end_col = "Expiry_Date" if "Expiry_Date" in df.columns else None

    if not vendor_col or not start_col:
        return entries

    for _, row in df.head(200).iterrows():
        vendor = str(row.get(vendor_col, "") or "")
        start = row.get(start_col)
        end = row.get(end_col) if end_col else None

        if not vendor or pd.isna(start):
            continue

        try:
            start_dt = pd.to_datetime(start)
            start_str = start_dt.strftime("%Y-%m-%d")
        except Exception:
            continue

        end_str = None
        if end and not pd.isna(end):
            try:
                end_str = pd.to_datetime(end).strftime("%Y-%m-%d")
            except Exception:
                pass

        acv = row.get("ACV")
        acv_val = float(acv) if acv and not pd.isna(acv) else None

        entries.append({
            "vendor": vendor,
            "start": start_str,
            "end": end_str,
            "acv": acv_val,
            "product": str(row.get("Product", "") or ""),
            "id": str(row.get("Contract_ID", "") or ""),
        })

    return entries


# ---------------------------------------------------------------------------
# Run server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    print("\n  🚀  DataOrgModel running at http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
