"""
server.py — FastAPI server for the Contract Data Merger.

Run:  python3 server.py
Open: http://localhost:8000

Flow:
  1. POST /api/upload    — Ingest Excel/CSV, return sheets + columns
  2. POST /api/merge     — Map columns, select sheets, merge by composite key
  3. POST /api/append    — Upload more files, add columns to master
  4. GET  /api/download  — Download master sheet
"""

import logging
import tempfile
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from src.ingestion import ingest_file
from src.merger import (
    KEY_COLS,
    apply_column_mapping,
    append_to_master,
    get_master_summary,
    merge_sheets,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ContractMerger")

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(title="Contract Data Merger", version="1.0.0")

STATIC_DIR = Path(__file__).parent / "frontend"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory session store
_sessions: dict[str, dict] = {}


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return (STATIC_DIR / "index.html").read_text()


# =====================================================================
# Upload — Parse file, return sheet info
# =====================================================================


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """Ingest uploaded file and return sheet metadata."""
    job_id = str(uuid.uuid4())[:8]

    suffix = Path(file.filename).suffix
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    contents = await file.read()
    tmp.write(contents)
    tmp.close()

    try:
        sheets = ingest_file(tmp.name)
        if not sheets:
            raise HTTPException(
                status_code=400,
                detail="No usable data found in file.",
            )

        # Store in session
        _sessions[job_id] = {
            "sheets": sheets,
            "filename": file.filename,
            "master": None,
        }

        # Build response
        sheet_info = []
        for s in sheets:
            sheet_info.append({
                "sheet_name": s["sheet_name"],
                "row_count": s["row_count"],
                "column_count": s["column_count"],
                "columns": s["columns"],
                "sample_values": s["sample_values"],
            })

        return {
            "job_id": job_id,
            "filename": file.filename,
            "sheets": sheet_info,
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Upload failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# =====================================================================
# Merge — Apply mappings and merge selected sheets
# =====================================================================


class MergeRequest(BaseModel):
    job_id: str
    selected_sheets: list[str]
    mappings: dict[str, str | None]


@app.post("/api/merge")
async def merge_selected(req: MergeRequest):
    """Map columns and merge selected sheets by composite key."""
    session = _sessions.get(req.job_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session '{req.job_id}' not found.")

    try:
        sheets = session["sheets"]

        # Filter to selected sheets
        selected = [s for s in sheets if s["sheet_name"] in req.selected_sheets]
        if not selected:
            raise HTTPException(
                status_code=400,
                detail="No sheets selected. Please select at least one.",
            )

        # Build clean mapping (skip null/empty)
        clean_mapping = {
            raw: target
            for raw, target in req.mappings.items()
            if target and target.strip()
        }

        # Validate that account_id and close_date are mapped
        mapped_targets = set(clean_mapping.values())
        missing_keys = [k for k in KEY_COLS if k not in mapped_targets]
        if missing_keys:
            raise HTTPException(
                status_code=400,
                detail=f"Required key column(s) not mapped: {missing_keys}. "
                f"Please map columns to 'account_id' and 'close_date'.",
            )

        # Apply mapping to each selected sheet
        mapped_dfs = []
        for s in selected:
            # Build sheet-specific mapping (only columns that exist in this sheet)
            sheet_mapping = {
                raw: target
                for raw, target in clean_mapping.items()
                if raw in s["dataframe"].columns
            }
            if not sheet_mapping:
                continue
            mapped_df = apply_column_mapping(s["dataframe"].copy(), sheet_mapping)
            # Only keep if it has at least one key column
            if any(k in mapped_df.columns for k in KEY_COLS):
                mapped_dfs.append(mapped_df)

        if not mapped_dfs:
            raise HTTPException(
                status_code=400,
                detail="No sheets contain the required key columns after mapping.",
            )

        # Merge all mapped DataFrames
        master = merge_sheets(mapped_dfs)
        session["master"] = master

        # Save to disk
        _save_master(req.job_id, master)

        # Build response
        preview = (
            master.head(100)
            .replace({np.nan: None})
            .to_dict(orient="records")
        )

        summary = get_master_summary(master)

        return {
            "job_id": req.job_id,
            "status": "success",
            "summary": summary,
            "preview": preview,
            "columns": list(master.columns),
            "sheets_merged": len(mapped_dfs),
        }

    except HTTPException:
        raise
    except ValueError as exc:
        # Conflict errors from merger
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.error("Merge failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# =====================================================================
# Append — Add more data to the master sheet
# =====================================================================


class AppendMappingRequest(BaseModel):
    job_id: str
    mappings: dict[str, str | None]
    selected_sheets: list[str] | None = None


@app.post("/api/append/upload")
async def append_upload(file: UploadFile = File(...), job_id: str = Form(...)):
    """Upload additional file(s) to append to the master sheet."""
    session = _sessions.get(job_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session '{job_id}' not found.")
    if session.get("master") is None:
        raise HTTPException(
            status_code=400,
            detail="No master sheet exists yet. Please merge sheets first.",
        )

    suffix = Path(file.filename).suffix
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    contents = await file.read()
    tmp.write(contents)
    tmp.close()

    try:
        sheets = ingest_file(tmp.name)
        if not sheets:
            raise HTTPException(
                status_code=400,
                detail="No usable data found in file.",
            )

        # Store append sheets separately
        session["append_sheets"] = sheets
        session["append_filename"] = file.filename

        sheet_info = []
        for s in sheets:
            sheet_info.append({
                "sheet_name": s["sheet_name"],
                "row_count": s["row_count"],
                "column_count": s["column_count"],
                "columns": s["columns"],
                "sample_values": s["sample_values"],
            })

        return {
            "job_id": job_id,
            "filename": file.filename,
            "sheets": sheet_info,
            "existing_columns": list(session["master"].columns),
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Append upload failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/append/confirm")
async def append_confirm(req: AppendMappingRequest):
    """Apply mappings and append new data to the master sheet."""
    session = _sessions.get(req.job_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session '{req.job_id}' not found.")
    if session.get("master") is None:
        raise HTTPException(status_code=400, detail="No master sheet to append to.")
    if not session.get("append_sheets"):
        raise HTTPException(status_code=400, detail="No append data uploaded.")

    try:
        append_sheets = session["append_sheets"]

        # Filter to selected sheets
        if req.selected_sheets:
            append_sheets = [
                s for s in append_sheets if s["sheet_name"] in req.selected_sheets
            ]

        if not append_sheets:
            raise HTTPException(status_code=400, detail="No sheets selected.")

        # Build clean mapping
        clean_mapping = {
            raw: target
            for raw, target in req.mappings.items()
            if target and target.strip()
        }

        # Apply mapping and merge append sheets together first
        mapped_dfs = []
        for s in append_sheets:
            sheet_mapping = {
                raw: target
                for raw, target in clean_mapping.items()
                if raw in s["dataframe"].columns
            }
            if not sheet_mapping:
                continue
            mapped_df = apply_column_mapping(s["dataframe"].copy(), sheet_mapping)
            if any(k in mapped_df.columns for k in KEY_COLS):
                mapped_dfs.append(mapped_df)

        if not mapped_dfs:
            raise HTTPException(
                status_code=400,
                detail="No valid data after applying column mappings.",
            )

        # Merge the appended sheets together, then append to master
        if len(mapped_dfs) == 1:
            new_data = mapped_dfs[0]
        else:
            new_data = merge_sheets(mapped_dfs)

        master = session["master"]
        updated_master = append_to_master(master, new_data)
        session["master"] = updated_master

        # Clean up append data
        session.pop("append_sheets", None)
        session.pop("append_filename", None)

        # Save to disk
        _save_master(req.job_id, updated_master)

        preview = (
            updated_master.head(100)
            .replace({np.nan: None})
            .to_dict(orient="records")
        )

        summary = get_master_summary(updated_master)

        return {
            "job_id": req.job_id,
            "status": "success",
            "summary": summary,
            "preview": preview,
            "columns": list(updated_master.columns),
            "message": f"Appended {len(mapped_dfs)} sheet(s). "
                       f"Master now has {len(updated_master)} rows, "
                       f"{len(updated_master.columns)} columns.",
        }

    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.error("Append failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


# =====================================================================
# Download
# =====================================================================


@app.get("/api/download/{job_id}")
async def download_master(job_id: str, format: str = "csv"):
    """Download the master sheet."""
    session = _sessions.get(job_id)
    if not session or session.get("master") is None:
        # Try from disk
        csv_path = OUTPUT_DIR / f"{job_id}_master.csv"
        xlsx_path = OUTPUT_DIR / f"{job_id}_master.xlsx"
        if format == "xlsx" and xlsx_path.exists():
            return FileResponse(
                str(xlsx_path),
                filename=f"master_sheet.xlsx",
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        elif csv_path.exists():
            return FileResponse(
                str(csv_path),
                filename=f"master_sheet.csv",
                media_type="text/csv",
            )
        raise HTTPException(status_code=404, detail="No master sheet found.")

    master = session["master"]

    if format == "xlsx":
        path = OUTPUT_DIR / f"{job_id}_master.xlsx"
        master.to_excel(path, index=False, engine="openpyxl")
        return FileResponse(
            str(path),
            filename="master_sheet.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        path = OUTPUT_DIR / f"{job_id}_master.csv"
        master.to_csv(path, index=False)
        return FileResponse(
            str(path),
            filename="master_sheet.csv",
            media_type="text/csv",
        )


# =====================================================================
# Helpers
# =====================================================================


def _save_master(job_id: str, df: pd.DataFrame) -> None:
    """Persist master sheet to disk in both CSV and Excel formats."""
    csv_path = OUTPUT_DIR / f"{job_id}_master.csv"
    xlsx_path = OUTPUT_DIR / f"{job_id}_master.xlsx"
    df.to_csv(csv_path, index=False)
    try:
        df.to_excel(xlsx_path, index=False, engine="openpyxl")
    except Exception as exc:
        logger.warning("Excel export failed: %s", exc)


# ---------------------------------------------------------------------------
# Run server
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    print("\n  🚀  Contract Data Merger running at http://localhost:8000\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
