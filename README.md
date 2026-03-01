# Pareto — Contract Data Merger

A streamlined web tool for merging and consolidating contract data from multiple spreadsheets. Upload Excel or CSV files, map columns visually, and build a unified master sheet — matched by **Account ID + Close Date** as the composite key.

---

## ✨ Features

- **Multi-sheet Excel/CSV upload** — Drag-and-drop interface that auto-filters junk sheets and pivot tables
- **Visual column mapping** — Map your raw columns to standardized fields with sample value previews and auto-detection of key columns
- **Composite key merging** — Joins data from multiple sheets/files using `(account_id, close_date)` as the unique contract identifier
- **Conflict detection** — If two sheets have conflicting values for the same contract + column, a clear error is raised with examples
- **Incremental append** — Upload additional files over time to add new columns to your master sheet without losing existing data
- **Quality dashboard** — Completeness metrics per column with a visual ring chart
- **One-click export** — Download the master sheet as CSV or Excel

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python3 server.py
```

Then open **http://localhost:8000** in your browser.

## 📋 How It Works

### 1. Upload
Drop an Excel file (multi-sheet) or CSV. The system parses all sheets, filters out empty/pivot sheets, and shows you what's inside.

### 2. Map & Select
- Select which sheets to include using the chip toggles
- Map each source column to a target field name
- `account_id` and `close_date` are **required** — they form the composite key that identifies each unique contract

### 3. Master Sheet
The selected sheets are merged via an outer join on `(account_id, close_date)`:
- Rows with matching keys get their columns combined into a single row
- Non-overlapping rows are preserved with nulls for missing columns
- If a column name appears in multiple sheets, the values must match for overlapping keys — otherwise a conflict error is shown

### 4. Append More Data
From the master sheet view, upload additional files to add more columns. The new data is matched by the same composite key and merged into the existing master.

## 🏗 Architecture

```
├── server.py           # FastAPI server with 5 API endpoints
├── requirements.txt
├── src/
│   ├── ingestion.py    # Excel/CSV parsing + sheet filtering
│   └── merger.py       # Composite-key merge + conflict validation
├── frontend/
│   ├── index.html      # Single-page app (3-step flow)
│   ├── style.css       # Glassmorphism design
│   └── app.js          # Upload, map, merge, append, download logic
└── _legacy/            # Previous pipeline code (archived)
```

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/upload` | POST | Parse uploaded file, return sheet info + column samples |
| `/api/merge` | POST | Apply column mappings and merge selected sheets |
| `/api/append/upload` | POST | Upload additional file for appending |
| `/api/append/confirm` | POST | Apply mappings and append to master |
| `/api/download/{job_id}` | GET | Download master sheet (CSV or Excel) |

## 🛠 Tech Stack

- **Backend:** Python, FastAPI, Pandas, OpenPyXL
- **Frontend:** Vanilla HTML/CSS/JS with glassmorphism design
- **No database required** — session-based with file export
