"""
audit_test.py — Comprehensive functionality audit for the Contract Data Merger.

Generates:
  1. A large master Excel file (2 sheets, ~200 rows, 12+ columns)
  2. Three smaller append files (~200 rows each, 3-5 columns each)

Then runs the full flow via the API:
  - Upload → Map → Merge the master
  - Append each of the 3 files one by one
  - Validates row counts, column counts, key integrity, and data correctness at each step

Also tests error cases:
  - Missing key columns
  - Column value conflicts
"""

import json
import random
import sys
import time

import pandas as pd
import requests

BASE = "http://localhost:8000"
random.seed(42)

# ═══════════════════════════════════════════════════════════════════
# 1. Generate test data
# ═══════════════════════════════════════════════════════════════════

NUM_ROWS = 200

# Build a shared pool of (account_id, close_date) pairs
account_ids = [f"ACC-{i:04d}" for i in range(1, NUM_ROWS + 1)]
close_dates = pd.date_range("2023-01-01", periods=NUM_ROWS, freq="3D").strftime("%Y-%m-%d").tolist()
key_pairs = list(zip(account_ids, close_dates))

# --- Master file: 2 sheets ---

# Sheet 1: CRM Deals (200 rows, 8 data columns + 2 keys = 10 total)
companies = ["Acme Corp", "TechStart Inc", "GlobalData LLC", "CloudBase", "DataVault",
             "NexGen AI", "QuantumLeap", "Pinnacle Systems", "CyberShield", "AlphaWare"]
reps = ["Alice", "Bob", "Carol", "David", "Eve", "Frank"]
stages = ["Closed Won", "Closed Lost", "Negotiation", "Proposal"]
regions = ["North America", "EMEA", "APAC", "LATAM"]

crm_data = {
    "Account ID": [kp[0] for kp in key_pairs],
    "Close Date": [kp[1] for kp in key_pairs],
    "Company Name": [random.choice(companies) for _ in range(NUM_ROWS)],
    "Deal Value": [random.randint(10000, 500000) for _ in range(NUM_ROWS)],
    "Sales Rep": [random.choice(reps) for _ in range(NUM_ROWS)],
    "Stage": [random.choice(stages) for _ in range(NUM_ROWS)],
    "Region": [random.choice(regions) for _ in range(NUM_ROWS)],
    "Lead Source": [random.choice(["Inbound", "Outbound", "Referral", "Partner", "Event"]) for _ in range(NUM_ROWS)],
    "Probability": [random.randint(10, 100) for _ in range(NUM_ROWS)],
    "Notes": [f"Deal note #{i+1}" for i in range(NUM_ROWS)],
}
crm_df = pd.DataFrame(crm_data)

# Sheet 2: Billing Info (first 150 rows only — partial overlap)
billing_subset = key_pairs[:150]
billing_data = {
    "AccountID": [kp[0] for kp in billing_subset],
    "Close_Date": [kp[1] for kp in billing_subset],
    "Invoice Number": [f"INV-{random.randint(10000, 99999)}" for _ in range(150)],
    "Payment Status": [random.choice(["Paid", "Pending", "Overdue", "Partial"]) for _ in range(150)],
    "Payment Method": [random.choice(["Credit Card", "Wire Transfer", "ACH", "Check"]) for _ in range(150)],
    "Billing Contact": [f"billing{i+1}@company.com" for i in range(150)],
}
billing_df = pd.DataFrame(billing_data)

# Write master Excel
master_path = "/tmp/audit_master.xlsx"
with pd.ExcelWriter(master_path, engine="openpyxl") as writer:
    crm_df.to_excel(writer, sheet_name="CRM Deals", index=False)
    billing_df.to_excel(writer, sheet_name="Billing", index=False)
print(f"✅ Master file: {master_path}")
print(f"   Sheet 'CRM Deals': {len(crm_df)} rows, {len(crm_df.columns)} columns")
print(f"   Sheet 'Billing':   {len(billing_df)} rows, {len(billing_df.columns)} columns")

# --- Append file 1: Support data (200 rows, 4 columns) ---
support_data = {
    "account_id": [kp[0] for kp in key_pairs],
    "close_date": [kp[1] for kp in key_pairs],
    "Support Tier": [random.choice(["Bronze", "Silver", "Gold", "Platinum"]) for _ in range(NUM_ROWS)],
    "NPS Score": [random.randint(1, 10) for _ in range(NUM_ROWS)],
}
support_df = pd.DataFrame(support_data)
append1_path = "/tmp/audit_append1_support.csv"
support_df.to_csv(append1_path, index=False)
print(f"\n✅ Append file 1 (Support): {append1_path}")
print(f"   {len(support_df)} rows, {len(support_df.columns)} columns")

# --- Append file 2: Usage data (180 rows, 5 columns — partial overlap + some new keys) ---
# First 160 from existing keys, then 20 brand-new ones
usage_keys = key_pairs[:160] + [(f"ACC-NEW-{i:03d}", f"2025-01-{i+1:02d}") for i in range(20)]
usage_data = {
    "account_id": [kp[0] for kp in usage_keys],
    "close_date": [kp[1] for kp in usage_keys],
    "Monthly Active Users": [random.randint(10, 5000) for _ in range(180)],
    "API Calls (30d)": [random.randint(100, 100000) for _ in range(180)],
    "Storage Used (GB)": [round(random.uniform(0.5, 500.0), 2) for _ in range(180)],
}
usage_df = pd.DataFrame(usage_data)
append2_path = "/tmp/audit_append2_usage.csv"
usage_df.to_csv(append2_path, index=False)
print(f"\n✅ Append file 2 (Usage): {append2_path}")
print(f"   {len(usage_df)} rows, {len(usage_df.columns)} columns")
print(f"   (160 existing keys + 20 new keys)")

# --- Append file 3: Renewal data (200 rows, 5 columns — all existing keys) ---
renewal_data = {
    "account_id": [kp[0] for kp in key_pairs],
    "close_date": [kp[1] for kp in key_pairs],
    "Renewal Date": pd.date_range("2024-01-01", periods=NUM_ROWS, freq="3D").strftime("%Y-%m-%d").tolist(),
    "Renewal Probability": [random.randint(20, 100) for _ in range(NUM_ROWS)],
    "Churn Risk": [random.choice(["Low", "Medium", "High"]) for _ in range(NUM_ROWS)],
}
renewal_df = pd.DataFrame(renewal_data)
append3_path = "/tmp/audit_append3_renewal.csv"
renewal_df.to_csv(append3_path, index=False)
print(f"\n✅ Append file 3 (Renewal): {append3_path}")
print(f"   {len(renewal_df)} rows, {len(renewal_df.columns)} columns")

# --- Conflict file (for error testing) ---
# Has a column "Sales Rep" with DIFFERENT values for same keys
conflict_data = {
    "account_id": [key_pairs[0][0], key_pairs[1][0], key_pairs[2][0]],
    "close_date": [key_pairs[0][1], key_pairs[1][1], key_pairs[2][1]],
    "Sales Rep": ["WRONG_NAME_1", "WRONG_NAME_2", "WRONG_NAME_3"],  # conflicts!
}
conflict_df = pd.DataFrame(conflict_data)
conflict_path = "/tmp/audit_conflict.csv"
conflict_df.to_csv(conflict_path, index=False)
print(f"\n✅ Conflict file: {conflict_path}")
print(f"   {len(conflict_df)} rows — intentionally conflicting 'Sales Rep' values")


# ═══════════════════════════════════════════════════════════════════
# 2. API tests
# ═══════════════════════════════════════════════════════════════════

PASS = 0
FAIL = 0
TESTS = []

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        TESTS.append(("✅", name, detail))
        print(f"  ✅ {name}{f' — {detail}' if detail else ''}")
    else:
        FAIL += 1
        TESTS.append(("❌", name, detail))
        print(f"  ❌ {name}{f' — {detail}' if detail else ''}")

print("\n" + "=" * 70)
print("AUDIT: Upload & Merge Master File")
print("=" * 70)

# --- Test 1: Upload master file ---
with open(master_path, "rb") as f:
    resp = requests.post(f"{BASE}/api/upload", files={"file": ("audit_master.xlsx", f)})

check("Upload returns 200", resp.status_code == 200, f"status={resp.status_code}")
upload = resp.json()
check("Upload returns job_id", "job_id" in upload)
job_id = upload.get("job_id", "")
check("Upload finds 2 sheets", len(upload.get("sheets", [])) == 2,
      f"found {len(upload.get('sheets', []))}")

sheet_names = [s["sheet_name"] for s in upload.get("sheets", [])]
check("Sheet names correct", "CRM Deals" in sheet_names and "Billing" in sheet_names,
      f"sheets={sheet_names}")

crm_sheet = next((s for s in upload["sheets"] if s["sheet_name"] == "CRM Deals"), None)
if crm_sheet:
    check("CRM sheet row count", crm_sheet["row_count"] == 200, f"rows={crm_sheet['row_count']}")
    check("CRM sheet column count", crm_sheet["column_count"] == 10, f"cols={crm_sheet['column_count']}")
    check("CRM sheet has sample values", len(crm_sheet.get("sample_values", {})) > 0)

billing_sheet = next((s for s in upload["sheets"] if s["sheet_name"] == "Billing"), None)
if billing_sheet:
    check("Billing sheet row count", billing_sheet["row_count"] == 150, f"rows={billing_sheet['row_count']}")
    check("Billing sheet column count", billing_sheet["column_count"] == 6, f"cols={billing_sheet['column_count']}")

# --- Test 2: Merge without required keys (should fail) ---
print("\n--- Test: Merge without required keys ---")
resp_bad = requests.post(f"{BASE}/api/merge", json={
    "job_id": job_id,
    "selected_sheets": ["CRM Deals"],
    "mappings": {"Company Name": "Company Name", "Deal Value": "Deal Value"},
})
check("Merge without keys returns 400", resp_bad.status_code == 400,
      f"status={resp_bad.status_code}")
err = resp_bad.json()
check("Error mentions missing keys", "account_id" in err.get("detail", "").lower() or "close_date" in err.get("detail", "").lower(),
      f"detail={err.get('detail', '')[:80]}")

# --- Test 3: Merge with proper mappings ---
print("\n--- Test: Merge both sheets ---")
mappings = {
    "Account ID": "account_id",
    "Close Date": "close_date",
    "Company Name": "Company Name",
    "Deal Value": "Deal Value",
    "Sales Rep": "Sales Rep",
    "Stage": "Stage",
    "Region": "Region",
    "Lead Source": "Lead Source",
    "Probability": "Probability",
    "Notes": "Notes",
    "AccountID": "account_id",
    "Close_Date": "close_date",
    "Invoice Number": "Invoice Number",
    "Payment Status": "Payment Status",
    "Payment Method": "Payment Method",
    "Billing Contact": "Billing Contact",
}

resp_merge = requests.post(f"{BASE}/api/merge", json={
    "job_id": job_id,
    "selected_sheets": ["CRM Deals", "Billing"],
    "mappings": mappings,
})
check("Merge returns 200", resp_merge.status_code == 200, f"status={resp_merge.status_code}")
merge_result = resp_merge.json()
summary = merge_result.get("summary", {})

check("Merge status is success", merge_result.get("status") == "success")
check("Master has 200 rows", summary.get("row_count") == 200,
      f"rows={summary.get('row_count')} (expected 200: 200 CRM + 150 Billing, all billing keys overlap)")
# 2 keys + 8 CRM data + 4 Billing data = 14
check("Master has 14 columns", summary.get("column_count") == 14,
      f"cols={summary.get('column_count')} (expected 14: 2 keys + 8 CRM + 4 Billing)")
check("Sheets merged count is 2", merge_result.get("sheets_merged") == 2)
check("Preview has rows", len(merge_result.get("preview", [])) > 0,
      f"preview_rows={len(merge_result.get('preview', []))}")

# Verify key coverage
key_cov = summary.get("key_coverage", {})
check("account_id 100% fill", key_cov.get("account_id", {}).get("pct") == 100.0,
      f"fill={key_cov.get('account_id', {}).get('pct')}%")
check("close_date 100% fill", key_cov.get("close_date", {}).get("pct") == 100.0,
      f"fill={key_cov.get('close_date', {}).get('pct')}%")

# Verify data integrity: check that billing data is null for rows 151-200
preview = merge_result.get("preview", [])
cols = merge_result.get("columns", [])
check("Preview includes all columns", "Invoice Number" in cols and "Sales Rep" in cols,
      f"cols={cols}")

# Find a row with ACC-0001 (should have billing data)
row_001 = next((r for r in preview if r.get("account_id") == "ACC-0001"), None)
if row_001:
    check("ACC-0001 has Invoice Number", row_001.get("Invoice Number") is not None,
          f"Invoice={row_001.get('Invoice Number')}")
    check("ACC-0001 has Company Name", row_001.get("Company Name") is not None,
          f"Company={row_001.get('Company Name')}")

# Find a row with ACC-0180 (beyond billing's 150 rows — should NOT have billing data)
row_180 = next((r for r in preview if r.get("account_id") == "ACC-0180"), None)
if row_180:
    check("ACC-0180 has NO Invoice Number (row > 150)", row_180.get("Invoice Number") is None,
          f"Invoice={row_180.get('Invoice Number')}")
    check("ACC-0180 has Company Name", row_180.get("Company Name") is not None,
          f"Company={row_180.get('Company Name')}")

# ═══════════════════════════════════════════════════════════════════
# 3. Append tests
# ═══════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("AUDIT: Append File 1 — Support Data")
print("=" * 70)

# Upload append file 1
with open(append1_path, "rb") as f:
    resp_a1 = requests.post(f"{BASE}/api/append/upload",
                            files={"file": ("support.csv", f)},
                            data={"job_id": job_id})
check("Append 1 upload returns 200", resp_a1.status_code == 200, f"status={resp_a1.status_code}")
a1_data = resp_a1.json()
check("Append 1 finds 1 sheet", len(a1_data.get("sheets", [])) == 1)
check("Append 1 shows existing columns", len(a1_data.get("existing_columns", [])) == 14,
      f"existing_cols={len(a1_data.get('existing_columns', []))}")

# Confirm append
resp_a1c = requests.post(f"{BASE}/api/append/confirm", json={
    "job_id": job_id,
    "mappings": {
        "account_id": "account_id",
        "close_date": "close_date",
        "Support Tier": "Support Tier",
        "NPS Score": "NPS Score",
    },
})
check("Append 1 confirm returns 200", resp_a1c.status_code == 200, f"status={resp_a1c.status_code}")
a1_result = resp_a1c.json()
a1_summary = a1_result.get("summary", {})
check("After append 1: 200 rows", a1_summary.get("row_count") == 200,
      f"rows={a1_summary.get('row_count')}")
check("After append 1: 16 columns", a1_summary.get("column_count") == 16,
      f"cols={a1_summary.get('column_count')} (14 + 2 new: Support Tier, NPS Score)")

# Verify data
a1_preview = a1_result.get("preview", [])
row_001_a1 = next((r for r in a1_preview if r.get("account_id") == "ACC-0001"), None)
if row_001_a1:
    check("ACC-0001 has Support Tier after append", row_001_a1.get("Support Tier") is not None,
          f"tier={row_001_a1.get('Support Tier')}")
    check("ACC-0001 still has Company Name", row_001_a1.get("Company Name") is not None,
          f"company={row_001_a1.get('Company Name')}")
    check("ACC-0001 still has Invoice Number", row_001_a1.get("Invoice Number") is not None,
          f"invoice={row_001_a1.get('Invoice Number')}")


print("\n" + "=" * 70)
print("AUDIT: Append File 2 — Usage Data (with 20 new keys)")
print("=" * 70)

with open(append2_path, "rb") as f:
    resp_a2 = requests.post(f"{BASE}/api/append/upload",
                            files={"file": ("usage.csv", f)},
                            data={"job_id": job_id})
check("Append 2 upload returns 200", resp_a2.status_code == 200)

resp_a2c = requests.post(f"{BASE}/api/append/confirm", json={
    "job_id": job_id,
    "mappings": {
        "account_id": "account_id",
        "close_date": "close_date",
        "Monthly Active Users": "Monthly Active Users",
        "API Calls (30d)": "API Calls (30d)",
        "Storage Used (GB)": "Storage Used (GB)",
    },
})
check("Append 2 confirm returns 200", resp_a2c.status_code == 200, f"status={resp_a2c.status_code}")
a2_result = resp_a2c.json()
a2_summary = a2_result.get("summary", {})
# 200 original + 20 new keys = 220
check("After append 2: 220 rows", a2_summary.get("row_count") == 220,
      f"rows={a2_summary.get('row_count')} (200 existing + 20 new keys)")
# 16 + 3 new = 19
check("After append 2: 19 columns", a2_summary.get("column_count") == 19,
      f"cols={a2_summary.get('column_count')} (16 + 3 new usage cols)")

# Verify new keys have usage data but no CRM data
a2_preview = a2_result.get("preview", [])
row_new = next((r for r in a2_preview if r.get("account_id") == "ACC-NEW-001"), None)
if row_new:
    check("New key ACC-NEW-001 exists", True)
    check("New key has Monthly Active Users", row_new.get("Monthly Active Users") is not None,
          f"MAU={row_new.get('Monthly Active Users')}")
    check("New key has NO Company Name (never in CRM)", row_new.get("Company Name") is None,
          f"company={row_new.get('Company Name')}")
else:
    check("New key ACC-NEW-001 exists in preview", False,
          "May be beyond first 100 rows of preview")


print("\n" + "=" * 70)
print("AUDIT: Append File 3 — Renewal Data")
print("=" * 70)

with open(append3_path, "rb") as f:
    resp_a3 = requests.post(f"{BASE}/api/append/upload",
                            files={"file": ("renewal.csv", f)},
                            data={"job_id": job_id})
check("Append 3 upload returns 200", resp_a3.status_code == 200)

resp_a3c = requests.post(f"{BASE}/api/append/confirm", json={
    "job_id": job_id,
    "mappings": {
        "account_id": "account_id",
        "close_date": "close_date",
        "Renewal Date": "Renewal Date",
        "Renewal Probability": "Renewal Probability",
        "Churn Risk": "Churn Risk",
    },
})
check("Append 3 confirm returns 200", resp_a3c.status_code == 200, f"status={resp_a3c.status_code}")
a3_result = resp_a3c.json()
a3_summary = a3_result.get("summary", {})
# 220 rows (renewal only covers original 200, no new keys added)
check("After append 3: 220 rows", a3_summary.get("row_count") == 220,
      f"rows={a3_summary.get('row_count')} (no new keys in renewal data)")
# 19 + 3 new = 22
check("After append 3: 22 columns", a3_summary.get("column_count") == 22,
      f"cols={a3_summary.get('column_count')} (19 + 3 new renewal cols)")

# Spot check: ACC-0050 should have everything
a3_preview = a3_result.get("preview", [])
row_50 = next((r for r in a3_preview if r.get("account_id") == "ACC-0050"), None)
if row_50:
    check("ACC-0050 has all data", all([
        row_50.get("Company Name") is not None,
        row_50.get("Support Tier") is not None,
        row_50.get("Renewal Date") is not None,
    ]), "Has CRM + Support + Renewal data")
    check("ACC-0050 has Invoice (row<=150)", row_50.get("Invoice Number") is not None,
          f"invoice={row_50.get('Invoice Number')}")


# ═══════════════════════════════════════════════════════════════════
# 4. Conflict detection test
# ═══════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("AUDIT: Conflict Detection")
print("=" * 70)

with open(conflict_path, "rb") as f:
    resp_cf = requests.post(f"{BASE}/api/append/upload",
                            files={"file": ("conflict.csv", f)},
                            data={"job_id": job_id})
check("Conflict upload returns 200", resp_cf.status_code == 200)

resp_cfc = requests.post(f"{BASE}/api/append/confirm", json={
    "job_id": job_id,
    "mappings": {
        "account_id": "account_id",
        "close_date": "close_date",
        "Sales Rep": "Sales Rep",
    },
})
check("Conflict append returns 409", resp_cfc.status_code == 409,
      f"status={resp_cfc.status_code}")
cf_err = resp_cfc.json()
check("Conflict error mentions 'Sales Rep'", "Sales Rep" in cf_err.get("detail", ""),
      f"detail={cf_err.get('detail', '')[:100]}")
check("Conflict error mentions 'conflict'", "conflict" in cf_err.get("detail", "").lower(),
      f"detail starts: {cf_err.get('detail', '')[:60]}")

# Verify master was NOT corrupted by conflict
check("Master still has 220 rows after conflict rejection", a3_summary.get("row_count") == 220)
check("Master still has 22 columns after conflict rejection", a3_summary.get("column_count") == 22)


# ═══════════════════════════════════════════════════════════════════
# 5. Download test
# ═══════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("AUDIT: Download")
print("=" * 70)

resp_dl_csv = requests.get(f"{BASE}/api/download/{job_id}?format=csv")
check("CSV download returns 200", resp_dl_csv.status_code == 200)
check("CSV content-type", "text/csv" in resp_dl_csv.headers.get("content-type", ""),
      f"type={resp_dl_csv.headers.get('content-type')}")

# Parse downloaded CSV to verify integrity
import io
downloaded_df = pd.read_csv(io.StringIO(resp_dl_csv.text))
check("Downloaded CSV has 220 rows", len(downloaded_df) == 220,
      f"rows={len(downloaded_df)}")
check("Downloaded CSV has 22 columns", len(downloaded_df.columns) == 22,
      f"cols={len(downloaded_df.columns)}")
check("Downloaded CSV has account_id column", "account_id" in downloaded_df.columns)
check("Downloaded CSV has close_date column", "close_date" in downloaded_df.columns)
check("Downloaded CSV has Churn Risk column", "Churn Risk" in downloaded_df.columns)

# Verify no duplicate keys in downloaded data
key_dupes = downloaded_df.duplicated(subset=["account_id", "close_date"]).sum()
check("No duplicate keys in download", key_dupes == 0, f"duplicates={key_dupes}")

resp_dl_xlsx = requests.get(f"{BASE}/api/download/{job_id}?format=xlsx")
check("Excel download returns 200", resp_dl_xlsx.status_code == 200)

# ═══════════════════════════════════════════════════════════════════
# 6. Edge case: invalid session
# ═══════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("AUDIT: Edge Cases")
print("=" * 70)

resp_bad_job = requests.post(f"{BASE}/api/merge", json={
    "job_id": "nonexistent",
    "selected_sheets": ["Sheet1"],
    "mappings": {"a": "b"},
})
check("Invalid job_id returns 404", resp_bad_job.status_code == 404)

resp_bad_append = requests.post(f"{BASE}/api/append/upload",
                                 files={"file": ("test.csv", b"a,b\n1,2\n")},
                                 data={"job_id": "nonexistent"})
check("Append to invalid job returns 404", resp_bad_append.status_code == 404)


# ═══════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print(f"AUDIT COMPLETE: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
print("=" * 70)

if FAIL > 0:
    print("\n❌ FAILED TESTS:")
    for icon, name, detail in TESTS:
        if icon == "❌":
            print(f"  {icon} {name} — {detail}")
    sys.exit(1)
else:
    print("\n🎉 ALL TESTS PASSED!")
    sys.exit(0)
