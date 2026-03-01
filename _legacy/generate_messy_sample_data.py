#!/usr/bin/env python3
"""
Generate a sample 'messy' Excel file for testing the pipeline.

Contains:
  - CRM Accounts: contract-level data with messy column names (matches Churn model format template)
  - Line Items: line-item level data with Account_Ref for joining; multiple rows per account

Use as example input: upload this file, apply "Churn model format" template, confirm, transform.
Output should be contract-level canonical data suitable for churn modeling.
"""

import pandas as pd
from pathlib import Path

# Contract-level sheet: messy headers that map to canonical via template/schema
accounts = pd.DataFrame({
    "Account ID": ["ACC-101", "ACC-102", "ACC-103", "ACC-104"],
    "Close Date": ["2023-01-15", "2023-03-22", "2022-11-05", "2023-06-10"],
    "Vendor": ["Acme Corp", "Globex", "Initech", "Acme Corp"],
    "Total Value": ["50000", "12000", "75000", "32000"],
    "Start Date": ["2023-01-01", "2023-03-01", "2022-11-01", "2023-06-01"],
    "End Date": ["2024-01-01", "2024-03-01", "2023-11-01", "2024-06-01"],
    "Term (months)": ["12", "12", "12", "12"],
    "Auto Renew": ["Yes", "No", "Yes", ""],
})

# Add a contract ID column (often different from account in real data)
accounts.insert(0, "Contract #", ["C-101", "C-102", "C-103", "C-104"])

# Line-item sheet: multiple rows per account for aggregation test
line_items = pd.DataFrame({
    "Account_Ref": ["ACC-101", "ACC-101", "ACC-102", "ACC-102", "ACC-103", "ACC-104"],
    "Contract_Ref": ["C-101", "C-101", "C-102", "C-102", "C-103", "C-104"],
    "Amount": [25000, 25000, 6000, 6000, 75000, 32000],
    "Product": ["License A", "Support", "License B", "Support", "Full Suite", "License A"],
})

out_path = Path(__file__).parent / "messy_contract_data.xlsx"
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    accounts.to_excel(writer, sheet_name="CRM Accounts", index=False)
    line_items.to_excel(writer, sheet_name="Line Items", index=False)

print(f"Sample messy data written to: {out_path}")
print("  Sheets: 'CRM Accounts' (contract-level), 'Line Items' (line-item level)")
print("  Use with pipeline: upload file → apply 'Churn model format' template → map Contract # → contract_id, etc. → Confirm & Transform")
