"""
test_pipeline.py — Unit tests for every module in the DataOrgModel pipeline.

Uses synthetic inline DataFrames — no external test files needed.
Run with: python -m pytest tests/ -v
"""

import pandas as pd
import numpy as np
import pytest

# Module imports
from src.ingest import load_excel, load_csv_dir
from src.schema import fuzzy_map_headers
from src.cleaner import Cleaner
from src.vendor import resolve_vendors
from src.merge import concat_all, merge_quote_lines, deduplicate


# =====================================================================
# Schema tests
# =====================================================================
class TestFuzzyMapHeaders:
    """Tests for schema.fuzzy_map_headers."""

    def test_exact_alias_match(self):
        df = pd.DataFrame({"contract_value": [100], "vendor_name": ["Acme"]})
        mapped, unmapped = fuzzy_map_headers(df)
        assert "ACV" in mapped.columns
        assert "Vendor" in mapped.columns
        assert unmapped == []

    def test_fuzzy_match(self):
        df = pd.DataFrame({"Annual_Cost": [500], "Supplier": ["AWS"]})
        mapped, unmapped = fuzzy_map_headers(df)
        assert "ACV" in mapped.columns
        assert "Vendor" in mapped.columns

    def test_unmapped_column(self):
        df = pd.DataFrame({
            "contract_value": [100],
            "totally_random_xyz": ["foo"],
        })
        mapped, unmapped = fuzzy_map_headers(df)
        assert "totally_random_xyz" in unmapped

    def test_meta_columns_preserved(self):
        df = pd.DataFrame({
            "contract_value": [100],
            "meta_source_tab": ["Sheet1"],
            "meta_source_file": ["file.xlsx"],
        })
        mapped, _ = fuzzy_map_headers(df)
        assert "meta_source_tab" in mapped.columns
        assert "meta_source_file" in mapped.columns


# =====================================================================
# Cleaner tests
# =====================================================================
class TestCleaner:
    """Tests for cleaner.Cleaner."""

    def setup_method(self):
        self.cleaner = Cleaner()

    # --- Date normalization ---
    def test_date_parsing(self):
        df = pd.DataFrame({"Effective_Date": ["Jan 15, 2024", "2023-06-01", "12/31/2025"]})
        result = self.cleaner.normalize_dates(df)
        assert result["Effective_Date"].iloc[0] == "2024-01-15"
        assert result["Effective_Date"].iloc[1] == "2023-06-01"
        assert result["Effective_Date"].iloc[2] == "2025-12-31"

    def test_auto_renew_detection(self):
        df = pd.DataFrame({"Renewal_Date": ["Auto-renew", "2024-03-01", "N/A"]})
        result = self.cleaner.normalize_dates(df)
        assert result["is_auto_renew"].iloc[0] == True
        assert result["is_auto_renew"].iloc[1] == False
        assert result["is_auto_renew"].iloc[2] == True

    def test_empty_date(self):
        df = pd.DataFrame({"Effective_Date": [None, pd.NA, ""]})
        result = self.cleaner.normalize_dates(df)
        # Should not crash
        assert len(result) == 3

    # --- Financial scrubbing ---
    def test_currency_stripping(self):
        df = pd.DataFrame({"ACV": ["$1,234.56", "£50k", "1.2M"]})
        result = self.cleaner.scrub_financials(df)
        assert result["ACV"].iloc[0] == 1234.56
        assert result["ACV"].iloc[1] == 50000.0
        assert result["ACV"].iloc[2] == 1200000.0

    def test_acv_calculation(self):
        df = pd.DataFrame({
            "Total_Value": [120000],
            "Term_Months": [24],
        })
        result = self.cleaner.scrub_financials(df)
        assert "ACV" in result.columns
        assert result["ACV"].iloc[0] == 60000.0

    def test_na_financial(self):
        df = pd.DataFrame({"ACV": ["N/A", "TBD", None]})
        result = self.cleaner.scrub_financials(df)
        assert result["ACV"].iloc[0] is None
        assert result["ACV"].iloc[1] is None

    # --- Boolean standardization ---
    def test_boolean_conversion(self):
        df = pd.DataFrame({"Auto_Renew": ["Yes", "No", "Checked"]})
        result = self.cleaner.standardize_booleans(df)
        assert result["Auto_Renew"].iloc[0] == True
        assert result["Auto_Renew"].iloc[1] == False
        assert result["Auto_Renew"].iloc[2] == True

    def test_non_boolean_column_untouched(self):
        df = pd.DataFrame({"Vendor": ["AWS", "Google", "Azure"]})
        result = self.cleaner.standardize_booleans(df)
        assert result["Vendor"].iloc[0] == "AWS"

    # --- Excel error sanitization ---
    def test_excel_errors_replaced(self):
        df = pd.DataFrame({
            "ACV": ["$100", "#REF!", "#VALUE!", "#DIV/0!"],
            "Vendor": ["AWS", "#ERROR!", "#NAME?", "Google"],
        })
        result = self.cleaner.sanitize_errors(df)
        assert result["ACV"].iloc[0] == "$100"
        assert pd.isna(result["ACV"].iloc[1])
        assert pd.isna(result["ACV"].iloc[2])
        assert pd.isna(result["ACV"].iloc[3])
        assert result["Vendor"].iloc[0] == "AWS"
        assert pd.isna(result["Vendor"].iloc[1])
        assert pd.isna(result["Vendor"].iloc[2])
        assert result["Vendor"].iloc[3] == "Google"

    def test_clean_runs_error_sanitization_first(self):
        """Errors should be NaN before date/financial parsing runs."""
        df = pd.DataFrame({
            "ACV": ["#REF!", "$500"],
            "Effective_Date": ["#VALUE!", "2024-01-01"],
        })
        result = self.cleaner.clean(df)
        assert pd.isna(result["ACV"].iloc[0])
        assert result["ACV"].iloc[1] == 500.0


# =====================================================================
# Vendor tests
# =====================================================================
class TestVendorResolution:
    """Tests for vendor.resolve_vendors."""

    def test_basic_clustering(self):
        df = pd.DataFrame({
            "Vendor": ["AWS", "Amazon Web Services", "AWS Cloud", "Google Cloud", "GCP"],
        })
        result = resolve_vendors(df, threshold=70)
        # AWS variants should share a cluster
        aws_cluster = result.loc[result["Vendor"] == "AWS", "Vendor_Cluster_ID"].iloc[0]
        aws2_cluster = result.loc[result["Vendor"] == "Amazon Web Services", "Vendor_Cluster_ID"].iloc[0]
        # They should be in the same cluster
        assert aws_cluster == aws2_cluster

    def test_missing_vendor_column(self):
        df = pd.DataFrame({"Other": [1, 2]})
        result = resolve_vendors(df, vendor_col="Vendor")
        assert "Vendor_Canonical" in result.columns
        assert "Vendor_Cluster_ID" in result.columns

    def test_null_vendors(self):
        df = pd.DataFrame({"Vendor": [None, pd.NA, "Salesforce"]})
        result = resolve_vendors(df)
        assert result["Vendor_Canonical"].iloc[2] == "Salesforce"


# =====================================================================
# Merge tests
# =====================================================================
class TestMerge:
    """Tests for merge.concat_all, merge_quote_lines, deduplicate."""

    def test_concat_outer_join(self):
        df1 = pd.DataFrame({"A": [1], "B": [2]})
        df2 = pd.DataFrame({"A": [3], "C": [4]})
        result = concat_all([df1, df2])
        assert "A" in result.columns
        assert "B" in result.columns
        assert "C" in result.columns
        assert len(result) == 2

    def test_quote_line_merge(self):
        df = pd.DataFrame({
            "Contract_ID": ["C001", "C001", "C002"],
            "Contract_Close_Date": ["2024-01-01", "2024-01-01", "2024-02-01"],
            "ACV": [1000, 2000, 5000],
            "Product": ["Seats", "Support", "Platform"],
        })
        result = merge_quote_lines(df)
        # C001 should be merged: ACV = 3000
        c001 = result[result["Contract_ID"] == "C001"]
        assert len(c001) == 1
        assert c001["ACV"].iloc[0] == 3000

    def test_dedup_keeps_most_complete(self):
        df = pd.DataFrame({
            "Vendor_Canonical": ["AWS", "AWS"],
            "Effective_Date": ["2024-01-01", "2024-01-01"],
            "ACV": [1000, 1000],
            "Notes": ["Important note", None],
        })
        result, removed = deduplicate(df)
        assert removed == 1
        assert len(result) == 1
        # Should keep the row with the note (fewer nulls)
        assert result["Notes"].iloc[0] == "Important note"

    def test_dedup_no_duplicates(self):
        df = pd.DataFrame({
            "Vendor_Canonical": ["AWS", "Google"],
            "Effective_Date": ["2024-01-01", "2024-02-01"],
            "ACV": [1000, 2000],
        })
        result, removed = deduplicate(df)
        assert removed == 0
        assert len(result) == 2

    def test_empty_concat(self):
        result = concat_all([])
        assert result.empty


# =====================================================================
# Integration test
# =====================================================================
class TestEndToEnd:
    """Lightweight integration test for the full pipeline."""

    def test_full_pipeline(self, tmp_path):
        """Create a small Excel file with 2 tabs and run everything."""
        # Create test data
        df1 = pd.DataFrame({
            "Contract ID": ["C001", "C001", "C002"],
            "Close Date": ["2024-01-15", "2024-01-15", "2024-03-01"],
            "Vendor Name": ["AWS", "Amazon Web Services", "Salesforce"],
            "Annual_Cost": ["$10k", "$5k", "£20,000"],
            "Start Date": ["Jan 1 2024", "Jan 1 2024", "Mar 1 2024"],
            "Auto Renew": ["Yes", "Yes", "No"],
        })
        df2 = pd.DataFrame({
            "Quote Line ID": ["C003"],
            "Contract Close Date": ["2024-06-01"],
            "Supplier": ["Google Cloud"],
            "Contract_Value": ["$50,000"],
            "Effective Date": ["2024-06-01"],
            "Renewal Date": ["Auto-renew"],
        })

        # Write to an Excel file with two tabs
        excel_path = tmp_path / "test_contracts.xlsx"
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            df1.to_excel(writer, sheet_name="Q1_Contracts", index=False)
            df2.to_excel(writer, sheet_name="Q2_Contracts", index=False)

        # --- Run pipeline components manually ---
        from src.ingest import load_excel
        from src.schema import fuzzy_map_headers
        from src.cleaner import Cleaner
        from src.vendor import resolve_vendors
        from src.merge import concat_all, merge_quote_lines, deduplicate

        frames = load_excel(str(excel_path))
        assert len(frames) == 2

        mapped = []
        for f in frames:
            m, _ = fuzzy_map_headers(f)
            mapped.append(m)

        master = concat_all(mapped)
        assert len(master) == 4  # 3 + 1

        cleaner = Cleaner()
        master = cleaner.clean(master)
        master = resolve_vendors(master, threshold=70)
        master = merge_quote_lines(master)
        master, dupes = deduplicate(master)

        # Verify output has expected columns
        assert "Contract_ID" in master.columns
        assert "Vendor_Canonical" in master.columns
        assert "is_auto_renew" in master.columns

        # Export
        out = tmp_path / "output.csv"
        master.to_csv(out, index=False)
        assert out.exists()
