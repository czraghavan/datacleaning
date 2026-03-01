"""
test_layers.py — Unit tests for each layer in the 10-layer pipeline.

Uses synthetic inline DataFrames — no external test files needed.
Run with: python -m pytest tests/test_layers.py -v
"""

import json
import tempfile
import os
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Layer 1: Ingestion
# ---------------------------------------------------------------------------
from src.layers.ingestion import ingest_excel, ingest_csv, _compute_hash


class TestIngestion:
    """Tests for Layer 1 — Raw Ingestion."""

    def test_ingest_excel_returns_sheets(self, tmp_path):
        """Ingesting an Excel file returns sheet records with metadata."""
        # Create a test Excel file
        df = pd.DataFrame({"A": ["1", "2"], "B": ["x", "y"]})
        xlsx_path = tmp_path / "test.xlsx"
        df.to_excel(xlsx_path, index=False, sheet_name="Sheet1")

        sheets = ingest_excel(str(xlsx_path))
        assert len(sheets) == 1

        sheet = sheets[0]
        assert "dataset_id" in sheet
        assert "sheet_id" in sheet
        assert "content_hash" in sheet
        assert "upload_timestamp" in sheet
        assert sheet["row_count"] == 2
        assert "A" in sheet["original_columns"]
        assert "B" in sheet["original_columns"]

    def test_content_hash_is_deterministic(self):
        """Same DataFrame should produce the same hash."""
        df = pd.DataFrame({"x": ["1", "2", "3"], "y": ["a", "b", "c"]})
        h1 = _compute_hash(df)
        h2 = _compute_hash(df)
        assert h1 == h2

    def test_content_hash_changes_with_data(self):
        """Different data should produce different hashes."""
        df1 = pd.DataFrame({"x": ["1"]})
        df2 = pd.DataFrame({"x": ["2"]})
        assert _compute_hash(df1) != _compute_hash(df2)

    def test_ingest_csv(self, tmp_path):
        """Ingesting a CSV file returns one sheet record."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("col1,col2\na,b\nc,d\n")

        sheets = ingest_csv(str(csv_path))
        assert len(sheets) == 1
        assert sheets[0]["row_count"] == 2

    def test_empty_sheet_skipped(self, tmp_path):
        """Empty sheets are excluded from results."""
        xlsx_path = tmp_path / "empty.xlsx"
        with pd.ExcelWriter(xlsx_path) as writer:
            pd.DataFrame({"A": ["data"]}).to_excel(writer, sheet_name="HasData", index=False)
            pd.DataFrame().to_excel(writer, sheet_name="Empty", index=False)

        sheets = ingest_excel(str(xlsx_path))
        assert len(sheets) == 1
        assert sheets[0]["sheet_name"] == "HasData"


# ---------------------------------------------------------------------------
# Layer 2: Profiling
# ---------------------------------------------------------------------------
from src.layers.profiling import profile_sheet


class TestProfiling:
    """Tests for Layer 2 — Profiling & Metadata."""

    def _make_sheet(self, df, name="test_sheet"):
        return {
            "sheet_id": "test-001",
            "dataset_id": "ds-001",
            "sheet_name": name,
            "source_file": "test.xlsx",
            "dataframe": df,
        }

    def test_basic_profiling(self):
        df = pd.DataFrame({
            "id": ["A", "B", "C", "C"],
            "value": ["100", "200", "300", None],
        })
        profile = profile_sheet(self._make_sheet(df))

        assert profile["row_count"] == 4
        assert profile["column_count"] == 2
        assert profile["columns"]["id"]["unique_count"] == 3
        assert profile["columns"]["value"]["null_count"] == 1

    def test_duplicate_detection(self):
        df = pd.DataFrame({
            "a": ["x", "x", "y"],
            "b": ["1", "1", "2"],
        })
        profile = profile_sheet(self._make_sheet(df))
        assert profile["duplicate_row_count"] == 1  # one duplicate pair


# ---------------------------------------------------------------------------
# Layer 3: Canonical Schema
# ---------------------------------------------------------------------------
from src.layers.canonical_schema import load_schema, CanonicalSchema


class TestCanonicalSchema:
    """Tests for Layer 3 — Canonical Schema."""

    def test_load_schema(self):
        schema_path = Path(__file__).parent.parent / "configs" / "canonical_schema_v1.json"
        if not schema_path.exists():
            pytest.skip("canonical_schema_v1.json not found")

        schema = load_schema(str(schema_path))
        assert schema.version == "v1"
        assert len(schema.fields) > 0

    def test_required_fields(self):
        schema_path = Path(__file__).parent.parent / "configs" / "canonical_schema_v1.json"
        if not schema_path.exists():
            pytest.skip("canonical_schema_v1.json not found")

        schema = load_schema(str(schema_path))
        required = schema.get_required_fields()
        assert "contract_id" in required
        assert "total_value" in required

    def test_alias_lookup(self):
        schema_path = Path(__file__).parent.parent / "configs" / "canonical_schema_v1.json"
        if not schema_path.exists():
            pytest.skip("canonical_schema_v1.json not found")

        schema = load_schema(str(schema_path))
        assert schema.lookup_alias("contract_id") == "contract_id"
        assert schema.lookup_alias("acv") == "acv"
        assert schema.lookup_alias("annual contract value") == "acv"

    def test_field_categories(self):
        schema_path = Path(__file__).parent.parent / "configs" / "canonical_schema_v1.json"
        if not schema_path.exists():
            pytest.skip("canonical_schema_v1.json not found")

        schema = load_schema(str(schema_path))
        financial = schema.get_financial_fields()
        assert "total_value" in financial
        assert "acv" in financial

        dates = schema.get_date_fields()
        assert "start_date" in dates
        assert "end_date" in dates


# ---------------------------------------------------------------------------
# Layer 4: Schema Mapping
# ---------------------------------------------------------------------------
from src.layers.schema_mapping import suggest_mappings, MappingConfig


class TestSchemaMapping:
    """Tests for Layer 4 — Schema Mapping."""

    def _make_sheets(self):
        df = pd.DataFrame({
            "Contract ID": ["C001", "C002"],
            "Annual Cost": ["$10,000", "$20,000"],
            "Start Date": ["2024-01-01", "2024-06-01"],
            "Random Column": ["x", "y"],
        })
        return [{
            "sheet_id": "s-001",
            "dataset_id": "ds-001",
            "sheet_name": "Sheet1",
            "source_file": "test.xlsx",
            "dataframe": df,
        }]

    def test_suggest_mappings(self):
        schema_path = Path(__file__).parent.parent / "configs" / "canonical_schema_v1.json"
        if not schema_path.exists():
            pytest.skip("canonical_schema_v1.json not found")

        schema = load_schema(str(schema_path))
        sheets = self._make_sheets()
        suggestions = suggest_mappings(sheets, schema)

        assert "s-001" in suggestions
        suggested = suggestions["s-001"]["suggested"]
        # Contract ID should be mapped
        assert "Contract ID" in suggested

    def test_mapping_config_serialization(self):
        config = MappingConfig(dataset_id="ds-001", version="v1")
        config.set_mapping("contract_id", "s-001", "Contract ID")
        config.set_mapping("total_value", "s-001", "Amount")

        data = config.to_dict()
        loaded = MappingConfig.from_dict(data)

        assert loaded.dataset_id == "ds-001"
        assert loaded.get_mapping("contract_id") == ("s-001", "Contract ID")

    def test_required_field_validation(self):
        config = MappingConfig(dataset_id="ds-001")
        config.set_mapping("contract_id", "s-001", "ID")
        # Missing total_value, start_date, end_date
        assert not config.is_complete()
        assert "total_value" in config.get_unmapped_required()


# ---------------------------------------------------------------------------
# Layer 5: Transformation
# ---------------------------------------------------------------------------
from src.layers.transformation import classify_rows, DatasetClassification, compute_derived_fields


class TestTransformation:
    """Tests for Layer 5 — Transformation Engine."""

    def test_contract_level_classification(self):
        df = pd.DataFrame({"contract_id": ["A", "B", "C", "D", "E"]})
        result = classify_rows(df)
        assert result.level == DatasetClassification.CONTRACT_LEVEL

    def test_line_item_classification(self):
        df = pd.DataFrame({"contract_id": ["A", "A", "A", "B", "B"]})
        result = classify_rows(df)
        assert result.level == DatasetClassification.LINE_ITEM_LEVEL

    def test_derived_date_diff(self):
        df = pd.DataFrame({
            "start_date": ["2024-01-01", "2024-06-01"],
            "end_date": ["2025-01-01", "2025-06-01"],
            "total_value": [10000, 20000],
        })
        config = {
            "fields": {
                "contract_duration_days": {
                    "operation": "date_diff_days",
                    "start": "start_date",
                    "end": "end_date",
                }
            }
        }
        result = compute_derived_fields(df, config)
        assert "contract_duration_days" in result.columns
        assert result["contract_duration_days"].iloc[0] == 366  # 2024 is leap year


# ---------------------------------------------------------------------------
# Layer 6: Validation
# ---------------------------------------------------------------------------
from src.layers.validation import validate


class TestValidation:
    """Tests for Layer 6 — Validation & Integrity."""

    def _load_val_config(self):
        path = Path(__file__).parent.parent / "configs" / "validation_rules_v1.json"
        if not path.exists():
            pytest.skip("validation_rules_v1.json not found")
        with open(path) as f:
            return json.load(f)

    def test_valid_data_passes(self):
        df = pd.DataFrame({
            "contract_id": ["A", "B"],
            "total_value": [1000.0, 2000.0],
            "start_date": ["2024-01-01", "2024-06-01"],
            "end_date": ["2025-01-01", "2025-06-01"],
        })
        config = self._load_val_config()
        result = validate(df, config)
        assert result.passed

    def test_missing_required_field_fails(self):
        df = pd.DataFrame({
            "contract_id": ["A"],
            # Missing total_value, start_date, end_date
        })
        config = self._load_val_config()
        result = validate(df, config)
        assert not result.passed
        assert any("required" in e["type"] for e in result.errors)

    def test_end_before_start_detected(self):
        df = pd.DataFrame({
            "contract_id": ["A"],
            "total_value": [1000.0],
            "start_date": ["2025-06-01"],
            "end_date": ["2024-01-01"],  # Before start
        })
        config = self._load_val_config()
        result = validate(df, config)
        assert any("end_before_start" in e["type"] for e in result.errors)


# ---------------------------------------------------------------------------
# Layer 8: ML Interface
# ---------------------------------------------------------------------------
from src.layers.ml_interface import validate_ml_contract, prepare_for_ml, MLContractViolation


class TestMLInterface:
    """Tests for Layer 8 — ML Interface Contract."""

    def test_valid_ml_input(self):
        df = pd.DataFrame({
            "contract_id": ["A", "B"],
            "total_value": [1000.0, 2000.0],
            "start_date": ["2024-01-01", "2024-06-01"],
            "end_date": ["2025-01-01", "2025-06-01"],
        })
        valid, violations = validate_ml_contract(df)
        assert valid

    def test_missing_column_fails(self):
        df = pd.DataFrame({"contract_id": ["A"]})
        valid, violations = validate_ml_contract(df)
        assert not valid

    def test_prepare_rejects_invalid(self):
        df = pd.DataFrame({"some_col": [1]})
        with pytest.raises(MLContractViolation):
            prepare_for_ml(df)


# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
from src.utils import parse_currency, parse_date, parse_boolean


class TestUtils:
    """Tests for shared utility functions."""

    def test_parse_currency_basic(self):
        assert parse_currency("$1,000.00") == 1000.0
        assert parse_currency("£50k") == 50000.0
        assert parse_currency("1.2M") == 1200000.0
        assert parse_currency("€1 234") == 1234.0

    def test_parse_currency_na(self):
        assert parse_currency(None) is None
        assert parse_currency("N/A") is None
        assert parse_currency("") is None

    def test_parse_date(self):
        dt = parse_date("2024-01-15")
        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_parse_date_auto_renew(self):
        assert parse_date("Auto-Renew") is None
        assert parse_date("Evergreen") is None

    def test_parse_boolean(self):
        assert parse_boolean("Yes") is True
        assert parse_boolean("No") is False
        assert parse_boolean("1") is True
        assert parse_boolean("random") is None
