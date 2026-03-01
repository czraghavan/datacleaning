"""
pipeline.py — Central orchestrator that chains all 10 layers in sequence.

Persists artifacts at each stage. Ensures deterministic, reproducible execution.

Usage:
    from pipeline import Pipeline
    pipe = Pipeline(configs_dir="configs", artifacts_dir="artifacts")
    result = pipe.run(file_path="data.xlsx")
"""

import json
import logging
from pathlib import Path

import pandas as pd

from src.layers.canonical_output import (persist_canonical_output,
                                         produce_canonical_output)
from src.layers.canonical_schema import load_latest_schema
from src.layers.ingestion import (ingest_csv, ingest_directory, ingest_excel,
                                  persist_raw)
from src.layers.ml_interface import validate_ml_contract
from src.layers.profiling import (cross_sheet_analysis, persist_profiles,
                                  profile_all_sheets)
from src.layers.schema_mapping import (MappingConfig,
                                       apply_suggestions_to_config,
                                       persist_mapping_config,
                                       suggest_mappings)
from src.layers.transformation import (aggregate, classify_rows,
                                       compute_derived_fields,
                                       extract_mapped_columns,
                                       join_entities,
                                       log_transformation)
from src.layers.validation import persist_validation_result, validate
from src.layers.versioning import create_manifest, persist_manifest

logger = logging.getLogger(__name__)


class Pipeline:
    """Central orchestrator for the 10-layer data transformation pipeline."""

    def __init__(
        self,
        configs_dir: str = "configs",
        artifacts_dir: str = "artifacts",
    ):
        self.configs_dir = Path(configs_dir)
        self.artifacts_dir = Path(artifacts_dir)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        file_path: str | None = None,
        dir_path: str | None = None,
        company_id: str | None = None,
        mapping_config: MappingConfig | None = None,
        auto_confirm_mappings: bool = False,
    ) -> dict:
        """Execute the full pipeline from ingestion through canonical output.

        Args:
            file_path: Path to a single .xlsx or .csv file.
            dir_path: Path to a directory of files.
            company_id: Optional company identifier.
            mapping_config: Pre-confirmed mapping config. If None, suggestions are auto-confirmed.
            auto_confirm_mappings: If True and no mapping_config, auto-confirm all suggestions.

        Returns:
            Pipeline result dict with all outputs, profiles, and metadata.
        """
        result = {}

        # ── Layer 1: Raw Ingestion ──────────────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 1 — Raw Ingestion")
        logger.info("=" * 60)

        if file_path:
            if file_path.endswith(".csv"):
                sheets = ingest_csv(file_path, company_id=company_id)
            else:
                sheets = ingest_excel(file_path, company_id=company_id)
        elif dir_path:
            sheets = ingest_directory(dir_path, company_id=company_id)
        else:
            raise ValueError("Must provide either file_path or dir_path")

        if not sheets:
            raise ValueError("No data loaded. Check your input path.")

        dataset_id = sheets[0]["dataset_id"]
        sheets = persist_raw(sheets, str(self.artifacts_dir))
        result["sheets"] = sheets
        result["dataset_id"] = dataset_id

        # ── Layer 2: Profiling & Metadata ──────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 2 — Profiling & Metadata")
        logger.info("=" * 60)

        profiles = profile_all_sheets(sheets)
        cross_analysis = (
            cross_sheet_analysis(profiles, sheets) if len(sheets) > 1 else None
        )
        persist_profiles(profiles, cross_analysis, str(self.artifacts_dir), dataset_id)

        result["profiles"] = profiles
        result["cross_analysis"] = cross_analysis

        # ── Layer 3: Canonical Schema ──────────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 3 — Canonical Schema")
        logger.info("=" * 60)

        schema = load_latest_schema(str(self.configs_dir))
        result["schema"] = schema
        result["schema_version"] = schema.version

        # ── Layer 4: Schema Mapping ────────────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 4 — Schema Mapping")
        logger.info("=" * 60)

        suggestions = suggest_mappings(sheets, schema)
        result["mapping_suggestions"] = suggestions

        if mapping_config is None:
            if auto_confirm_mappings:
                # First sheet wins for overlapping canonicals (e.g. contract-level sheet first in file)
                primary_sid = sheets[0]["sheet_id"] if sheets else None
                mapping_config = apply_suggestions_to_config(
                    suggestions, dataset_id, sheets, primary_sheet_id=primary_sid
                )
                logger.info("Auto-confirmed all mapping suggestions.")
            else:
                # Return early — user must confirm mappings
                result["status"] = "awaiting_mapping_confirmation"
                return result

        persist_mapping_config(mapping_config, str(self.artifacts_dir))
        result["mapping_config"] = mapping_config

        # ── Layer 5: Transformation Engine ─────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 5 — Transformation Engine")
        logger.info("=" * 60)

        # 5A: Extraction
        extracted = extract_mapped_columns(sheets, mapping_config, schema)
        rows_before = sum(len(df) for df in extracted.values())

        # 5B: Row Classification
        classifications = classify_rows(extracted)
        result["classification"] = classifications
        primary_classification = (
            next(iter(classifications.values()))
            if classifications
            else None
        )

        # 5C: Aggregation
        agg_config = self._load_config("aggregation_rules")
        aggregated_sheets, original_line_items_dict = aggregate(
            extracted, classifications, agg_config
        )
        aggregated = join_entities(aggregated_sheets, primary_sheet_id=None)
        rows_after = len(aggregated)
        line_items = None
        if original_line_items_dict:
            line_items = pd.concat(
                original_line_items_dict.values(), ignore_index=True
            )

        # 5D: Derived Fields
        derived_config = self._load_config("derived_fields")
        aggregated = compute_derived_fields(aggregated, derived_config)

        # 5E: Transformation Logging
        from src.layers.transformation import DatasetClassification
        transform_log = log_transformation(
            dataset_id=dataset_id,
            mapping_version=mapping_config.version,
            schema_version=schema.version,
            aggregation_config_version=agg_config.get("version", "v1"),
            derived_config_version=derived_config.get("version", "v1"),
            classification=primary_classification or DatasetClassification("mixed", {}),
            rows_before=rows_before,
            rows_after=rows_after,
            artifacts_dir=str(self.artifacts_dir),
        )

        result["extracted_rows"] = rows_before
        result["aggregated_rows"] = rows_after
        result["transform_log"] = transform_log

        # ── Layer 6: Validation & Integrity ────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 6 — Validation & Integrity")
        logger.info("=" * 60)

        val_config = self._load_config("validation_rules")
        validation_result = validate(aggregated, val_config, line_items)
        persist_validation_result(
            validation_result, str(self.artifacts_dir), dataset_id
        )

        result["validation"] = validation_result

        # ── Layer 7: Canonical Output ──────────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 7 — Canonical Output")
        logger.info("=" * 60)

        canonical_output = produce_canonical_output(
            contracts_df=aggregated,
            line_items_df=line_items,
            schema_version=schema.version,
            mapping_version=mapping_config.version,
            transform_version=agg_config.get("version", "v1"),
            dataset_id=dataset_id,
        )
        output_paths = persist_canonical_output(
            canonical_output, str(self.artifacts_dir)
        )

        result["canonical_output"] = canonical_output
        result["output_paths"] = output_paths

        # ── Layer 8: ML Contract Check ─────────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 8 — ML Interface Contract Check")
        logger.info("=" * 60)

        ml_valid, ml_violations = validate_ml_contract(canonical_output["contracts"])
        result["ml_ready"] = ml_valid
        result["ml_violations"] = ml_violations

        if not ml_valid:
            logger.warning("ML contract not satisfied — downstream ML blocked.")
        if not validation_result.passed:
            logger.warning("Validation failed — ML ingestion blocked.")
            result["ml_ready"] = False

        # ── Layer 9: Versioning ────────────────────────────────────
        logger.info("=" * 60)
        logger.info("LAYER 9 — Versioning")
        logger.info("=" * 60)

        manifest = create_manifest(
            dataset_id=dataset_id,
            schema_version=schema.version,
            mapping_version=mapping_config.version,
            aggregation_version=agg_config.get("version", "v1"),
            derived_fields_version=derived_config.get("version", "v1"),
            validation_version=val_config.get("version", "v1"),
            configs_dir=str(self.configs_dir),
        )
        persist_manifest(manifest, str(self.artifacts_dir))
        result["version_manifest"] = manifest

        result["status"] = "complete"

        # ── Summary ────────────────────────────────────────────────
        self._print_summary(result)

        return result

    def _load_config(self, component: str) -> dict:
        """Load the latest version of a config component."""
        files = sorted(self.configs_dir.glob(f"{component}_v*.json"))
        if not files:
            raise FileNotFoundError(
                f"No {component} config found in {self.configs_dir}"
            )
        with open(files[-1]) as f:
            return json.load(f)

    def _print_summary(self, result: dict):
        """Print an audit summary of the pipeline run."""
        print("\n" + "=" * 60)
        print("  PIPELINE SUMMARY")
        print("=" * 60)
        print(f"  Dataset ID:           {result['dataset_id']}")
        print(f"  Sheets Ingested:      {len(result['sheets'])}")
        print(f"  Schema Version:       {result['schema_version']}")
        cls = result["classification"]
        if isinstance(cls, dict):
            first = next(iter(cls.values()), None)
            cls_str = first.level if first else "—"
        else:
            cls_str = getattr(cls, "level", str(cls))
        print(f"  Classification:       {cls_str}")
        print(f"  Rows Before Agg:      {result['extracted_rows']}")
        print(f"  Rows After Agg:       {result['aggregated_rows']}")
        print(
            f"  Validation:           {'PASSED' if result['validation'].passed else 'FAILED'}"
        )
        print(f"    Errors:             {len(result['validation'].errors)}")
        print(f"    Warnings:           {len(result['validation'].warnings)}")
        print(f"  ML Ready:             {result['ml_ready']}")
        if result["ml_violations"]:
            for v in result["ml_violations"]:
                print(f"    ⚠ {v}")
        print(
            f"  Output:               {result['output_paths'].get('contracts_csv', 'N/A')}"
        )
        print("=" * 60 + "\n")
