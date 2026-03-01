#!/usr/bin/env python3
"""
main.py — CLI entry point for the 10-layer data transformation pipeline.

Usage:
    python main.py --file contracts.xlsx --output artifacts/
    python main.py --dir ./contract_csvs/ --output artifacts/
"""

import argparse
import logging
import sys

from pipeline import Pipeline

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DataOrgModel")


def main():
    parser = argparse.ArgumentParser(
        description="SaaS Contract Data Pipeline — 10-layer deterministic transformation.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--file",
        "-f",
        help="Path to a single .xlsx or .csv file.",
    )
    group.add_argument(
        "--dir",
        "-d",
        help="Path to a directory of .csv / .xlsx files.",
    )
    parser.add_argument(
        "--configs",
        "-c",
        default="configs",
        help="Path to configuration directory (default: configs/).",
    )
    parser.add_argument(
        "--artifacts",
        "-a",
        default="artifacts",
        help="Path to artifacts output directory (default: artifacts/).",
    )
    parser.add_argument(
        "--company-id",
        default=None,
        help="Company identifier for this dataset.",
    )
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        help="Auto-confirm suggested schema mappings (non-interactive).",
    )

    args = parser.parse_args()

    try:
        pipe = Pipeline(
            configs_dir=args.configs,
            artifacts_dir=args.artifacts,
        )

        result = pipe.run(
            file_path=args.file,
            dir_path=args.dir,
            company_id=args.company_id,
            auto_confirm_mappings=args.auto_confirm,
        )

        if result.get("status") == "awaiting_mapping_confirmation":
            print("\n⚠️  Mappings require confirmation.")
            print("   Suggested mappings:")
            for sheet_id, data in result["mapping_suggestions"].items():
                print(f"\n   Sheet: {sheet_id}")
                for raw_col, canonical in data["suggested"].items():
                    print(f"     {raw_col} → {canonical}")
                if data["unmapped"]:
                    print(f"     Unmapped: {', '.join(data['unmapped'])}")
            print("\n   Use --auto-confirm to accept all suggestions,")
            print("   or use the web UI (python server.py) for interactive mapping.")
            sys.exit(0)

        if result.get("status") == "complete":
            csv_path = result.get("output_paths", {}).get("contracts_csv")
            if csv_path:
                print(f"\n✅ Pipeline complete. Canonical output: {csv_path}")
            sys.exit(0)

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
