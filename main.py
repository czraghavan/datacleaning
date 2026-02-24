#!/usr/bin/env python3
"""
main.py — CLI entry point for SaaS Contract Data Organizer.

Usage:
    python main.py --file contracts.xlsx --output master.csv
    python main.py --dir ./contract_csvs/ --output master.csv
"""

import argparse
import logging
import sys

import pandas as pd

from src.ingest import load_excel, load_csv_dir
from src.schema import fuzzy_map_headers
from src.cleaner import Cleaner
from src.vendor import resolve_vendors
from src.merge import concat_all, merge_quote_lines, deduplicate

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DataOrgModel")


def run_pipeline(
    file_path: str | None = None,
    dir_path: str | None = None,
    output: str = "master_contracts.csv",
    vendor_threshold: int = 85,
    header_threshold: int = 80,
) -> None:
    """Execute the full ingest → clean → merge → export pipeline."""

    # ── 1. Ingest ─────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 1 — Ingesting data")
    logger.info("=" * 60)

    if file_path:
        frames = load_excel(file_path)
    elif dir_path:
        frames = load_csv_dir(dir_path)
    else:
        logger.error("No input specified. Use --file or --dir.")
        sys.exit(1)

    if not frames:
        logger.error("No data loaded. Check your input path.")
        sys.exit(1)

    total_rows_ingested = sum(len(f) for f in frames)

    # ── 2. Schema mapping ─────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 2 — Mapping headers to canonical schema")
    logger.info("=" * 60)

    all_unmapped: list[str] = []
    mapped_frames: list[pd.DataFrame] = []
    for i, df in enumerate(frames):
        df_mapped, unmapped = fuzzy_map_headers(df, threshold=header_threshold)
        all_unmapped.extend(unmapped)
        mapped_frames.append(df_mapped)

    all_unmapped = sorted(set(all_unmapped))

    # ── 3. Concatenate ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 3 — Concatenating all frames")
    logger.info("=" * 60)

    master = concat_all(mapped_frames)

    # ── 4. Clean ──────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 4 — Cleaning data (dates, financials, booleans)")
    logger.info("=" * 60)

    cleaner = Cleaner()
    master = cleaner.clean(master)

    # ── 5. Vendor resolution ──────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 5 — Resolving vendor entities")
    logger.info("=" * 60)

    master = resolve_vendors(master, threshold=vendor_threshold)
    unique_vendors = master["Vendor_Canonical"].nunique() if "Vendor_Canonical" in master.columns else 0

    # ── 6. Merge quote lines ──────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 6 — Merging quote lines into contracts")
    logger.info("=" * 60)

    master = merge_quote_lines(master)

    # ── 7. Deduplicate ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 7 — Deduplicating")
    logger.info("=" * 60)

    master, dupes_removed = deduplicate(master)

    # ── 8. Export ──────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("STEP 8 — Exporting to %s", output)
    logger.info("=" * 60)

    master.to_csv(output, index=False, encoding="utf-8")

    # ── 9. Audit Report ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  AUDIT REPORT")
    print("=" * 60)
    print(f"  Total Rows Ingested:    {total_rows_ingested}")
    print(f"  Final Rows Exported:    {len(master)}")
    print(f"  Unique Vendors Found:   {unique_vendors}")
    print(f"  Duplicates Removed:     {dupes_removed}")
    if all_unmapped:
        print(f"  Unmapped Columns:       {', '.join(all_unmapped)}")
    else:
        print("  Unmapped Columns:       (none)")
    print(f"  Output File:            {output}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="SaaS Contract Data Organizer — merge & clean B2B contract data.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--file", "-f",
        help="Path to a single .xlsx file (multi-tab).",
    )
    group.add_argument(
        "--dir", "-d",
        help="Path to a directory of .csv / .xlsx files.",
    )
    parser.add_argument(
        "--output", "-o",
        default="master_contracts.csv",
        help="Output CSV path (default: master_contracts.csv).",
    )
    parser.add_argument(
        "--vendor-threshold",
        type=int,
        default=85,
        help="Fuzzy match threshold for vendor names (0-100, default: 85).",
    )
    parser.add_argument(
        "--header-threshold",
        type=int,
        default=80,
        help="Fuzzy match threshold for header mapping (0-100, default: 80).",
    )

    args = parser.parse_args()

    run_pipeline(
        file_path=args.file,
        dir_path=args.dir,
        output=args.output,
        vendor_threshold=args.vendor_threshold,
        header_threshold=args.header_threshold,
    )


if __name__ == "__main__":
    main()
