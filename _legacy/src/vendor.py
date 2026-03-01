"""
vendor.py — Vendor entity resolution using RapidFuzz.

Clusters similar vendor names and assigns a canonical spelling.
"""

import logging
from collections import Counter

import pandas as pd
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

# Well-known SaaS/tech vendor abbreviations → canonical full names.
# Used to expand short forms before fuzzy matching so that e.g.
# "AWS" and "Amazon Web Services" cluster together.
COMMON_ABBREVIATIONS: dict[str, str] = {
    "aws": "amazon web services",
    "gcp": "google cloud platform",
    "msft": "microsoft",
    "ms": "microsoft",
    "sf": "salesforce",
    "sfdc": "salesforce",
    "gh": "github",
    "o365": "office 365",
    "k8s": "kubernetes",
}


def resolve_vendors(
    df: pd.DataFrame,
    vendor_col: str = "Vendor",
    threshold: int = 85,
) -> pd.DataFrame:
    """Cluster similar vendor names and add canonical columns.

    Adds:
        Vendor_Canonical — the most common spelling in each cluster
        Vendor_Cluster_ID — integer ID for the cluster (for auditing)

    Args:
        df: DataFrame containing a vendor column.
        vendor_col: Name of the vendor column to resolve.
        threshold: Minimum token_sort_ratio score (0-100) to merge names.

    Returns:
        DataFrame with two new columns.
    """
    if vendor_col not in df.columns:
        logger.warning(
            "Vendor column '%s' not found — skipping vendor resolution.", vendor_col
        )
        df["Vendor_Canonical"] = pd.NA
        df["Vendor_Cluster_ID"] = pd.NA
        return df

    # Collect unique non-null vendor names
    unique_names = df[vendor_col].dropna().unique().tolist()
    unique_names = [str(n).strip() for n in unique_names if str(n).strip()]

    if not unique_names:
        df["Vendor_Canonical"] = pd.NA
        df["Vendor_Cluster_ID"] = pd.NA
        return df

    # Build clusters via greedy single-linkage
    clusters: list[list[str]] = []
    assigned: set[str] = set()

    def _expand(name: str) -> str:
        """Expand known abbreviations for better fuzzy matching."""
        low = name.lower().strip()
        return COMMON_ABBREVIATIONS.get(low, low)

    for name in unique_names:
        if name in assigned:
            continue

        cluster = [name]
        assigned.add(name)
        name_exp = _expand(name)

        for other in unique_names:
            if other in assigned:
                continue
            other_exp = _expand(other)
            # Compare expanded forms using the best of two scorers
            score = max(
                fuzz.token_sort_ratio(name_exp, other_exp),
                fuzz.partial_ratio(name_exp, other_exp),
            )
            if score >= threshold:
                cluster.append(other)
                assigned.add(other)

        clusters.append(cluster)

    # Build mapping: original_name -> (canonical, cluster_id)
    name_to_canonical: dict[str, str] = {}
    name_to_cluster: dict[str, int] = {}

    for cluster_id, cluster in enumerate(clusters):
        # Canonical = the most commonly occurring spelling in the data
        counts = Counter()
        for member in cluster:
            counts[member] = int(
                df[vendor_col].astype(str).str.strip().eq(member).sum()
            )
        canonical = counts.most_common(1)[0][0]

        if len(cluster) > 1:
            logger.info(
                "Vendor cluster %d: %s → canonical '%s'",
                cluster_id,
                cluster,
                canonical,
            )

        for member in cluster:
            name_to_canonical[member] = canonical
            name_to_cluster[member] = cluster_id

    # Apply mapping
    stripped = df[vendor_col].astype(str).str.strip()
    df["Vendor_Canonical"] = stripped.map(name_to_canonical)
    df["Vendor_Cluster_ID"] = stripped.map(name_to_cluster)

    logger.info(
        "Vendor resolution complete: %d unique names → %d clusters.",
        len(unique_names),
        len(clusters),
    )

    return df
