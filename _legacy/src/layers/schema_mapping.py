"""
Layer 4 — Schema Mapping

Map raw column names to canonical field names. Supports:
  1. Exact alias lookup
  2. Fuzzy matching via RapidFuzz (suggestion only)
  3. Content-based heuristic detection (suggestion only)

All mappings require explicit confirmation — no auto-apply.
Mapping configs are versioned and persisted.
"""

import json
import logging
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

from src.layers.canonical_schema import CanonicalSchema
from src.utils import detect_value_type, sample_values

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Semantic validation for fuzzy matches
# ---------------------------------------------------------------------------

_FINANCIAL_WORDS = {
    "arr",
    "amount",
    "cost",
    "price",
    "revenue",
    "total",
    "fee",
    "spend",
    "value",
    "discount",
    "margin",
    "ppu",
    "rate",
}
_ID_WORDS = {"id", "number", "no", "#", "name", "code", "ref", "reference"}


def _validate_fuzzy_match(
    col_lower: str, alias: str, canonical: str, score: int
) -> bool:
    """Validate that a fuzzy match is semantically reasonable."""
    col_words = set(col_lower.replace("_", " ").split())

    # Block financial words mapping to ID columns
    if canonical == "contract_id":
        if col_words & _FINANCIAL_WORDS:
            return False

    # Block ID words mapping to financial columns
    if canonical in {"acv", "total_value"}:
        if col_words & _ID_WORDS and not col_words & _FINANCIAL_WORDS:
            return False

    # For low-confidence matches, require meaningful word overlap
    if score < 90:
        alias_words = set(alias.replace("_", " ").split())
        meaningful_overlap = {w for w in (col_words & alias_words) if len(w) > 2}
        if not meaningful_overlap:
            return False

    return True


def _detect_column_type_from_values(series: pd.Series, col_name: str) -> str | None:
    """Inspect cell values to guess what canonical type a column might be."""
    samples = sample_values(series)
    value_type = detect_value_type(samples)
    col_lower = col_name.strip().lower()

    if value_type == "date":
        if any(
            kw in col_lower
            for kw in ["close", "win", "won", "book", "sign", "exec", "creat", "order"]
        ):
            return "close_date"
        elif any(
            kw in col_lower
            for kw in ["start", "begin", "effect", "from", "commence", "activ"]
        ):
            return "start_date"
        elif any(
            kw in col_lower
            for kw in ["end", "expir", "termin", "thru", "through", "to "]
        ):
            return "end_date"
        elif any(kw in col_lower for kw in ["renew"]):
            return "renewal_date"
        return None

    if value_type == "currency":
        if any(kw in col_lower for kw in ["total", "tcv", "grand", "lifetime", "ltv"]):
            return "total_value"
        return "acv"

    if value_type == "id":
        if any(
            kw in col_lower
            for kw in [
                "id",
                "#",
                "number",
                "num",
                "no",
                "ref",
                "code",
                "opp",
                "deal",
                "quote",
                "order",
                "contract",
                "agreement",
                "po",
                "subscription",
            ]
        ):
            return "contract_id"

    return None


# ---------------------------------------------------------------------------
# Mapping suggestion
# ---------------------------------------------------------------------------


def suggest_mappings(
    sheets: list[dict],
    schema: CanonicalSchema,
    threshold: int = 80,
) -> dict:
    """Generate mapping suggestions for all sheets.

    Returns a mapping suggestion dict:
    {
        sheet_id: {
            "suggested": {raw_col: canonical_field, ...},
            "unmapped": [raw_col, ...],
            "confidence": {raw_col: {"canonical": field, "score": N, "method": str}, ...}
        }
    }

    These are SUGGESTIONS ONLY — they must be explicitly confirmed.
    """
    all_suggestions = {}

    for sheet in sheets:
        sheet_id = sheet["sheet_id"]
        df = sheet["dataframe"]
        suggestions, unmapped, confidence = _suggest_for_sheet(df, schema, threshold)

        all_suggestions[sheet_id] = {
            "suggested": suggestions,
            "unmapped": unmapped,
            "confidence": confidence,
        }

        logger.info(
            "Sheet '%s': %d suggested mappings, %d unmapped columns",
            sheet["sheet_name"],
            len(suggestions),
            len(unmapped),
        )

    return all_suggestions


def _suggest_for_sheet(
    df: pd.DataFrame,
    schema: CanonicalSchema,
    threshold: int,
) -> tuple[dict[str, str], list[str], dict[str, dict]]:
    """Generate suggestions for a single sheet."""
    suggestions: dict[str, str] = {}
    unmapped: list[str] = []
    confidence: dict[str, dict] = {}
    mapped_canonicals: set[str] = set()

    alias_lookup = schema.alias_lookup
    all_aliases = schema.all_aliases
    still_unmapped: list[str] = []

    for col in df.columns:
        col_lower = col.strip().lower()

        # Pass 1: Exact alias lookup
        if col_lower in alias_lookup:
            canonical = alias_lookup[col_lower]
            suggestions[col] = canonical
            mapped_canonicals.add(canonical)
            confidence[col] = {
                "canonical": canonical,
                "score": 100,
                "method": "exact_alias",
            }
            continue

        # Pass 2: Fuzzy match
        match = process.extractOne(
            col_lower,
            all_aliases,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=threshold,
        )
        if match:
            matched_alias, score, _ = match
            canonical = alias_lookup[matched_alias]
            if _validate_fuzzy_match(col_lower, matched_alias, canonical, score):
                suggestions[col] = canonical
                mapped_canonicals.add(canonical)
                confidence[col] = {
                    "canonical": canonical,
                    "score": int(score),
                    "method": "fuzzy",
                    "matched_alias": matched_alias,
                }
                continue
            else:
                logger.debug("Rejected fuzzy: '%s' → '%s' (semantics)", col, canonical)

        still_unmapped.append(col)

    # Pass 3: Content-based detection
    for col in still_unmapped:
        detected = _detect_column_type_from_values(df[col], col)
        if detected and detected not in mapped_canonicals:
            suggestions[col] = detected
            mapped_canonicals.add(detected)
            confidence[col] = {
                "canonical": detected,
                "score": 70,
                "method": "content_detection",
            }
        else:
            unmapped.append(col)

    return suggestions, unmapped, confidence


# ---------------------------------------------------------------------------
# Mapping configuration management
# ---------------------------------------------------------------------------


class MappingConfig:
    """A confirmed mapping configuration linking raw columns to canonical fields.

    Supports multiple sheets mapping to the same canonical (e.g. contract_id from
    both CRM and Line Items sheets) so extraction can pull from each sheet.
    """

    def __init__(self, dataset_id: str, version: str = "v1"):
        self.dataset_id = dataset_id
        self.version = version
        # (canonical, sheet_id, column_name) so same canonical can come from multiple sheets
        self._entries: list[tuple[str, str, str]] = []
        self._required_explicit: set[str] = {
            "contract_id",
            "total_value",
            "start_date",
            "end_date",
        }

    @property
    def mappings(self) -> dict[str, tuple[str, str]]:
        """Primary mapping: canonical → first (sheet_id, column_name)."""
        out: dict[str, tuple[str, str]] = {}
        for canonical, sid, col in self._entries:
            if canonical not in out:
                out[canonical] = (sid, col)
        return out

    def set_mapping(
        self, canonical_field: str, sheet_id: str, column_name: str
    ) -> None:
        """Set a mapping; same canonical can be set from multiple sheets."""
        self._entries.append((canonical_field, sheet_id, column_name))

    def get_sheet_mappings(self) -> dict[str, dict[str, str]]:
        """Return per-sheet canonical → raw column for extraction."""
        out: dict[str, dict[str, str]] = {}
        for canonical, sheet_id, col in self._entries:
            out.setdefault(sheet_id, {})[canonical] = col
        return out

    def remove_mapping(self, canonical_field: str) -> None:
        """Remove all mappings for a canonical field."""
        self._entries = [(c, s, col) for c, s, col in self._entries if c != canonical_field]

    def get_mapping(self, canonical_field: str) -> tuple[str, str] | None:
        """Get first (sheet_id, column_name) for a canonical field."""
        return self.mappings.get(canonical_field)

    def get_unmapped_required(self) -> list[str]:
        """Return required fields that are not yet mapped."""
        mapped = {c for c, _, _ in self._entries}
        return [f for f in self._required_explicit if f not in mapped]

    def is_complete(self) -> bool:
        """Check if all required fields have mappings."""
        return len(self.get_unmapped_required()) == 0

    def validate(self) -> list[str]:
        """Validate the mapping config. Returns list of error messages."""
        errors = []
        unmapped = self.get_unmapped_required()
        if unmapped:
            errors.append(f"Required fields not mapped: {unmapped}")
        return errors

    def to_dict(self) -> dict:
        """Serialize to dict for persistence (all entries for multi-sheet)."""
        return {
            "dataset_id": self.dataset_id,
            "version": self.version,
            "mappings": {
                canonical: {"sheet_id": sid, "column_name": col}
                for canonical, (sid, col) in self.mappings.items()
            },
            "entries": [
                {"canonical": c, "sheet_id": s, "column_name": col}
                for c, s, col in self._entries
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MappingConfig":
        """Deserialize from dict; restores full entries if present."""
        config = cls(dataset_id=data["dataset_id"], version=data.get("version", "v1"))
        entries = data.get("entries")
        if entries:
            for e in entries:
                config.set_mapping(e["canonical"], e["sheet_id"], e["column_name"])
        else:
            for canonical, mapping in data.get("mappings", {}).items():
                config.set_mapping(canonical, mapping["sheet_id"], mapping["column_name"])
        return config

    def __repr__(self) -> str:
        return f"MappingConfig(dataset={self.dataset_id}, v={self.version}, {len(self._entries)} entries)"


def apply_suggestions_to_config(
    suggestions: dict,
    dataset_id: str,
    sheets: list[dict],
    primary_sheet_id: str | None = None,
) -> MappingConfig:
    """Create a MappingConfig from accepted suggestions.

    Adds every (sheet, raw_col, canonical) so multiple sheets can contribute
    the same canonical (e.g. contract_id from both CRM and Line Items).
    """
    config = MappingConfig(dataset_id=dataset_id)
    for sheet_id, sheet_suggestions in suggestions.items():
        for raw_col, canonical in sheet_suggestions["suggested"].items():
            config.set_mapping(canonical, sheet_id, raw_col)
    return config


def persist_mapping_config(config: MappingConfig, artifacts_dir: str) -> str:
    """Persist a mapping configuration to disk.

    Writes to: {artifacts_dir}/{dataset_id}/mappings/mapping_{version}.json

    Returns the path to the saved file.
    """
    mapping_dir = Path(artifacts_dir) / config.dataset_id / "mappings"
    mapping_dir.mkdir(parents=True, exist_ok=True)

    path = mapping_dir / f"mapping_{config.version}.json"
    with open(path, "w") as f:
        json.dump(config.to_dict(), f, indent=2)

    logger.info("Persisted mapping config %s → %s", config.version, path)
    return str(path)


def load_mapping_config(path: str) -> MappingConfig:
    """Load a mapping config from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    return MappingConfig.from_dict(data)
