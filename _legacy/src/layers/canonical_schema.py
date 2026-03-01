"""
Layer 3 — Canonical Schema

Define, load, and version the canonical schema. Provides contract-level
and line-item-level schema views. Internal canonical field names are
stable and never auto-modified.
"""

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class CanonicalSchema:
    """Loaded canonical schema with querying capabilities."""

    def __init__(self, schema_data: dict):
        self._data = schema_data
        self.version = schema_data["version"]
        self.fields = schema_data["fields"]
        self._build_indexes()

    def _build_indexes(self):
        """Build lookup indexes for fast querying."""
        # Alias → canonical field name
        self._alias_lookup: dict[str, str] = {}
        self._all_aliases: list[str] = []

        for canonical, field_def in self.fields.items():
            for alias in field_def.get("aliases", []):
                self._alias_lookup[alias.lower()] = canonical
                self._all_aliases.append(alias.lower())

    @property
    def alias_lookup(self) -> dict[str, str]:
        return dict(self._alias_lookup)

    @property
    def all_aliases(self) -> list[str]:
        return list(self._all_aliases)

    def get_required_fields(self) -> list[str]:
        """Return list of canonical field names marked as required."""
        return [name for name, f in self.fields.items() if f.get("required")]

    def get_optional_fields(self) -> list[str]:
        """Return list of canonical field names that are optional."""
        return [name for name, f in self.fields.items() if not f.get("required")]

    def get_contract_level_fields(self) -> list[str]:
        """Return fields applicable at the contract level."""
        return [name for name, f in self.fields.items() if f.get("level") == "contract"]

    def get_line_item_fields(self) -> list[str]:
        """Return fields applicable at the line-item level."""
        return [
            name for name, f in self.fields.items() if f.get("level") == "line_item"
        ]

    def get_field_type(self, canonical_name: str) -> str | None:
        """Get the expected data type for a canonical field."""
        field = self.fields.get(canonical_name)
        return field["type"] if field else None

    def get_field_category(self, canonical_name: str) -> str | None:
        """Get the category for a canonical field (identity, financial, dates, etc.)."""
        field = self.fields.get(canonical_name)
        return field["category"] if field else None

    def get_financial_fields(self) -> list[str]:
        """Return all fields in the 'financial' category."""
        return [
            name for name, f in self.fields.items() if f.get("category") == "financial"
        ]

    def get_date_fields(self) -> list[str]:
        """Return all fields in the 'dates' category."""
        return [name for name, f in self.fields.items() if f.get("category") == "dates"]

    def get_boolean_fields(self) -> list[str]:
        """Return all fields with boolean type."""
        return [name for name, f in self.fields.items() if f.get("type") == "boolean"]

    def get_derived_fields(self) -> list[str]:
        """Return the list of derived field names."""
        return self._data.get("derived_contract_fields", [])

    def lookup_alias(self, alias: str) -> str | None:
        """Look up a canonical field name by alias."""
        return self._alias_lookup.get(alias.lower().strip())

    def to_dict(self) -> dict:
        """Serialize schema to dict for persistence."""
        return self._data

    def __repr__(self) -> str:
        return f"CanonicalSchema(version={self.version}, fields={len(self.fields)})"


# ---------------------------------------------------------------------------
# Schema loading and versioning
# ---------------------------------------------------------------------------


def load_schema(config_path: str) -> CanonicalSchema:
    """Load a canonical schema from a versioned JSON config file.

    Args:
        config_path: Path to the canonical_schema_v{N}.json file.

    Returns:
        CanonicalSchema instance.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema config not found: {config_path}")

    with open(path) as f:
        data = json.load(f)

    schema = CanonicalSchema(data)
    logger.info(
        "Loaded canonical schema %s: %d fields", schema.version, len(schema.fields)
    )
    return schema


def load_latest_schema(configs_dir: str) -> CanonicalSchema:
    """Load the latest version of the canonical schema from the configs directory.

    Scans for files matching canonical_schema_v*.json and loads the one
    with the highest version number.
    """
    configs = Path(configs_dir)
    schema_files = sorted(configs.glob("canonical_schema_v*.json"))

    if not schema_files:
        raise FileNotFoundError(f"No canonical schema files found in {configs_dir}")

    latest = schema_files[-1]
    return load_schema(str(latest))


def get_schema_version(config_path: str) -> str:
    """Read just the version string from a schema config file."""
    with open(config_path) as f:
        data = json.load(f)
    return data.get("version", "unknown")
