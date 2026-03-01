"""
Layer 9 — Versioning

Track and manage versions of all configuration components:
  - Canonical schema
  - Mapping configurations
  - Aggregation rules
  - Derived field rules
  - Validation rules

Supports historical re-execution: same dataset + same versions = identical output.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class VersionManifest:
    """Tracks all configuration versions used in a pipeline run."""

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.versions: dict[str, str] = {}
        self.config_paths: dict[str, str] = {}
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def set_version(self, component: str, version: str, config_path: str | None = None):
        """Record the version of a configuration component."""
        self.versions[component] = version
        if config_path:
            self.config_paths[component] = config_path

    def get_version(self, component: str) -> str | None:
        return self.versions.get(component)

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "timestamp": self.timestamp,
            "versions": self.versions,
            "config_paths": self.config_paths,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "VersionManifest":
        manifest = cls(data["dataset_id"])
        manifest.versions = data.get("versions", {})
        manifest.config_paths = data.get("config_paths", {})
        manifest.timestamp = data.get("timestamp", "")
        return manifest


def create_manifest(
    dataset_id: str,
    schema_version: str,
    mapping_version: str,
    aggregation_version: str,
    derived_fields_version: str,
    validation_version: str,
    configs_dir: str | None = None,
) -> VersionManifest:
    """Create a version manifest for a pipeline run.

    Args:
        dataset_id: Dataset identifier.
        schema_version: Version of canonical schema.
        mapping_version: Version of mapping config.
        aggregation_version: Version of aggregation rules.
        derived_fields_version: Version of derived field rules.
        validation_version: Version of validation rules.
        configs_dir: Optional path to configs directory.

    Returns:
        VersionManifest with all versions recorded.
    """
    manifest = VersionManifest(dataset_id)

    manifest.set_version(
        "canonical_schema",
        schema_version,
        (
            f"{configs_dir}/canonical_schema_{schema_version}.json"
            if configs_dir
            else None
        ),
    )
    manifest.set_version("mapping", mapping_version)
    manifest.set_version(
        "aggregation_rules",
        aggregation_version,
        (
            f"{configs_dir}/aggregation_rules_{aggregation_version}.json"
            if configs_dir
            else None
        ),
    )
    manifest.set_version(
        "derived_fields",
        derived_fields_version,
        (
            f"{configs_dir}/derived_fields_{derived_fields_version}.json"
            if configs_dir
            else None
        ),
    )
    manifest.set_version(
        "validation_rules",
        validation_version,
        (
            f"{configs_dir}/validation_rules_{validation_version}.json"
            if configs_dir
            else None
        ),
    )

    logger.info(
        "Version manifest created for dataset %s: %s", dataset_id, manifest.versions
    )
    return manifest


def persist_manifest(manifest: VersionManifest, artifacts_dir: str) -> str:
    """Persist a version manifest to the artifacts directory.

    Writes to: {artifacts_dir}/{dataset_id}/version_manifest.json

    Returns the path to the saved file.
    """
    out_dir = Path(artifacts_dir) / manifest.dataset_id
    out_dir.mkdir(parents=True, exist_ok=True)

    path = out_dir / "version_manifest.json"
    with open(path, "w") as f:
        json.dump(manifest.to_dict(), f, indent=2, default=str)

    logger.info("Persisted version manifest → %s", path)
    return str(path)


def load_manifest(path: str) -> VersionManifest:
    """Load a version manifest from a JSON file."""
    with open(path) as f:
        data = json.load(f)
    return VersionManifest.from_dict(data)


def load_config_by_version(configs_dir: str, component: str, version: str) -> dict:
    """Load a specific config version.

    Args:
        configs_dir: Path to the configs directory.
        component: Config component name (e.g., 'canonical_schema').
        version: Version string (e.g., 'v1').

    Returns:
        Config dict loaded from JSON.
    """
    path = Path(configs_dir) / f"{component}_{version}.json"
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    with open(path) as f:
        return json.load(f)


def list_available_versions(configs_dir: str) -> dict[str, list[str]]:
    """List all available config versions in the configs directory.

    Returns:
        Dict mapping component name → list of version strings.
    """
    configs = Path(configs_dir)
    if not configs.is_dir():
        return {}

    components: dict[str, list[str]] = {}

    for f in sorted(configs.glob("*.json")):
        name = f.stem
        # Parse component_vN format
        parts = name.rsplit("_", 1)
        if len(parts) == 2 and parts[1].startswith("v"):
            component = parts[0]
            version = parts[1]
            components.setdefault(component, []).append(version)

    return components


def verify_reproducibility(
    manifest1: VersionManifest,
    manifest2: VersionManifest,
) -> bool:
    """Check if two manifests use identical versions (implies identical output).

    Returns True if all config versions match.
    """
    return manifest1.versions == manifest2.versions
