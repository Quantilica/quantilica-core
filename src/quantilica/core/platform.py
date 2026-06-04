"""Platform bridge utilities for catalogs, runs, and manifests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .exceptions import MetadataError, StorageError
from .manifests import DownloadManifest
from .metadata import (
    Dataset,
    Dimension,
    IngestionRun,
    MetadataCatalog,
    Period,
    Resource,
    Series,
    Source,
    Territory,
    Variable,
)
from .storage import LocalStorage, ObjectStat

DEFAULT_CATALOG_KEY = "catalog/catalog.json"
DEFAULT_RUNS_PREFIX = "runs"
DEFAULT_MANIFESTS_PREFIX = "manifests/downloads"


@dataclass(frozen=True)
class CatalogIndex:
    """In-memory lookup index for a metadata catalog."""

    catalog: MetadataCatalog

    def __post_init__(self) -> None:
        self.catalog.validate_references()
        self._ensure_unique("source", [item.id for item in self.catalog.sources])
        self._ensure_unique("dataset", [item.id for item in self.catalog.datasets])
        self._ensure_unique("resource", [item.id for item in self.catalog.resources])
        self._ensure_unique("variable", [item.id for item in self.catalog.variables])
        self._ensure_unique("dimension", [item.id for item in self.catalog.dimensions])
        self._ensure_unique("series", [item.id for item in self.catalog.series])
        self._ensure_unique("territory", [item.id for item in self.catalog.territories])

    @staticmethod
    def _ensure_unique(kind: str, ids: list[str]) -> None:
        duplicates = sorted({item_id for item_id in ids if ids.count(item_id) > 1})
        if duplicates:
            raise MetadataError(f"Duplicate {kind} id(s): {', '.join(duplicates)}")

    @property
    def sources_by_id(self) -> dict[str, Source]:
        """Return sources indexed by id."""
        return {item.id: item for item in self.catalog.sources}

    @property
    def datasets_by_id(self) -> dict[str, Dataset]:
        """Return datasets indexed by id."""
        return {item.id: item for item in self.catalog.datasets}

    @property
    def resources_by_id(self) -> dict[str, Resource]:
        """Return resources indexed by id."""
        return {item.id: item for item in self.catalog.resources}

    def datasets_for_source(self, source_id: str) -> list[Dataset]:
        """Return datasets belonging to a source."""
        return [
            dataset
            for dataset in self.catalog.datasets
            if dataset.source_id == source_id
        ]

    def resources_for_dataset(self, dataset_id: str) -> list[Resource]:
        """Return resources belonging to a dataset."""
        return [
            resource
            for resource in self.catalog.resources
            if resource.dataset_id == dataset_id
        ]

    def series_for_dataset(self, dataset_id: str) -> list[Series]:
        """Return series belonging to a dataset."""
        return [item for item in self.catalog.series if item.dataset_id == dataset_id]

    def search_datasets(self, query: str) -> list[Dataset]:
        """Search datasets by id, name, and description."""
        normalized = query.casefold().strip()
        if not normalized:
            return []
        return [
            dataset
            for dataset in self.catalog.datasets
            if normalized in dataset.id.casefold()
            or normalized in dataset.name.casefold()
            or (
                dataset.description is not None
                and normalized in dataset.description.casefold()
            )
        ]


class CatalogRepository:
    """Read and write platform catalog artifacts in LocalStorage."""

    def __init__(
        self,
        storage: LocalStorage,
        *,
        catalog_key: str = DEFAULT_CATALOG_KEY,
        runs_prefix: str = DEFAULT_RUNS_PREFIX,
        manifests_prefix: str = DEFAULT_MANIFESTS_PREFIX,
    ) -> None:
        self.storage = storage
        self.catalog_key = catalog_key
        self.runs_prefix = runs_prefix.strip("/")
        self.manifests_prefix = manifests_prefix.strip("/")

    def publish_catalog(
        self,
        catalog: MetadataCatalog,
        *,
        validate: bool = True,
    ) -> ObjectStat:
        """Publish a metadata catalog JSON artifact."""
        if validate:
            CatalogIndex(catalog)
        return self.storage.write_text(self.catalog_key, catalog.to_json())

    def load_catalog(self) -> MetadataCatalog:
        """Load the published metadata catalog."""
        payload = self._read_json(self.catalog_key)
        return catalog_from_dict(payload)

    def load_index(self) -> CatalogIndex:
        """Load the published metadata catalog as an index."""
        return CatalogIndex(self.load_catalog())

    def write_run(self, run: IngestionRun) -> ObjectStat:
        """Write an ingestion run metadata artifact."""
        key = f"{self.runs_prefix}/{run.id}.json"
        return self.storage.write_text(key, run.to_json())

    def list_runs(self) -> list[IngestionRun]:
        """Load all ingestion run artifacts."""
        runs = [
            ingestion_run_from_dict(self._read_json(key))
            for key in self.storage.list(self.runs_prefix)
            if key.endswith(".json")
        ]
        return sorted(runs, key=lambda run: run.started_at)

    def write_download_manifest(
        self,
        manifest: DownloadManifest,
        *,
        key: str | None = None,
    ) -> ObjectStat:
        """Write a download manifest artifact."""
        target_key = key or self._download_manifest_key(manifest)
        return self.storage.write_text(target_key, manifest.to_json())

    def list_download_manifests(self) -> list[DownloadManifest]:
        """Load all download manifest artifacts."""
        manifests = [
            download_manifest_from_dict(self._read_json(key))
            for key in self.storage.list(self.manifests_prefix)
            if key.endswith(".json")
        ]
        return sorted(manifests, key=lambda manifest: manifest.fetched_at)

    def _download_manifest_key(self, manifest: DownloadManifest) -> str:
        resource_id = manifest.resource_id or "resource"
        return (
            f"{self.manifests_prefix}/"
            f"{manifest.source_id}/{manifest.dataset_id}/{resource_id}.json"
        )

    def _read_json(self, key: str) -> dict[str, Any]:
        try:
            return json.loads(self.storage.read_text(key))
        except json.JSONDecodeError as exc:
            raise StorageError(f"Could not parse JSON object: {key}") from exc


def catalog_from_dict(payload: dict[str, Any]) -> MetadataCatalog:
    """Build a MetadataCatalog from a dictionary."""
    return MetadataCatalog(
        sources=[Source(**item) for item in payload.get("sources", [])],
        datasets=[Dataset(**item) for item in payload.get("datasets", [])],
        resources=[Resource(**item) for item in payload.get("resources", [])],
        variables=[Variable(**item) for item in payload.get("variables", [])],
        dimensions=[Dimension(**item) for item in payload.get("dimensions", [])],
        series=[Series(**item) for item in payload.get("series", [])],
        periods=[Period(**item) for item in payload.get("periods", [])],
        territories=[Territory(**item) for item in payload.get("territories", [])],
        generated_at=payload.get("generated_at"),
        metadata=dict(payload.get("metadata", {})),
    )


def ingestion_run_from_dict(payload: dict[str, Any]) -> IngestionRun:
    """Build an IngestionRun from a dictionary."""
    return IngestionRun(**payload)


def download_manifest_from_dict(payload: dict[str, Any]) -> DownloadManifest:
    """Build a DownloadManifest from a dictionary."""
    return DownloadManifest(**payload)
