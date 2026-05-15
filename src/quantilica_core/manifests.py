"""Provenance manifests for downloaded and generated data artifacts."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .dates import isoformat_utc
from .files import sha256_bytes, sha256_file, write_text_atomic

# Current DownloadManifest schema version. Readers treat a missing
# ``manifest_version`` field as version 1.
MANIFEST_VERSION = 2


@dataclass(frozen=True)
class ContentFingerprint:
    """Structural fingerprint of a downloaded artifact's content.

    Goes beyond the byte-level ``sha256`` to describe the shape of the
    data, so a structural change can be detected without a full diff.
    """

    content_type: str | None = None
    schema_hash: str | None = None
    row_count: int | None = None
    column_count: int | None = None
    # Smallest and largest period covered, e.g. ["2020-01", "2025-04"].
    temporal_extent: list[str] | None = None
    geographic_extent: list[str] | None = None


@dataclass(frozen=True)
class SourceMetadata:
    """Metadata reported by the upstream source about the resource."""

    etag: str | None = None
    last_modified: str | None = None
    published_at: str | None = None
    # Expected update cadence: daily, weekly, monthly, quarterly, yearly...
    expected_cadence: str | None = None


@dataclass(frozen=True)
class Lineage:
    """Provenance links from this artifact to the inputs it derives from."""

    # SHA-256 digests of the input artifacts (e.g. transform inputs).
    derived_from: list[str] = field(default_factory=list)
    pipeline_id: str | None = None


@dataclass(frozen=True)
class QualitySignals:
    """Data quality signals computed during ingestion."""

    validation_status: str | None = None
    null_ratio: float | None = None
    # Difference from the previous version: rows added/removed, cells changed.
    diff_from_previous: dict[str, Any] | None = None


@dataclass(frozen=True)
class ExecutionInfo:
    """Execution and environment details supporting reproducibility."""

    duration_ms: int | None = None
    retry_count: int | None = None
    environment: dict[str, Any] | None = None
    data_license: str | None = None


@dataclass(frozen=True)
class DownloadManifest:
    """Provenance for a downloaded resource.

    The optional ``fingerprint``, ``source_meta``, ``lineage``, ``quality``
    and ``execution`` groups are usually populated after the base manifest is
    built (e.g. once the data is parsed or validated). Since the manifest is
    frozen, enrich it with :func:`dataclasses.replace`::

        manifest = replace(manifest, quality=QualitySignals(...))
    """

    source_id: str
    dataset_id: str
    url: str
    fetched_at: str
    sha256: str
    size_bytes: int
    resource_id: str | None = None
    path: str | None = None
    producer: str | None = None
    producer_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    manifest_version: int = MANIFEST_VERSION
    fingerprint: ContentFingerprint | None = None
    source_meta: SourceMetadata | None = None
    lineage: Lineage | None = None
    quality: QualitySignals | None = None
    execution: ExecutionInfo | None = None

    @classmethod
    def from_content(
        cls,
        *,
        source_id: str,
        dataset_id: str,
        url: str,
        content: bytes,
        resource_id: str | None = None,
        path: str | None = None,
        producer: str | None = None,
        producer_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DownloadManifest:
        """Build a download manifest from bytes."""
        return cls(
            source_id=source_id,
            dataset_id=dataset_id,
            resource_id=resource_id,
            url=url,
            path=path,
            fetched_at=isoformat_utc(),
            sha256=sha256_bytes(content),
            size_bytes=len(content),
            producer=producer,
            producer_version=producer_version,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def from_digest(
        cls,
        *,
        source_id: str,
        dataset_id: str,
        url: str,
        sha256: str,
        size_bytes: int,
        resource_id: str | None = None,
        path: str | None = None,
        producer: str | None = None,
        producer_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DownloadManifest:
        """Build a download manifest from a precomputed digest and size.

        Useful when the digest was computed incrementally during a streaming
        download, so the file does not need to be re-read.
        """
        return cls(
            source_id=source_id,
            dataset_id=dataset_id,
            resource_id=resource_id,
            url=url,
            path=path,
            fetched_at=isoformat_utc(),
            sha256=sha256,
            size_bytes=size_bytes,
            producer=producer,
            producer_version=producer_version,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def from_file(
        cls,
        *,
        source_id: str,
        dataset_id: str,
        url: str,
        file_path: str | Path,
        resource_id: str | None = None,
        path: str | None = None,
        producer: str | None = None,
        producer_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DownloadManifest:
        """Build a download manifest from a file."""
        target = Path(file_path)
        return cls(
            source_id=source_id,
            dataset_id=dataset_id,
            resource_id=resource_id,
            url=url,
            path=path or str(target),
            fetched_at=isoformat_utc(),
            sha256=sha256_file(target),
            size_bytes=target.stat().st_size,
            producer=producer,
            producer_version=producer_version,
            metadata=dict(metadata or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return this manifest as a dictionary."""
        return asdict(self)

    def to_json(self, *, indent: int = 2) -> str:
        """Serialize this manifest as JSON."""
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            indent=indent,
            sort_keys=True,
        )

    def write_json(self, path: str | Path) -> Path:
        """Write this manifest to a JSON file."""
        return write_text_atomic(path, self.to_json())


@dataclass(frozen=True)
class DatasetManifest:
    """Generic manifest for a dataset snapshot."""

    source_id: str
    dataset_id: str
    generated_at: str
    resources: list[DownloadManifest] = field(default_factory=list)
    title: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        source_id: str,
        dataset_id: str,
        resources: list[DownloadManifest] | None = None,
        title: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> DatasetManifest:
        """Create a dataset manifest using the current UTC timestamp."""
        return cls(
            source_id=source_id,
            dataset_id=dataset_id,
            generated_at=isoformat_utc(),
            resources=list(resources or []),
            title=title,
            description=description,
            metadata=dict(metadata or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return this manifest as a dictionary."""
        payload = asdict(self)
        payload["resources"] = [resource.to_dict() for resource in self.resources]
        return payload

    def to_json(self, *, indent: int = 2) -> str:
        """Serialize this manifest as JSON."""
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            indent=indent,
            sort_keys=True,
        )

    def write_json(self, path: str | Path) -> Path:
        """Write this manifest to a JSON file."""
        return write_text_atomic(path, self.to_json())


@dataclass(frozen=True)
class RunManifest:
    """Manifest describing one ingestion or processing run."""

    run_id: str
    started_at: str
    finished_at: str | None = None
    status: str = "running"
    source_id: str | None = None
    dataset_id: str | None = None
    producer: str | None = None
    producer_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def start(
        cls,
        *,
        run_id: str,
        source_id: str | None = None,
        dataset_id: str | None = None,
        producer: str | None = None,
        producer_version: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunManifest:
        """Create a running manifest."""
        return cls(
            run_id=run_id,
            started_at=isoformat_utc(),
            source_id=source_id,
            dataset_id=dataset_id,
            producer=producer,
            producer_version=producer_version,
            metadata=dict(metadata or {}),
        )

    def finish(self, *, status: str = "success") -> RunManifest:
        """Return a finished copy of this run manifest."""
        return RunManifest(
            run_id=self.run_id,
            started_at=self.started_at,
            finished_at=isoformat_utc(),
            status=status,
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            producer=self.producer,
            producer_version=self.producer_version,
            metadata=dict(self.metadata),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return this manifest as a dictionary."""
        return asdict(self)

    def to_json(self, *, indent: int = 2) -> str:
        """Serialize this manifest as JSON."""
        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            indent=indent,
            sort_keys=True,
        )

    def write_json(self, path: str | Path) -> Path:
        """Write this manifest to a JSON file."""
        return write_text_atomic(path, self.to_json())
