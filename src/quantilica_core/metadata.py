"""Generic metadata models for public data platforms."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .dates import isoformat_utc
from .exceptions import MetadataError
from .files import write_text_atomic

ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:-]*$")
RUN_STATUSES = frozenset({"running", "success", "failed", "cancelled"})


def validate_id(value: str, *, field_name: str = "id") -> str:
    """Validate a stable platform identifier."""
    if not value or not ID_PATTERN.fullmatch(value):
        raise MetadataError(f"Invalid {field_name}: {value!r}")
    return value


def _json_dumps(payload: dict[str, Any], *, indent: int = 2) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=indent, sort_keys=True)


@dataclass(frozen=True)
class MetadataModel:
    """Base class for serializable metadata dataclasses."""

    def to_dict(self) -> dict[str, Any]:
        """Return this metadata object as a dictionary."""
        return asdict(self)

    def to_json(self, *, indent: int = 2) -> str:
        """Serialize this metadata object as JSON."""
        return _json_dumps(self.to_dict(), indent=indent)

    def write_json(self, path: str | Path) -> Path:
        """Write this metadata object to a JSON file."""
        return write_text_atomic(path, self.to_json())


@dataclass(frozen=True)
class Source(MetadataModel):
    """A data source or publishing institution."""

    id: str
    name: str
    homepage_url: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="source id")


@dataclass(frozen=True)
class Dataset(MetadataModel):
    """A logical dataset published by a source."""

    id: str
    source_id: str
    name: str
    description: str | None = None
    license: str | None = None
    homepage_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="dataset id")
        validate_id(self.source_id, field_name="source id")


@dataclass(frozen=True)
class Resource(MetadataModel):
    """A concrete resource belonging to a dataset."""

    id: str
    dataset_id: str
    name: str
    url: str | None = None
    format: str | None = None
    media_type: str | None = None
    path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="resource id")
        validate_id(self.dataset_id, field_name="dataset id")


@dataclass(frozen=True)
class Variable(MetadataModel):
    """A measured variable in a dataset or series."""

    id: str
    dataset_id: str
    name: str
    unit: str | None = None
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="variable id")
        validate_id(self.dataset_id, field_name="dataset id")


@dataclass(frozen=True)
class Dimension(MetadataModel):
    """A classification or dimension used to slice observations."""

    id: str
    dataset_id: str
    name: str
    values: list[str] = field(default_factory=list)
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="dimension id")
        validate_id(self.dataset_id, field_name="dataset id")


@dataclass(frozen=True)
class Period(MetadataModel):
    """A period covered by a dataset, resource, or series."""

    id: str
    label: str
    start_date: str | None = None
    end_date: str | None = None
    frequency: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="period id")


@dataclass(frozen=True)
class Territory(MetadataModel):
    """A geographic or administrative territory."""

    id: str
    name: str
    level: str | None = None
    parent_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="territory id")
        if self.parent_id is not None:
            validate_id(self.parent_id, field_name="parent territory id")


@dataclass(frozen=True)
class Series(MetadataModel):
    """A time series or comparable analytical series."""

    id: str
    dataset_id: str
    name: str
    variable_id: str | None = None
    territory_id: str | None = None
    dimensions: dict[str, str] = field(default_factory=dict)
    unit: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="series id")
        validate_id(self.dataset_id, field_name="dataset id")
        if self.variable_id is not None:
            validate_id(self.variable_id, field_name="variable id")
        if self.territory_id is not None:
            validate_id(self.territory_id, field_name="territory id")
        for key in self.dimensions:
            validate_id(key, field_name="dimension id")


@dataclass(frozen=True)
class IngestionRun(MetadataModel):
    """Generic metadata for one ingestion run."""

    id: str
    started_at: str
    status: str = "running"
    source_id: str | None = None
    dataset_id: str | None = None
    finished_at: str | None = None
    resource_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_id(self.id, field_name="ingestion run id")
        if self.source_id is not None:
            validate_id(self.source_id, field_name="source id")
        if self.dataset_id is not None:
            validate_id(self.dataset_id, field_name="dataset id")
        for resource_id in self.resource_ids:
            validate_id(resource_id, field_name="resource id")
        if self.status not in RUN_STATUSES:
            raise MetadataError(f"Invalid ingestion run status: {self.status!r}")

    @classmethod
    def start(
        cls,
        *,
        id: str,
        source_id: str | None = None,
        dataset_id: str | None = None,
        resource_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> IngestionRun:
        """Create a running ingestion run."""
        return cls(
            id=id,
            started_at=isoformat_utc(),
            source_id=source_id,
            dataset_id=dataset_id,
            resource_ids=list(resource_ids or []),
            metadata=dict(metadata or {}),
        )

    def finish(self, *, status: str = "success") -> IngestionRun:
        """Return a finished copy of this ingestion run."""
        return IngestionRun(
            id=self.id,
            started_at=self.started_at,
            status=status,
            source_id=self.source_id,
            dataset_id=self.dataset_id,
            finished_at=isoformat_utc(),
            resource_ids=list(self.resource_ids),
            metadata=dict(self.metadata),
        )


@dataclass(frozen=True)
class MetadataCatalog(MetadataModel):
    """A lightweight in-memory catalog of generic metadata objects."""

    sources: list[Source] = field(default_factory=list)
    datasets: list[Dataset] = field(default_factory=list)
    resources: list[Resource] = field(default_factory=list)
    variables: list[Variable] = field(default_factory=list)
    dimensions: list[Dimension] = field(default_factory=list)
    series: list[Series] = field(default_factory=list)
    periods: list[Period] = field(default_factory=list)
    territories: list[Territory] = field(default_factory=list)
    generated_at: str = field(default_factory=isoformat_utc)
    metadata: dict[str, Any] = field(default_factory=dict)

    def validate_references(self) -> None:
        """Validate common source/dataset/resource references."""
        source_ids = {source.id for source in self.sources}
        dataset_ids = {dataset.id for dataset in self.datasets}
        resource_ids = {resource.id for resource in self.resources}
        variable_ids = {variable.id for variable in self.variables}
        territory_ids = {territory.id for territory in self.territories}

        for dataset in self.datasets:
            if dataset.source_id not in source_ids:
                raise MetadataError(f"Unknown source for dataset: {dataset.id}")
        for resource in self.resources:
            if resource.dataset_id not in dataset_ids:
                raise MetadataError(f"Unknown dataset for resource: {resource.id}")
        for variable in self.variables:
            if variable.dataset_id not in dataset_ids:
                raise MetadataError(f"Unknown dataset for variable: {variable.id}")
        for dimension in self.dimensions:
            if dimension.dataset_id not in dataset_ids:
                raise MetadataError(f"Unknown dataset for dimension: {dimension.id}")
        for item in self.series:
            if item.dataset_id not in dataset_ids:
                raise MetadataError(f"Unknown dataset for series: {item.id}")
            if item.variable_id is not None and item.variable_id not in variable_ids:
                raise MetadataError(f"Unknown variable for series: {item.id}")
            if item.territory_id is not None and item.territory_id not in territory_ids:
                raise MetadataError(f"Unknown territory for series: {item.id}")
        for resource_id in resource_ids:
            validate_id(resource_id, field_name="resource id")
