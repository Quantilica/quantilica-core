"""Layout contract for the Hive-partitioned Parquet lake.

The lake is a layer of the ecosystem, not a package: whoever honours this
contract writes to it, and whoever reads it does not need to know which
fetcher produced the data. This module holds only the stdlib-level pieces
of the contract (root, file keys, Hive paths, the ``_dataset.json``
descriptor, the per-dataset lock and converter discovery); writers and
readers live in ``quantilica-analytics``.

Layout::

    $QUANTILICA_LAKE_DIR/
      <source_id>/<dataset_id>/
        _dataset.json
        <k1>=<v1>/[<k2>=<v2>/...]
          <file_key>[-NNN].parquet
"""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from importlib.metadata import entry_points
from pathlib import Path, PurePath
from typing import Any

from .files import write_text_atomic

try:  # POSIX
    import fcntl
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    fcntl = None  # type: ignore[assignment]

try:  # Windows
    import msvcrt
except ImportError:
    msvcrt = None  # type: ignore[assignment]

LAKE_DIR_ENV = "QUANTILICA_LAKE_DIR"
DATA_DIR_ENV = "QUANTILICA_DATA_DIR"
LAKE_ENTRY_POINT_GROUP = "quantilica.lake"
DESCRIPTOR_FILENAME = "_dataset.json"
LOCK_FILENAME = ".lock"
LAYOUT_VERSION = 1

# Hive's marker for a missing partition value.
HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"

PARTITION_SOURCES = ("file", "rows")

_PARTITION_VALUE = re.compile(r"^[0-9A-Za-z._-]+$")
_PARTITION_KEY = re.compile(r"^[a-z][a-z0-9_]*$")
_PARTITION_SEGMENT = re.compile(r"^([^=/]+)=(.*)$")
# Extensions of raw files and compression wrappers dropped by ``file_key``.
# An allow-list, so dots that are part of the name ("v1.2") survive.
_RAW_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".txt",
        ".dat",
        ".json",
        ".jsonl",
        ".ndjson",
        ".xml",
        ".xls",
        ".xlsx",
        ".dbf",
        ".parquet",
        ".tar",
        ".zip",
        ".7z",
        ".rar",
        ".gz",
        ".tgz",
        ".bz2",
        ".xz",
        ".zst",
    }
)
_DATE_SUFFIX = re.compile(r"@[0-9T]+$")


def lake_root() -> Path:
    """Resolve the lake root directory.

    ``QUANTILICA_LAKE_DIR`` wins; otherwise ``$QUANTILICA_DATA_DIR/lake``;
    otherwise ``~/data/lake``. Empty variables count as unset.
    """
    explicit = os.environ.get(LAKE_DIR_ENV)
    if explicit:
        return Path(explicit).expanduser()
    data_dir = os.environ.get(DATA_DIR_ENV)
    if data_dir:
        return Path(data_dir).expanduser() / "lake"
    return Path.home() / "data" / "lake"


def file_key(path_or_name: str | PurePath) -> str:
    """Return the raw file's key: no extension and no ``@date`` suffix.

    ``caged_202401.7z`` -> ``caged_202401``;
    ``investidores@20261021.csv`` -> ``investidores``;
    ``data.csv.gz`` -> ``data``. Only the final path component is used,
    and stacked wrappers (``.tar.gz``, ``.csv.zip``) are all removed.
    """
    name = (
        path_or_name.name if isinstance(path_or_name, PurePath) else str(path_or_name)
    )
    # Accept Windows-style separators in plain strings.
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    while True:
        stem, dot, ext = name.rpartition(".")
        if stem and dot and f".{ext.lower()}" in _RAW_SUFFIXES:
            name = stem
        else:
            break
    return _DATE_SUFFIX.sub("", name)


def encode_partition_value(value: str | int | float | None) -> str:
    """Encode a partition value for use in a ``key=value`` directory name.

    ``None`` and the empty string map to ``__HIVE_DEFAULT_PARTITION__``.
    Anything outside ``[0-9A-Za-z._-]`` raises ``ValueError``, as do ``.``
    and ``..`` (they would escape the partition directory).
    """
    if value is None or value == "":
        return HIVE_DEFAULT_PARTITION
    text = str(value)
    if not _PARTITION_VALUE.fullmatch(text) or text in {".", ".."}:
        raise ValueError(
            f"Invalid partition value {text!r}: only [0-9A-Za-z._-] is allowed."
        )
    return text


def _validate_partition_key(key: str) -> str:
    if not isinstance(key, str) or not _PARTITION_KEY.fullmatch(key):
        raise ValueError(
            f"Invalid partition key {key!r}: use snake_case ASCII "
            "(lowercase letters, digits and '_', starting with a letter)."
        )
    return key


def partition_dir(base_dir: Path, partitions: Mapping[str, Any]) -> Path:
    """Build ``base_dir/k1=v1/k2=v2`` in the mapping's insertion order."""
    path = Path(base_dir)
    for key, value in partitions.items():
        path = path / f"{_validate_partition_key(key)}={encode_partition_value(value)}"
    return path


def parse_partition_path(path: PurePath, base_dir: PurePath) -> dict[str, str]:
    """Extract ``{key: value}`` from the ``key=value`` segments of ``path``.

    ``path`` may be a partition directory or a ``.parquet`` file inside one;
    segments are read relative to ``base_dir``. Values stay as strings, and
    the missing-value marker is returned untouched. Raises ``ValueError``
    when ``path`` is not under ``base_dir``.
    """
    try:
        relative = path.relative_to(base_dir)
    except ValueError:
        raise ValueError(f"{path} is not under {base_dir}") from None
    parts = list(relative.parts)
    if parts and relative.suffix.lower() == ".parquet":
        parts.pop()
    partitions: dict[str, str] = {}
    for part in parts:
        match = _PARTITION_SEGMENT.match(part)
        if match:
            partitions[match.group(1)] = match.group(2)
    return partitions


@dataclass
class DatasetDescriptor:
    """Model of a dataset's ``_dataset.json``."""

    source_id: str
    dataset_id: str
    alias: str
    partition_keys: list[str] = field(default_factory=list)
    partition_types: dict[str, str] = field(default_factory=dict)
    # "file": partition derived from the raw file's identity (default);
    # "rows": partition derived from row contents (replaced by generation).
    partition_source: str = "file"
    contract_id: str | None = None
    contract_version: int | None = None
    converter: str | None = None
    layout_version: int = LAYOUT_VERSION

    def __post_init__(self) -> None:
        if self.partition_source not in PARTITION_SOURCES:
            raise ValueError(
                f"Invalid partition_source {self.partition_source!r}: "
                f"expected one of {PARTITION_SOURCES}."
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> DatasetDescriptor:
        """Build a descriptor, ignoring unknown keys (newer writers)."""
        known = {f.name for f in fields(cls)}
        data = {k: v for k, v in d.items() if k in known}
        missing = [k for k in ("source_id", "dataset_id", "alias") if k not in data]
        if missing:
            raise ValueError(f"Descriptor is missing required keys: {missing}")
        return cls(**data)

    @classmethod
    def from_json(cls, s: str) -> DatasetDescriptor:
        data = json.loads(s)
        if not isinstance(data, dict):
            raise ValueError("Descriptor JSON must be an object.")
        return cls.from_dict(data)

    def save(self, dataset_dir: Path) -> Path:
        """Atomically write ``<dataset_dir>/_dataset.json``; return its path."""
        return write_text_atomic(
            Path(dataset_dir) / DESCRIPTOR_FILENAME, self.to_json()
        )

    @classmethod
    def load(cls, dataset_dir: Path) -> DatasetDescriptor:
        """Read ``<dataset_dir>/_dataset.json``."""
        path = Path(dataset_dir) / DESCRIPTOR_FILENAME
        return cls.from_json(path.read_text(encoding="utf-8"))


@contextmanager
def dataset_lock(dataset_dir: Path, *, blocking: bool = True) -> Iterator[None]:
    """Hold an exclusive lock on ``<dataset_dir>/.lock``.

    One writer per dataset; readers take no lock. With ``blocking=False`` a
    held lock raises ``BlockingIOError`` immediately. POSIX uses ``flock``;
    Windows locks the file's first byte with ``msvcrt.locking``. Elsewhere
    this is a no-op: callers get no mutual exclusion. The lock file is left
    in place on release, since removing it would race with another process
    waiting on it.
    """
    if fcntl is None:
        if msvcrt is None:
            yield
            return
        with _msvcrt_lock(Path(dataset_dir), blocking=blocking):
            yield
        return
    directory = Path(dataset_dir)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / LOCK_FILENAME).open("a+") as handle:
        flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        fcntl.flock(handle.fileno(), flags)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _msvcrt_lock(directory: Path, *, blocking: bool) -> Iterator[None]:
    """``dataset_lock`` on Windows: lock byte 0 of the lock file.

    ``LK_NBLCK`` fails at once on a held lock; blocking mode retries it, since
    ``LK_LOCK`` gives up after ten one-second attempts.
    """
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / LOCK_FILENAME).open("a+") as handle:
        fd = handle.fileno()
        while True:
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except OSError as exc:
                if not blocking:
                    raise BlockingIOError(
                        f"dataset lock held: {directory / LOCK_FILENAME}"
                    ) from exc
                time.sleep(0.1)
        try:
            yield
        finally:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


def load_lake_converter(source_id: str) -> Any | None:
    """Load the ``quantilica.lake`` entry point named ``source_id``.

    Returns ``None`` when no installed package provides one. Errors raised
    while importing a found entry point propagate to the caller.
    """
    for entry_point in entry_points(group=LAKE_ENTRY_POINT_GROUP, name=source_id):
        return entry_point.load()
    return None
