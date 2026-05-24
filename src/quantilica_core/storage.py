"""Local object storage abstraction for data artifacts."""

from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Literal

from .dates import isoformat_utc
from .exceptions import StorageError
from .files import ensure_dir, sha256_file, write_bytes_atomic, write_text_atomic


def stamp_filename(
    base: str,
    ext: str,
    timestamp: date | datetime | None,
    *,
    precision: Literal["date", "datetime"] = "date",
) -> str:
    """Return ``{base}@{stamp}.{ext}`` or ``{base}.{ext}`` when timestamp is None.

    ``precision="date"`` → ``@YYYYMMDD``
    ``precision="datetime"`` → ``@YYYYMMDDTHHMMSS``
    """
    if timestamp is None:
        return f"{base}.{ext}"
    if precision == "datetime":
        if isinstance(timestamp, datetime):
            return f"{base}@{timestamp:%Y%m%dT%H%M%S}.{ext}"
        return f"{base}@{timestamp:%Y%m%d}T000000.{ext}"
    return f"{base}@{timestamp:%Y%m%d}.{ext}"


def build_stamped_filename(
    *parts: str | int | None,
    ext: str,
    timestamp: date | datetime | None = None,
    precision: Literal["date", "datetime"] = "date",
) -> str:
    """Join truthy ``parts`` with ``_`` and apply :func:`stamp_filename`.

    Example: ``build_stamped_filename("exp", 2024, ext="csv", timestamp=d)``
    → ``exp_2024@20240315.csv``. Falsy parts (``None``/``""``) are dropped.
    """
    base = "_".join(str(p) for p in parts if p not in (None, ""))
    return stamp_filename(base, ext, timestamp, precision=precision)


def slugify(value: str) -> str:
    """Normalize a string to a URL-friendly slug."""
    value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[-\s]+", "-", value)


class BaseDataRepository:
    """Base class for data client repositories.

    Provides a standard structure for storing data under ``{dataset_id}/``
    directly at the storage root (no ``raw/``/``processed/``/``docs/`` wrapper).
    Fetchers with additional data categories (documentation, processed
    artifacts) are responsible for organizing them as they see fit, typically
    by using ``self.storage.path_for(...)`` directly.
    """

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.storage = LocalStorage(root)

    def dataset_path(self, dataset_id: str, *subkeys: str) -> Path:
        """Return the absolute path of ``dataset_id/subkeys`` under the storage root."""
        key = "/".join([dataset_id, *subkeys])
        return self.storage.path_for(key)

    def list_dataset_ids(self) -> list[str]:
        """Return all dataset directories at the storage root, sorted."""
        root = self.storage.root
        if not root.exists():
            return []
        return sorted(entry.name for entry in root.iterdir() if entry.is_dir())


class StampedDataRepository(BaseDataRepository):
    """BaseDataRepository with slug@timestamp filename conventions.

    Provides helpers for repositories whose files are stamped as
    ``{slug}@{YYYYMMDDTHHMMSS}.{ext}`` so multiple snapshots coexist and
    the latest one can be queried efficiently.
    """

    def get_latest_stamped_file(
        self, dataset_id: str, slug: str, ext: str = "csv"
    ) -> Path | None:
        """Return the newest ``{slug}@*.{ext}`` file under ``{dataset_id}/``."""
        dataset_dir = self.dataset_path(dataset_id)
        if not dataset_dir.exists():
            return None
        latest_file: Path | None = None
        latest_ts = ""
        for f in dataset_dir.iterdir():
            if not f.is_file():
                continue
            if not (f.name.startswith(f"{slug}@") and f.name.endswith(f".{ext}")):
                continue
            ts = f.name[len(slug) + 1 : -(len(ext) + 1)]
            if ts > latest_ts:
                latest_ts = ts
                latest_file = f
        return latest_file

    def get_all_latest_stamped_files(
        self, dataset_id: str, ext: str = "csv"
    ) -> list[Path]:
        """Return one file per slug — the latest @timestamp variant of each."""
        dataset_dir = self.dataset_path(dataset_id)
        if not dataset_dir.exists():
            return []
        by_slug: dict[str, tuple[Path, str]] = {}
        for f in dataset_dir.glob(f"*.{ext}"):
            if "@" not in f.name:
                continue
            slug, _, rest = f.name.partition("@")
            ts = rest.removesuffix(f".{ext}")
            current = by_slug.get(slug)
            if current is None or ts > current[1]:
                by_slug[slug] = (f, ts)
        return [pair[0] for pair in by_slug.values()]


@dataclass(frozen=True)
class ObjectStat:
    """Metadata about an object stored by a storage backend."""

    key: str
    path: str
    size_bytes: int
    sha256: str
    modified_at: str


class LocalStorage:
    """Filesystem-backed object storage using POSIX-style keys."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = ensure_dir(root).resolve()

    def path_for(self, key: str) -> Path:
        """Return the absolute path for a storage key."""
        normalized_key = self.normalize_key(key)
        target = (self.root / Path(normalized_key)).resolve()
        if target != self.root and self.root not in target.parents:
            raise StorageError(f"Storage key escapes root: {key}")
        return target

    def normalize_key(self, key: str) -> str:
        """Normalize and validate an object key."""
        if not key or key.strip() == "":
            raise StorageError("Storage key cannot be empty")
        normalized = PurePosixPath(key.replace("\\", "/"))
        if normalized.is_absolute() or ".." in normalized.parts:
            raise StorageError(f"Invalid storage key: {key}")
        return normalized.as_posix()

    def exists(self, key: str) -> bool:
        """Return True if a key exists and is a file."""
        return self.path_for(key).is_file()

    def read_bytes(self, key: str) -> bytes:
        """Read an object as bytes."""
        path = self.path_for(key)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise StorageError(f"Could not read object: {key}") from exc

    def read_text(self, key: str, *, encoding: str = "utf-8") -> str:
        """Read an object as text."""
        path = self.path_for(key)
        try:
            return path.read_text(encoding=encoding)
        except OSError as exc:
            raise StorageError(f"Could not read object: {key}") from exc

    def write_bytes(self, key: str, content: bytes) -> ObjectStat:
        """Write bytes atomically and return object metadata."""
        path = write_bytes_atomic(self.path_for(key), content)
        return self.stat(self._key_from_path(path))

    def write_text(
        self,
        key: str,
        content: str,
        *,
        encoding: str = "utf-8",
    ) -> ObjectStat:
        """Write text atomically and return object metadata."""
        path = write_text_atomic(self.path_for(key), content, encoding=encoding)
        return self.stat(self._key_from_path(path))

    def delete(self, key: str, *, missing_ok: bool = True) -> None:
        """Delete an object."""
        path = self.path_for(key)
        try:
            path.unlink(missing_ok=missing_ok)
        except OSError as exc:
            raise StorageError(f"Could not delete object: {key}") from exc

    def list(self, prefix: str = "") -> list[str]:
        """List object keys under a prefix."""
        base = self.path_for(prefix) if prefix else self.root
        if not base.exists():
            return []
        if base.is_file():
            return [self._key_from_path(base)]
        try:
            keys = [
                self._key_from_path(path) for path in base.rglob("*") if path.is_file()
            ]
        except OSError as exc:
            raise StorageError(f"Could not list objects: {prefix}") from exc
        return sorted(keys)

    def stat(self, key: str) -> ObjectStat:
        """Return object metadata."""
        path = self.path_for(key)
        try:
            raw_stat = path.stat()
        except OSError as exc:
            raise StorageError(f"Could not stat object: {key}") from exc
        if not path.is_file():
            raise StorageError(f"Object is not a file: {key}")
        return ObjectStat(
            key=self._key_from_path(path),
            path=str(path),
            size_bytes=raw_stat.st_size,
            sha256=sha256_file(path),
            modified_at=isoformat_utc(
                datetime.fromtimestamp(raw_stat.st_mtime, tz=UTC)
            ),
        )

    def _key_from_path(self, path: Path) -> str:
        relative = path.resolve().relative_to(self.root)
        return relative.as_posix()
