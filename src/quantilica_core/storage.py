"""Local object storage abstraction for data artifacts."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal
from pathlib import Path, PurePosixPath

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


class BaseDataRepository:
    """Base class for data client repositories.

    Provides a standard structure for storing raw and processed data.
    """

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.storage = LocalStorage(root)

    def raw_path(self, dataset_id: str, *subkeys: str) -> Path:
        """Return a path for raw data."""
        key = "/".join(["raw", dataset_id, *subkeys])
        return self.storage.path_for(key)

    def processed_path(self, dataset_id: str, *subkeys: str) -> Path:
        """Return a path for processed data."""
        key = "/".join(["processed", dataset_id, *subkeys])
        return self.storage.path_for(key)

    def docs_path(self, dataset_id: str, *subkeys: str) -> Path:
        """Return a path for documentation and metadata."""
        key = "/".join(["docs", dataset_id, *subkeys])
        return self.storage.path_for(key)


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
                self._key_from_path(path)
                for path in base.rglob("*")
                if path.is_file()
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
