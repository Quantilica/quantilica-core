"""File-based cache helpers for deterministic data downloads."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from .dates import isoformat_utc, parse_iso_datetime, utc_now
from .exceptions import StorageError
from .files import ensure_dir, sha256_bytes, write_bytes_atomic, write_text_atomic


@dataclass(frozen=True)
class CacheEntry:
    """Metadata for one cached object."""

    key: str
    path: str
    created_at: str
    size_bytes: int
    sha256: str
    metadata: dict[str, Any]


class FileCache:
    """Simple content cache stored on the local filesystem."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = ensure_dir(root)

    def key_for_url(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """Return a deterministic key for a URL request."""
        parts = [url]
        if params:
            parts.append(urlencode(sorted(params.items()), doseq=True))
        if headers:
            normalized_headers = {
                key.lower(): value for key, value in sorted(headers.items())
            }
            parts.append(json.dumps(normalized_headers, sort_keys=True))
        return sha256_bytes("\n".join(parts).encode("utf-8"))

    def content_path(self, key: str) -> Path:
        """Return the content path for a cache key."""
        return self.root / key[:2] / key[2:]

    def metadata_path(self, key: str) -> Path:
        """Return the metadata path for a cache key."""
        return self.root / key[:2] / f"{key[2:]}.json"

    def exists(self, key: str) -> bool:
        """Return True when content and metadata exist for a key."""
        return self.content_path(key).exists() and self.metadata_path(key).exists()

    def read_bytes(self, key: str) -> bytes:
        """Read cached bytes."""
        path = self.content_path(key)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise StorageError(f"Could not read cache entry: {key}") from exc

    def read_metadata(self, key: str) -> CacheEntry:
        """Read cache metadata."""
        path = self.metadata_path(key)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(f"Could not read cache metadata: {key}") from exc
        return CacheEntry(**payload)

    def write_bytes(
        self,
        key: str,
        content: bytes,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> CacheEntry:
        """Write content and metadata for a cache key."""
        content_path = self.content_path(key)
        metadata_path = self.metadata_path(key)
        write_bytes_atomic(content_path, content)

        entry = CacheEntry(
            key=key,
            path=str(content_path.relative_to(self.root)),
            created_at=isoformat_utc(),
            size_bytes=len(content),
            sha256=sha256_bytes(content),
            metadata=dict(metadata or {}),
        )
        write_text_atomic(
            metadata_path,
            json.dumps(asdict(entry), ensure_ascii=False, indent=2, sort_keys=True),
        )
        return entry

    def is_fresh(self, key: str, *, ttl_seconds: int | None = None) -> bool:
        """Return True if a cache entry exists and is within TTL."""
        if not self.exists(key):
            return False
        if ttl_seconds is None:
            return True
        entry = self.read_metadata(key)
        created_at = parse_iso_datetime(entry.created_at)
        age = (utc_now() - created_at).total_seconds()
        return age <= ttl_seconds

    def get_bytes(self, key: str, *, ttl_seconds: int | None = None) -> bytes | None:
        """Return cached bytes when present and fresh, otherwise None."""
        if not self.is_fresh(key, ttl_seconds=ttl_seconds):
            return None
        return self.read_bytes(key)
