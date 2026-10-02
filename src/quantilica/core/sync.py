"""Remote freshness probes and local skip policies for synchronized data.

This module composes the pieces needed to decide whether a locally cached
artifact can be safely skipped during ingestion:

- :class:`RemoteStat`: normalized metadata reported by a remote resource.
- :class:`FreshnessProbe`: protocol implemented by the HTTP and FTP probes.
- :func:`should_skip`: the single decision point used by ingestion jobs.
- :func:`is_manifest_valid`: strict artifact/manifest consistency check.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import email.utils
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

from .dates import to_utc
from .exceptions import StorageError
from .files import sha256_file
from .manifests import MANIFEST_SIDECAR_SUFFIX, manifest_sidecar_path

if TYPE_CHECKING:  # evita custo/ciclo em tempo de importação
    from .ftp import FtpClient
    from .http import HttpClient

SkipPolicy = Literal["freshness", "strict_manifest", "exists", "never"]
"""Skip policies accepted by :func:`should_skip`."""

_FRESHNESS_BUFFER_S = 1.0
"""Tolerance (seconds) when comparing local mtime against remote timestamps."""


@dataclass(frozen=True)
class RemoteStat:
    """Metadata reported by a remote resource, used as freshness evidence.

    All fields are optional: a source may not expose size, timestamps or an
    ETag. ``None`` always means "not advertised by the remote".
    """

    size: int | None = None
    last_modified: dt.datetime | None = None
    etag: str | None = None


@runtime_checkable
class FreshnessProbe(Protocol):
    """Protocol for probes that report remote metadata for a resource."""

    def probe(self, url: str) -> RemoteStat | None:
        """Return the remote metadata for ``url``.

        Implementations must never raise: any failure (network error,
        unsupported operation) should return ``None`` so callers fall back to
        (re)downloading instead of skipping.

        Args:
            url: The remote resource URL or path.

        Returns:
            RemoteStat | None: The remote stats, or None when unavailable.
        """
        ...


class HttpFreshnessProbe:
    """Freshness probe backed by a :class:`~quantilica.core.http.HttpClient`.

    Uses ``HEAD`` internally (falling back to a streaming ``GET`` for servers
    that reject ``HEAD``) and reads ``Content-Length``, ``Last-Modified`` and
    ``ETag`` from the response headers.
    """

    def __init__(self, client: HttpClient) -> None:
        """Initialize the probe.

        Args:
            client: The HTTP client used to fetch remote metadata.
        """
        self._client = client

    def probe(self, url: str) -> RemoteStat | None:
        """Inspect ``url`` and return its remote stats.

        Args:
            url: The URL to inspect.

        Returns:
            RemoteStat | None: The stats reported by the server, or None when
            the request fails, redirects away, or the server reports nothing
            useful (e.g. ``Content-Length=0`` on HEAD).
        """
        try:
            response = self._client.head_or_get(url)
        except Exception:
            return None
        size = response.headers.get("Content-Length")
        last_modified = response.headers.get("Last-Modified")
        etag = response.headers.get("ETag")
        stat = RemoteStat(
            size=int(size) if size and size.isdigit() else None,
            last_modified=_parse_http_date(last_modified),
            etag=etag or None,
        )
        if _is_empty_stat(stat):
            return None
        return stat


class FtpFreshnessProbe:
    """Freshness probe backed by a :class:`~quantilica.core.ftp.FtpClient`.

    Issues ``SIZE`` (best-effort) and ``MDTM`` commands over a single
    connection. ``MDTM`` timestamps are interpreted as UTC (RFC 3659).
    """

    def __init__(self, client: FtpClient) -> None:
        """Initialize the probe.

        Args:
            client: The FTP client used to query the server.
        """
        self._client = client

    def probe(self, url: str) -> RemoteStat | None:
        """Inspect the remote path and return its remote stats.

        Args:
            url: The remote path, optionally as ``ftp://host/path``.

        Returns:
            RemoteStat | None: The stats, or None when the connection or both
            commands fail.
        """
        path = _ftp_path(url)
        try:
            with self._client._connected() as ftp:
                stat = RemoteStat()
                with contextlib.suppress(Exception):
                    if (size := ftp.size(path)) is not None:
                        stat = replace(stat, size=int(size) or None)
                with contextlib.suppress(Exception):
                    stat = replace(
                        stat,
                        last_modified=_parse_ftp_timestamp(
                            ftp.sendcmd(f"MDTM {path}").split()[-1]
                        ),
                    )
        except Exception:
            return None
        if _is_empty_stat(stat):
            return None
        return stat


def should_skip(
    target: Path,
    stat: RemoteStat | None,
    policy: SkipPolicy = "freshness",
    force: bool = False,
) -> bool:
    """Decide whether a local download can be skipped for ``target``.

    Args:
        target: The local artifact path.
        stat: Remote stats from a probe (may be None when unknown).
        policy: One of:

            - ``freshness``: skip when the local file is not older than the
              remote (compares size, then ``Last-Modified``, then ``ETag``
              against the manifest's ``source_meta.etag``); when the remote
              state is unknown, the download is **not** skipped.
            - ``strict_manifest``: skip only when a valid manifest sidecar
              exists and the on-disk file matches its ``sha256`` and
              ``size_bytes``.
            - ``exists``: skip whenever the target file exists.
            - ``never``: never skip.
        force: Force re-download regardless of policy (skips are disabled).

    Returns:
        bool: True when the (re)download of ``target`` can be skipped.
    """
    if force or policy == "never":
        return False
    if not target.is_file():
        return False
    if policy == "exists":
        return True
    if policy == "strict_manifest":
        return is_manifest_valid(manifest_sidecar_path(target))
    return _is_fresh(target, stat)


def is_manifest_valid(manifest_path: Path) -> bool:
    """Check that a manifest sidecar is consistent with its artifact.

    A valid manifest parses as JSON, records ``sha256`` (string) and
    ``size_bytes`` (integer), and points at an existing artifact file whose
    size and SHA-256 digest match the recorded values. The artifact is
    derived as ``<manifest stem>`` (e.g. ``data.bin.manifest.json`` ->
    ``data.bin``).

    Args:
        manifest_path: Path to the ``*.manifest.json`` sidecar.

    Returns:
        bool: True when the manifest exists and matches the artifact.
    """
    payload = _read_manifest_payload(manifest_path)
    if payload is None:
        return False
    target = manifest_target_path(manifest_path)
    if not target.is_file():
        return False
    sha256 = payload.get("sha256")
    size_bytes = payload.get("size_bytes")
    if not isinstance(sha256, str):
        return False
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool):
        return False
    try:
        local_size = target.stat().st_size
    except OSError:
        return False
    if local_size != size_bytes:
        return False
    try:
        return sha256_file(target) == sha256
    except StorageError:
        return False


def manifest_target_path(manifest_path: Path) -> Path:
    """Return the artifact path a manifest sidecar describes.

    Inverse of :func:`manifest_sidecar_path`: strips the trailing
    ``.manifest.json`` suffix (e.g. ``data.bin.manifest.json`` ->
    ``data.bin``).

    Args:
        manifest_path: Path to the ``*.manifest.json`` sidecar.

    Returns:
        Path: The artifact path.
    """
    name = manifest_path.name
    if name.endswith(MANIFEST_SIDECAR_SUFFIX):
        name = name[: -len(MANIFEST_SIDECAR_SUFFIX)]
    return manifest_path.with_name(name)


def _is_fresh(target: Path, stat: RemoteStat | None) -> bool:
    """Evaluate the ``freshness`` skip policy against a remote stat."""
    if stat is None:
        return False
    try:
        local_stat = target.stat()
    except OSError:
        return False
    if stat.size is not None and local_stat.st_size != stat.size:
        return False
    if stat.last_modified is not None:
        return local_stat.st_mtime >= (
            stat.last_modified.timestamp() - _FRESHNESS_BUFFER_S
        )
    if stat.etag is not None:
        return _manifest_etag(manifest_sidecar_path(target)) == stat.etag
    return False


def _manifest_etag(manifest_path: Path) -> str | None:
    """Read the recorded ``source_meta.etag`` from a manifest sidecar."""
    payload = _read_manifest_payload(manifest_path)
    if payload is None:
        return None
    meta = payload.get("source_meta")
    if not isinstance(meta, dict):
        return None
    etag = meta.get("etag")
    return etag if isinstance(etag, str) else None


def _read_manifest_payload(manifest_path: Path) -> dict[str, Any] | None:
    """Read and parse a manifest JSON file, or return None on any failure."""
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _is_empty_stat(stat: RemoteStat) -> bool:
    """Return True when a probe succeeded but reported no usable metadata."""
    return stat.size is None and stat.last_modified is None and stat.etag is None


def _parse_http_date(value: str | None) -> dt.datetime | None:
    """Parse an HTTP date header into a timezone-aware UTC datetime."""
    if not value:
        return None
    try:
        return to_utc(email.utils.parsedate_to_datetime(value))
    except (TypeError, ValueError):
        return None


def _parse_ftp_timestamp(stamp: str) -> dt.datetime | None:
    """Parse an FTP MDTM timestamp (``YYYYMMDDHHMMSS``, UTC) into datetime."""
    try:
        parsed = dt.datetime.strptime(stamp, "%Y%m%d%H%M%S")
    except (ValueError, TypeError):
        return None
    return to_utc(parsed)


def _ftp_path(url: str) -> str:
    """Reduce an FTP URL to its server path portion."""
    if url.startswith("ftp://"):
        rest = url[len("ftp://") :]
        slash = rest.find("/")
        return "/" if slash == -1 else rest[slash:]
    return url
