"""HTTP helpers for data clients and ingestion jobs."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime as dt
import email.utils
import hashlib
import logging
import os
import ssl
import tempfile
import threading
import time
from collections.abc import AsyncGenerator, Callable, Mapping
from pathlib import Path
from typing import Any

import httpx2

from .exceptions import FetchError, StorageError
from .files import check_free_space, ensure_parent, write_bytes_atomic
from .logging import bind_context, get_logger, log_step
from .manifests import (
    DownloadManifest,
    SourceMetadata,
    manifest_sidecar_path,
    write_manifest_sidecar,
)
from .retry import RetryError, async_retry_call, exponential_delay, retry_call

ProgressCallback = Callable[[int, int], None]
"""Callback invoked as ``(downloaded_bytes, total_bytes)`` during a stream.

``total_bytes`` is ``0`` when the remote does not advertise ``Content-Length``.
"""

DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024

DEFAULT_USER_AGENT = "quantilica-core"
DEFAULT_TIMEOUT = 60.0
RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}

DEFAULT_LIMITS = httpx2.Limits(
    max_connections=50, max_keepalive_connections=20, keepalive_expiry=30.0
)

# Realistic browser headers for sites that reject non-browser User-Agents
# (e.g. some Brazilian government portals served behind WAFs).
BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


class HttpStatusError(FetchError):
    """Raised when an HTTP response has an unexpected status code."""

    def __init__(self, url: str, status_code: int) -> None:
        """Initialize the HttpStatusError.

        Args:
            url: The URL that caused the error.
            status_code: The HTTP status code received.
        """
        super().__init__(f"HTTP {status_code} while fetching {url}")
        self.url = url
        self.status_code = status_code


class RetryableHttpStatusError(HttpStatusError):
    """Raised for HTTP status codes that are safe to retry."""


DEFAULT_RETRY_EXCEPTIONS = (
    httpx2.TimeoutException,
    httpx2.ConnectError,
    httpx2.NetworkError,
    httpx2.RemoteProtocolError,
    RetryableHttpStatusError,
    ConnectionError,
    TimeoutError,
)

VerifyOption = bool | str | ssl.SSLContext
"""TLS verification setting accepted by :class:`HttpClient`.

``True`` (default) verifies with the system/httpx trust store, a ``str``
path points at a custom CA bundle file or directory, an ``SSLContext``
is used as-is, and ``False`` disables verification (ops escape hatch
only — never ship it as a default).
"""

_FALSE_VALUES = frozenset({"0", "false", "no", "off", "disable", "disabled"})
_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enable", "enabled"})


def resolve_verify_from_env() -> bool | str:
    """Resolve TLS verification from the environment.

    Precedence:

    1. ``QUANTILICA_CA_BUNDLE`` pointing at an existing file/dir.
    2. ``QUANTILICA_SSL_VERIFY``: falsy value (``0``/``false``/``no``/
       ``off``) disables; truthy value enables; an existing path is
       used as the CA bundle.
    3. Default ``True``.

    Disabling verification is an explicit operator decision for hosts
    with broken chains (e.g. missing intermediates) — it must never be
    a code default.
    """
    bundle = (os.environ.get("QUANTILICA_CA_BUNDLE") or "").strip()
    if bundle and Path(bundle).exists():
        return bundle
    raw = (os.environ.get("QUANTILICA_SSL_VERIFY") or "").strip()
    if not raw:
        return True
    lowered = raw.lower()
    if lowered in _FALSE_VALUES:
        return False
    if lowered in _TRUE_VALUES:
        return True
    if Path(raw).exists():
        return raw
    return True


class RateLimiter:
    """Thread-safe minimal rate limiter (fixed spacing between slots).

    A fast lock is held only to reserve the next available slot; the actual
    wait (if any) happens outside the lock, so concurrent threads never block
    each other while sleeping.
    """

    def __init__(self, min_interval: float) -> None:
        """Initialize the rate limiter.

        Args:
            min_interval: Minimum spacing in seconds between consecutive
                slots. A value of ``0`` disables waiting entirely.
        """
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self) -> None:
        """Reserve the next rate-limited slot, sleeping if needed.

        Returns:
            None
        """
        if self.min_interval <= 0:
            return
        clock = time.monotonic
        with self._lock:
            now = clock()
            slot = self._next_slot if self._next_slot > now else now
            delay = slot - now
            self._next_slot = slot + self.min_interval
        if delay > 0:
            time.sleep(delay)


_RateLimiter = RateLimiter
"""Backward-compatible alias for the pre-0.9 private name."""

__all__ = [
    "AsyncHttpClient",
    "BROWSER_HEADERS",
    "DEFAULT_LIMITS",
    "DEFAULT_RETRY_EXCEPTIONS",
    "DEFAULT_STREAM_CHUNK_SIZE",
    "DEFAULT_TIMEOUT",
    "DEFAULT_USER_AGENT",
    "HttpClient",
    "HttpStatusError",
    "ProgressCallback",
    "RETRY_STATUS_CODES",
    "RateLimiter",
    "RetryableHttpStatusError",
    "VerifyOption",
    "is_remote_more_recent",
    "resolve_verify_from_env",
]


class HttpClient:
    """Small synchronous HTTP client wrapper around ``httpx2``."""

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
        attempts: int = 3,
        retry_base_delay: float = 1.0,
        verify: VerifyOption = True,
        transport: httpx2.BaseTransport | None = None,
        logger: logging.Logger | None = None,
        cookies: httpx2.Cookies | None = None,
        limits: httpx2.Limits | None = None,
        emulate_browser: bool = False,
        min_interval: float = 0.0,
    ) -> None:
        if emulate_browser:
            default_headers: dict[str, str] = dict(BROWSER_HEADERS)
            # Accept-Encoding seguro: nunca anunciar br/zstd (BCB retorna 406).
            default_headers.setdefault("Accept-Encoding", "gzip, deflate")
            if headers:
                default_headers.update(headers)
            # Garante que Accept-Encoding não seja sobrescrito com br/zstd.
            if "Accept-Encoding" in (headers or {}):
                ae = (headers or {}).get("Accept-Encoding", "")
                if "br" in ae or "zstd" in ae:
                    default_headers["Accept-Encoding"] = "gzip, deflate"
        else:
            default_headers = {"User-Agent": DEFAULT_USER_AGENT}
            if headers:
                default_headers.update(headers)
        self.timeout = timeout
        self.headers = default_headers
        self.follow_redirects = follow_redirects
        self.attempts = attempts
        self.retry_base_delay = retry_base_delay
        self.verify = verify
        self.transport = transport
        self.logger = logger or get_logger(__name__)
        self.cookies = cookies or httpx2.Cookies()
        self.limits = limits or DEFAULT_LIMITS
        self.emulate_browser = emulate_browser
        self._rate_limiter = _RateLimiter(max(0.0, float(min_interval)))
        self._client: httpx2.Client | None = None

    @property
    def min_interval(self) -> float:
        """Minimum spacing in seconds between consecutive requests."""
        return self._rate_limiter.min_interval

    @min_interval.setter
    def min_interval(self, value: float) -> None:
        self._rate_limiter.min_interval = max(0.0, float(value))

    def _build_client(self) -> httpx2.Client:
        return httpx2.Client(
            timeout=self.timeout,
            follow_redirects=self.follow_redirects,
            headers=self.headers,
            verify=self.verify,
            transport=self.transport,
            cookies=self.cookies,
            limits=self.limits,
        )

    def _get_client(self) -> tuple[httpx2.Client, bool]:
        """Retorna (client, is_persistent)."""  # noqa: E501
        if self._client is not None:
            return self._client, True
        return self._build_client(), False

    def __enter__(self) -> HttpClient:
        if self._client is None:
            self._client = self._build_client()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        data: Any | None = None,
        content: bytes | None = None,
        json: Any | None = None,
    ) -> httpx2.Response:
        """Perform an HTTP request with retries.

        Args:
            method: The HTTP method to use.
            url: The URL to request.
            params: Optional query parameters.
            headers: Optional headers.
            data: Optional form data.
            content: Optional byte content.
            json: Optional JSON data.

        Returns:
            httpx2.Response: The HTTP response.

        Raises:
            RetryableHttpStatusError: If a retryable status is received and
                attempts are exhausted.
            HttpStatusError: If a non-retryable error status is received.
        """

        def do_request() -> httpx2.Response:
            self._rate_limiter.acquire()
            start = time.perf_counter()
            client, is_persistent = self._get_client()
            if is_persistent:
                response = client.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    data=data,
                    content=content,
                    json=json,  # noqa: E501
                )
                self.cookies.update(response.cookies)
            else:
                with client as c:
                    response = c.request(
                        method,
                        url,
                        params=params,
                        headers=headers,
                        data=data,
                        content=content,
                        json=json,  # noqa: E501
                    )
                    self.cookies.update(response.cookies)
            elapsed = time.perf_counter() - start
            self.logger.debug(
                bind_context(
                    "HTTP Request",
                    method=method,
                    url=str(response.url),
                    status=response.status_code,
                    elapsed=f"{elapsed:.3f}s",
                )
            )
            if response.status_code in RETRY_STATUS_CODES:
                raise RetryableHttpStatusError(str(response.url), response.status_code)
            try:
                response.raise_for_status()
            except httpx2.HTTPStatusError as exc:
                raise HttpStatusError(str(response.url), response.status_code) from exc
            return response

        return retry_call(
            do_request,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
        )

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx2.Response:
        """Fetch a URL and return the response.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            httpx2.Response: The HTTP response.
        """
        return self.request("GET", url, params=params, headers=headers)

    def head(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx2.Response:
        """Perform a HEAD request and return the response.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            httpx2.Response: The HTTP response.
        """
        return self.request("HEAD", url, params=params, headers=headers)

    def head_or_get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx2.Response:
        """Perform a HEAD request, falling back to GET with streaming if unsupported.

        Some servers don't support HEAD requests (e.g. return 405 Method Not
        Allowed). This method tries HEAD first, and if it fails with 403, 405 or
        501, opens a GET with streaming, reads only headers, and closes the
        connection without downloading the body.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            httpx2.Response: The HTTP response.
        """
        try:
            return self.head(url, params=params, headers=headers)
        except HttpStatusError as e:
            if e.status_code not in (403, 405, 501):
                raise
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)

        def _stream_fallback() -> httpx2.Response:
            self._rate_limiter.acquire()
            with httpx2.Client(
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=request_headers,
                verify=self.verify,
                transport=self.transport,
                cookies=self.cookies,
            ) as client:
                with client.stream("GET", url, params=params) as response:
                    self.cookies.update(response.cookies)
                    try:
                        response.raise_for_status()
                    except httpx2.HTTPStatusError as exc:
                        raise HttpStatusError(
                            str(response.url), response.status_code
                        ) from exc
                    return response

        return retry_call(
            _stream_fallback,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
        )

    def head_metadata(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Perform a HEAD request and return parsed file metadata.

        Returns ``{"size": int, "last_modified": datetime | None}``.
        ``last_modified`` is timezone-aware (UTC) or ``None`` when the header
        is absent or unparseable.  Propagates ``FetchError`` on HTTP failure.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            dict[str, Any]: A dictionary containing 'size' and 'last_modified'.
        """
        resp = self.head(url, params=params, headers=headers)
        size = int(resp.headers.get("Content-Length", 0))
        lm_str = resp.headers.get("Last-Modified")
        last_modified: dt.datetime | None = None
        if lm_str:
            try:
                last_modified = email.utils.parsedate_to_datetime(lm_str)
            except Exception:
                pass
        return {"size": size, "last_modified": last_modified}

    def head_metadata_or_get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Perform a HEAD request for metadata, falling back to GET if unsupported.

        Returns ``{"size": int, "last_modified": datetime | None}``.
        Uses :meth:`head_or_get` internally.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            dict[str, Any]: A dictionary containing 'size' and 'last_modified'.
        """
        resp = self.head_or_get(url, params=params, headers=headers)
        size = int(resp.headers.get("Content-Length", 0))
        lm_str = resp.headers.get("Last-Modified")
        last_modified: dt.datetime | None = None
        if lm_str:
            try:
                last_modified = email.utils.parsedate_to_datetime(lm_str)
            except Exception:
                pass
        return {"size": size, "last_modified": last_modified}

    def head_last_modified_date(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dt.date | None:
        """Return the ``Last-Modified`` date from a HEAD request, or ``None``.

        Never raises: any fetch/parse failure is logged as a warning and
        returns ``None`` (handy for building stamped filenames).

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            dt.date | None: The parsed date, or None if unavailable or on error.
        """
        try:
            meta = self.head_metadata_or_get(url, params=params, headers=headers)
        except Exception as exc:
            self.logger.warning(f"Could not fetch metadata for {url}: {exc}")
            return None
        last_modified = meta.get("last_modified")
        return last_modified.date() if last_modified else None

    def get_bytes(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        progress: ProgressCallback | None = None,
    ) -> bytes:
        """Fetch a URL and return response bytes.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.
            progress: Optional progress callback.

        Returns:
            bytes: The response content.
        """
        if progress is None:
            return self.get(url, params=params, headers=headers).content

        def _stream_attempt() -> bytes:
            if progress is not None:
                progress(0, 0)
            downloaded = 0
            chunks = []
            with self.stream("GET", url, params=params, headers=headers) as response:
                total = int(response.headers.get("Content-Length", 0) or 0)
                for chunk in response.iter_bytes(chunk_size=DEFAULT_STREAM_CHUNK_SIZE):
                    chunks.append(chunk)
                    downloaded += len(chunk)
                    if progress is not None:
                        progress(downloaded, total)
            return b"".join(chunks)

        return retry_call(
            _stream_attempt,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
        )

    def get_text(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        encoding: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> str:
        """Fetch a URL and return response text.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.
            encoding: Optional encoding to override the response encoding.
            progress: Optional progress callback.

        Returns:
            str: The response text.
        """
        if progress is None:
            response = self.get(url, params=params, headers=headers)
            if encoding:
                response.encoding = encoding
            return response.text

        raw_bytes = self.get_bytes(
            url, params=params, headers=headers, progress=progress
        )
        return raw_bytes.decode(encoding or "utf-8")

    def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        progress: ProgressCallback | None = None,
    ) -> Any:
        """Fetch a URL and parse JSON.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.
            progress: Optional progress callback.

        Returns:
            Any: The parsed JSON data.

        Raises:
            FetchError: If the JSON is invalid.
        """
        if progress is None:
            response = self.get(url, params=params, headers=headers)
            try:
                return response.json()
            except ValueError as exc:
                raise FetchError(f"Invalid JSON while fetching {response.url}") from exc

        import json

        raw_bytes = self.get_bytes(
            url, params=params, headers=headers, progress=progress
        )
        try:
            return json.loads(raw_bytes)
        except ValueError as exc:
            raise FetchError(f"Invalid JSON while fetching {url}") from exc

    def download(
        self,
        url: str,
        target_path: str | Path,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Path:
        """Download a URL to a file using atomic write.

        Args:
            url: The URL to download.
            target_path: The local path to save the file.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            Path: The path to the downloaded file.
        """
        content = self.get_bytes(url, params=params, headers=headers)
        return write_bytes_atomic(target_path, content)

    def download_with_manifest(
        self,
        url: str,
        target_path: str | Path,
        *,
        source_id: str,
        dataset_id: str,
        producer: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        force: bool = False,
        check_size: bool = True,
        progress: ProgressCallback | None = None,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> Path:
        """Stream a URL to disk with freshness check, atomic write, and manifest.

        If the file exists and is up to date according to ``Last-Modified``
        (and optionally ``Content-Length``), it is not downloaded.

        **Ephemeral mode contract:** when the raw file is absent but its
        ``<target>.manifest.json`` sidecar survives, freshness is judged
        from the sidecar metadata instead of the file stat — a matching
        remote ``ETag``, or ``Content-Length`` + non-newer ``Last-Modified``,
        skips the download entirely and the file is *not* re-created
        (sidecar-only retention). If the sidecar exists but is corrupted
        (e.g. not a JSON object), the check fails safe: the file is simply
        (re)downloaded.

        The provenance sidecar written after a successful GET always records
        the headers captured from the GET stream itself (``ETag`` /
        ``Last-Modified``), so a failed freshness HEAD (e.g. HTTP 404/5xx
        before the GET) never aborts the download and never leaks a
        partially-bound ``head`` variable into the manifest.

        Interrupted streams are resumed with ``Range`` requests across
        attempts (when the server honors them); servers that ignore
        ``Range`` fall back to restart-from-zero.

        ``progress`` is invoked as ``(downloaded_bytes, total_bytes)`` after
        each chunk is written. ``total_bytes`` is ``0`` when the remote does
        not advertise ``Content-Length``.

        Args:
            url: The URL to download.
            target_path: The local path to save the file.
            source_id: The source identifier.
            dataset_id: The dataset identifier.
            producer: The producer name.
            params: Optional query parameters.
            headers: Optional headers.
            force: Whether to force download even if fresh.
            check_size: Whether to check file size for freshness.
            progress: Optional progress callback.
            chunk_size: The chunk size for streaming.

        Returns:
            Path: The path to the downloaded file.

        Raises:
            StorageError: If there is insufficient disk space or a stream error.
        """
        target = Path(target_path)
        with log_step(
            self.logger,
            "download-with-manifest",
            url=url,
            target=target.name,
            expected_exceptions=(HttpStatusError,),
        ):
            try:
                head = self.head_or_get(url, params=params, headers=headers)
            except FetchError as exc:
                self.logger.debug(
                    f"Could not check freshness for {target.name} ({exc}); "
                    "will (re)download"
                )
            else:
                if not force:
                    up_to_date, ephemeral = _head_fresh(
                        head, target, check_size=check_size
                    )
                    if up_to_date:
                        if ephemeral:
                            self.logger.debug(
                                f"File (ephemeral) is up to date via manifest: "
                                f"{target.name}"
                            )
                        else:
                            self.logger.debug(f"File is up to date: {target.name}")
                        return target

            outcome: dict[str, Any] = {}

            # Stream with resume: a partial temp file survives across
            # attempts and the next attempt sends ``Range`` so flaky
            # hosts don't force a restart from zero on every reset.
            fd, temp_path = _open_atomic_temp(target)
            os.close(fd)
            downloaded = 0
            digest = hashlib.sha256()
            last_error: BaseException | None = None
            succeeded = False
            try:
                for attempt in range(1, self.attempts + 1):
                    req_headers = dict(headers or {})
                    if downloaded:
                        req_headers["Range"] = f"bytes={downloaded}-"
                    if progress is not None:
                        progress(downloaded, downloaded)
                    try:
                        with self.stream(
                            "GET", url, params=params, headers=req_headers
                        ) as response:
                            if downloaded and response.status_code == 200:
                                # Server ignored Range — restart from zero.
                                self.logger.debug(
                                    f"Range ignored for {target.name}; restarting"
                                )
                                downloaded = 0
                                digest = hashlib.sha256()
                            total = downloaded + int(
                                response.headers.get("Content-Length", 0) or 0
                            )
                            if not check_free_space(
                                target, required_bytes=total - downloaded
                            ):
                                req_mb = (total - downloaded) / (1024 * 1024)
                                raise StorageError(
                                    f"Insufficient disk space to download "
                                    f"{target.name} to {target.parent} "
                                    f"(required: {req_mb:.1f} MB)"
                                )

                            outcome["etag"] = response.headers.get("ETag")
                            outcome["last_modified"] = response.headers.get(
                                "Last-Modified"
                            )
                            outcome["final_url"] = str(response.url)

                            mode = "ab" if downloaded else "wb"
                            if mode == "ab" and (
                                not temp_path.exists()
                                or temp_path.stat().st_size != downloaded
                            ):
                                # Partial file vanished or changed underneath
                                # us — restart cleanly instead of corrupting.
                                mode = "wb"
                                downloaded = 0
                                digest = hashlib.sha256()
                            try:
                                with open(temp_path, mode) as stream:
                                    for chunk in response.iter_bytes(
                                        chunk_size=chunk_size
                                    ):
                                        if not chunk:
                                            continue
                                        stream.write(chunk)
                                        digest.update(chunk)
                                        downloaded += len(chunk)
                                        if progress is not None:
                                            progress(downloaded, total)
                                    stream.flush()
                                    os.fsync(stream.fileno())
                            except OSError as exc:
                                raise StorageError(
                                    f"Could not stream download to {target}"
                                ) from exc
                    except HttpStatusError as exc:
                        if exc.status_code == 416 and downloaded:
                            # Offset beyond EOF (remote shrank?) — restart.
                            self.logger.debug(
                                f"Range unsatisfiable for {target.name}; restarting"
                            )
                            downloaded = 0
                            digest = hashlib.sha256()
                            continue
                        if exc.status_code not in RETRY_STATUS_CODES:
                            raise
                        last_error = exc
                    except DEFAULT_RETRY_EXCEPTIONS as exc:
                        last_error = exc
                    else:
                        succeeded = True
                        break
                    if attempt == self.attempts:
                        break
                    time.sleep(
                        exponential_delay(attempt, base_delay=self.retry_base_delay)
                    )
                if not succeeded:
                    raise RetryError(
                        f"Operation failed after {self.attempts} attempt(s)",
                        attempts=self.attempts,
                    ) from last_error
                outcome["sha256"] = digest.hexdigest()
                outcome["size_bytes"] = downloaded
                temp_path.replace(target)
            finally:
                if temp_path.exists():
                    with contextlib.suppress(OSError):
                        temp_path.unlink()

            _sync_mtime_from_last_modified(target, outcome.get("last_modified"))
            # ``head`` may be unbound when the freshness check failed with
            # a FetchError (e.g. HEAD 404/5xx) — always read headers captured
            # from the successful GET stream in ``outcome`` instead.
            _write_manifest(
                target,
                source_id=source_id,
                dataset_id=dataset_id,
                url=outcome.get("final_url", url),
                sha256=outcome["sha256"],
                size_bytes=outcome["size_bytes"],
                producer=producer,
                etag=outcome.get("etag"),
                last_modified=outcome.get("last_modified"),
            )
            return target

    @contextlib.contextmanager
    def stream(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        data: Any | None = None,
    ):
        """Open a streaming HTTP request.

        Args:
            method: The HTTP method to use.
            url: The URL to request.
            params: Optional query parameters.
            headers: Optional headers.
            data: Optional form data.

        Yields:
            httpx2.Response: The HTTP response stream.

        Raises:
            HttpStatusError: If a non-retryable error status is received.
        """
        self._rate_limiter.acquire()
        if self._client is not None:
            # Sessão persistente — reusa o pool.
            with self._client.stream(
                method, url, params=params, headers=headers, data=data
            ) as response:  # noqa: E501
                self.cookies.update(response.cookies)
                try:
                    response.raise_for_status()
                except httpx2.HTTPStatusError as exc:
                    url_str = str(response.url)
                    raise HttpStatusError(url_str, response.status_code) from exc
                yield response
        else:
            request_headers = dict(self.headers)
            if headers:
                request_headers.update(headers)
            with httpx2.Client(
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=request_headers,
                verify=self.verify,
                transport=self.transport,
                cookies=self.cookies,
                limits=self.limits,
            ) as client:
                with client.stream(method, url, params=params, data=data) as response:
                    self.cookies.update(response.cookies)
                    try:
                        response.raise_for_status()
                    except httpx2.HTTPStatusError as exc:
                        url_str = str(response.url)
                        raise HttpStatusError(url_str, response.status_code) from exc
                    yield response


class AsyncHttpClient:
    """Small asynchronous HTTP client wrapper around ``httpx2``."""

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
        attempts: int = 3,
        retry_base_delay: float = 1.0,
        verify: VerifyOption = True,
        transport: httpx2.AsyncBaseTransport | None = None,
        logger: logging.Logger | None = None,
        cookies: httpx2.Cookies | None = None,
        limits: httpx2.Limits | None = None,
        emulate_browser: bool = False,
    ) -> None:
        if emulate_browser:
            default_headers: dict[str, str] = dict(BROWSER_HEADERS)
            default_headers.setdefault("Accept-Encoding", "gzip, deflate")
            if headers:
                default_headers.update(headers)
            if "Accept-Encoding" in (headers or {}):
                ae = (headers or {}).get("Accept-Encoding", "")
                if "br" in ae or "zstd" in ae:
                    default_headers["Accept-Encoding"] = "gzip, deflate"
        else:
            default_headers = {"User-Agent": DEFAULT_USER_AGENT}
            if headers:
                default_headers.update(headers)
        self.timeout = timeout
        self.headers = default_headers
        self.follow_redirects = follow_redirects
        self.attempts = attempts
        self.retry_base_delay = retry_base_delay
        self.verify = verify
        self.transport = transport
        self.logger = logger or get_logger(__name__)
        self.cookies = cookies or httpx2.Cookies()
        self.limits = limits or DEFAULT_LIMITS
        self.emulate_browser = emulate_browser
        self._async_client: httpx2.AsyncClient | None = None

    def _build_async_client(self) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(
            timeout=self.timeout,
            follow_redirects=self.follow_redirects,
            headers=self.headers,
            verify=self.verify,
            transport=self.transport,
            cookies=self.cookies,
            limits=self.limits,
        )

    def _get_async_client(self) -> tuple[httpx2.AsyncClient, bool]:
        if self._async_client is not None:
            return self._async_client, True
        return self._build_async_client(), False

    async def __aenter__(self) -> AsyncHttpClient:
        if self._async_client is None:
            self._async_client = self._build_async_client()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._async_client is not None:
            try:
                await self._async_client.aclose()
            finally:
                self._async_client = None

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        data: Any | None = None,
        content: bytes | None = None,
        json: Any | None = None,
    ) -> httpx2.Response:
        """Perform an HTTP request with retries.

        Args:
            method: The HTTP method to use.
            url: The URL to request.
            params: Optional query parameters.
            headers: Optional headers.
            data: Optional form data.
            content: Optional byte content.
            json: Optional JSON data.

        Returns:
            httpx2.Response: The HTTP response.

        Raises:
            RetryableHttpStatusError: If a retryable status is received and
                attempts are exhausted.
            HttpStatusError: If a non-retryable error status is received.
        """

        async def do_request() -> httpx2.Response:
            start = time.perf_counter()
            client, is_persistent = self._get_async_client()
            if is_persistent:
                response = await client.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    data=data,
                    content=content,
                    json=json,  # noqa: E501
                )
                self.cookies.update(response.cookies)
            else:
                async with client as c:
                    response = await c.request(
                        method,
                        url,
                        params=params,
                        headers=headers,
                        data=data,
                        content=content,
                        json=json,  # noqa: E501
                    )
                    self.cookies.update(response.cookies)
            elapsed = time.perf_counter() - start
            self.logger.debug(
                bind_context(
                    "HTTP Request (Async)",
                    method=method,
                    url=str(response.url),
                    status=response.status_code,
                    elapsed=f"{elapsed:.3f}s",
                )
            )
            if response.status_code in RETRY_STATUS_CODES:
                raise RetryableHttpStatusError(str(response.url), response.status_code)
            try:
                response.raise_for_status()
            except httpx2.HTTPStatusError as exc:
                raise HttpStatusError(str(response.url), response.status_code) from exc
            return response

        return await async_retry_call(
            do_request,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
        )

    async def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx2.Response:
        """Fetch a URL and return the response.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            httpx2.Response: The HTTP response.
        """
        return await self.request("GET", url, params=params, headers=headers)

    async def head(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx2.Response:
        """Perform a HEAD request and return the response.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            httpx2.Response: The HTTP response.
        """
        return await self.request("HEAD", url, params=params, headers=headers)

    async def head_or_get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx2.Response:
        """Perform a HEAD request, falling back to GET with streaming if unsupported.

        Some servers don't support HEAD requests. This method tries HEAD first,
        and if it fails with 403, 405 or 501, opens a GET with streaming, reads only
        headers, and closes the connection without downloading the body.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            httpx2.Response: The HTTP response.
        """
        try:
            return await self.head(url, params=params, headers=headers)
        except HttpStatusError as e:
            if e.status_code not in (403, 405, 501):
                raise
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)

        async def _stream_fallback() -> httpx2.Response:
            async with httpx2.AsyncClient(
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=request_headers,
                verify=self.verify,
                transport=self.transport,
                cookies=self.cookies,
            ) as client:
                async with client.stream("GET", url, params=params) as response:
                    self.cookies.update(response.cookies)
                    try:
                        response.raise_for_status()
                    except httpx2.HTTPStatusError as exc:
                        raise HttpStatusError(
                            str(response.url), response.status_code
                        ) from exc
                    return response

        return await async_retry_call(
            _stream_fallback,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
        )

    async def head_metadata(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Async version of head_metadata.

        Returns ``{"size": int, "last_modified": datetime | None}``.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            dict[str, Any]: A dictionary containing 'size' and 'last_modified'.
        """
        resp = await self.head(url, params=params, headers=headers)
        size = int(resp.headers.get("Content-Length", 0))
        lm_str = resp.headers.get("Last-Modified")
        last_modified: dt.datetime | None = None
        if lm_str:
            try:
                last_modified = email.utils.parsedate_to_datetime(lm_str)
            except Exception:
                pass
        return {"size": size, "last_modified": last_modified}

    async def head_metadata_or_get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Async version of head_metadata with fallback.

        Returns ``{"size": int, "last_modified": datetime | None}``.
        Uses :meth:`head_or_get` internally.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            dict[str, Any]: A dictionary containing 'size' and 'last_modified'.
        """
        resp = await self.head_or_get(url, params=params, headers=headers)
        size = int(resp.headers.get("Content-Length", 0))
        lm_str = resp.headers.get("Last-Modified")
        last_modified: dt.datetime | None = None
        if lm_str:
            try:
                last_modified = email.utils.parsedate_to_datetime(lm_str)
            except Exception:
                pass
        return {"size": size, "last_modified": last_modified}

    async def head_last_modified_date(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dt.date | None:
        """Async version of :meth:`HttpClient.head_last_modified_date`.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            dt.date | None: The parsed date, or None if unavailable or on error.
        """
        try:
            meta = await self.head_metadata_or_get(url, params=params, headers=headers)
        except Exception as exc:
            self.logger.warning(f"Could not fetch metadata for {url}: {exc}")
            return None
        last_modified = meta.get("last_modified")
        return last_modified.date() if last_modified else None

    async def get_bytes(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        """Fetch a URL and return response bytes.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            bytes: The response content.
        """
        response = await self.get(url, params=params, headers=headers)
        return response.content

    async def get_text(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        encoding: str | None = None,
    ) -> str:
        """Fetch a URL and return response text.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.
            encoding: Optional encoding to override the response encoding.

        Returns:
            str: The response text.
        """
        response = await self.get(url, params=params, headers=headers)
        if encoding:
            response.encoding = encoding
        return response.text

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        """Fetch a URL and parse JSON.

        Args:
            url: The URL to fetch.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            Any: The parsed JSON data.

        Raises:
            FetchError: If the JSON is invalid.
        """
        response = await self.get(url, params=params, headers=headers)
        try:
            return response.json()
        except ValueError as exc:
            raise FetchError(f"Invalid JSON while fetching {response.url}") from exc

    async def download(
        self,
        url: str,
        target_path: str | Path,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Path:
        """Download a URL to a file using atomic write.

        Args:
            url: The URL to download.
            target_path: The local path to save the file.
            params: Optional query parameters.
            headers: Optional headers.

        Returns:
            Path: The path to the downloaded file.
        """
        content = await self.get_bytes(url, params=params, headers=headers)
        return write_bytes_atomic(target_path, content)

    async def download_with_manifest(
        self,
        url: str,
        target_path: str | Path,
        *,
        source_id: str,
        dataset_id: str,
        producer: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        force: bool = False,
        check_size: bool = True,
        progress: ProgressCallback | None = None,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> Path:
        """Stream a URL to disk asynchronously with freshness check and manifest.

        See :meth:`HttpClient.download_with_manifest` for parameter semantics.

        Args:
            url: The URL to download.
            target_path: The local path to save the file.
            source_id: The source identifier.
            dataset_id: The dataset identifier.
            producer: The producer name.
            params: Optional query parameters.
            headers: Optional headers.
            force: Whether to force download even if fresh.
            check_size: Whether to check file size for freshness.
            progress: Optional progress callback.
            chunk_size: The chunk size for streaming.

        Returns:
            Path: The path to the downloaded file.
        """
        target = Path(target_path)
        with log_step(
            self.logger,
            "download-with-manifest-async",
            url=url,
            target=target.name,
            expected_exceptions=(HttpStatusError,),
        ):
            try:
                head = await self.head_or_get(url, params=params, headers=headers)
            except FetchError as exc:
                self.logger.debug(
                    f"Could not check freshness for {target.name} ({exc}); "
                    "will (re)download"
                )
            else:
                if not force:
                    up_to_date, ephemeral = _head_fresh(
                        head, target, check_size=check_size
                    )
                    if up_to_date:
                        if ephemeral:
                            self.logger.debug(
                                f"File (ephemeral) is up to date via manifest: "
                                f"{target.name}"
                            )
                        else:
                            self.logger.debug(f"File is up to date: {target.name}")
                        return target

            outcome: dict[str, Any] = {}

            # Same resume semantics as the sync variant (see above).
            fd, temp_path = _open_atomic_temp(target)
            os.close(fd)
            downloaded = 0
            digest = hashlib.sha256()
            last_error: BaseException | None = None
            succeeded = False
            try:
                for attempt in range(1, self.attempts + 1):
                    req_headers = dict(headers or {})
                    if downloaded:
                        req_headers["Range"] = f"bytes={downloaded}-"
                    if progress is not None:
                        progress(downloaded, downloaded)
                    try:
                        async with self.stream(
                            "GET", url, params=params, headers=req_headers
                        ) as response:
                            if downloaded and response.status_code == 200:
                                self.logger.debug(
                                    f"Range ignored for {target.name}; restarting"
                                )
                                downloaded = 0
                                digest = hashlib.sha256()
                            total = downloaded + int(
                                response.headers.get("Content-Length", 0) or 0
                            )
                            outcome["etag"] = response.headers.get("ETag")
                            outcome["last_modified"] = response.headers.get(
                                "Last-Modified"
                            )
                            outcome["final_url"] = str(response.url)

                            mode = "ab" if downloaded else "wb"
                            if mode == "ab" and (
                                not temp_path.exists()
                                or temp_path.stat().st_size != downloaded
                            ):
                                mode = "wb"
                                downloaded = 0
                                digest = hashlib.sha256()
                            try:
                                with open(temp_path, mode) as stream:
                                    async for chunk in response.aiter_bytes(
                                        chunk_size=chunk_size
                                    ):
                                        if not chunk:
                                            continue
                                        stream.write(chunk)
                                        digest.update(chunk)
                                        downloaded += len(chunk)
                                        if progress is not None:
                                            progress(downloaded, total)
                                    stream.flush()
                                    os.fsync(stream.fileno())
                            except OSError as exc:
                                raise StorageError(
                                    f"Could not stream download to {target}"
                                ) from exc
                    except HttpStatusError as exc:
                        if exc.status_code == 416 and downloaded:
                            self.logger.debug(
                                f"Range unsatisfiable for {target.name}; restarting"
                            )
                            downloaded = 0
                            digest = hashlib.sha256()
                            continue
                        if exc.status_code not in RETRY_STATUS_CODES:
                            raise
                        last_error = exc
                    except DEFAULT_RETRY_EXCEPTIONS as exc:
                        last_error = exc
                    else:
                        succeeded = True
                        break
                    if attempt == self.attempts:
                        break
                    await asyncio.sleep(
                        exponential_delay(attempt, base_delay=self.retry_base_delay)
                    )
                if not succeeded:
                    raise RetryError(
                        f"Async operation failed after {self.attempts} attempt(s)",
                        attempts=self.attempts,
                    ) from last_error
                outcome["sha256"] = digest.hexdigest()
                outcome["size_bytes"] = downloaded
                temp_path.replace(target)
            finally:
                if temp_path.exists():
                    with contextlib.suppress(OSError):
                        temp_path.unlink()

            _sync_mtime_from_last_modified(target, outcome.get("last_modified"))
            # ``head`` may be unbound when the freshness check failed with
            # a FetchError (e.g. HEAD 404/5xx) — always read headers captured
            # from the successful GET stream in ``outcome`` instead.
            _write_manifest(
                target,
                source_id=source_id,
                dataset_id=dataset_id,
                url=outcome.get("final_url", url),
                sha256=outcome["sha256"],
                size_bytes=outcome["size_bytes"],
                producer=producer,
                etag=outcome.get("etag"),
                last_modified=outcome.get("last_modified"),
            )
            return target

    @contextlib.asynccontextmanager
    async def stream(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        data: Any | None = None,
    ) -> AsyncGenerator[httpx2.Response, None]:
        """Open a streaming HTTP request asynchronously.

        Args:
            method: The HTTP method to use.
            url: The URL to request.
            params: Optional query parameters.
            headers: Optional headers.
            data: Optional form data.

        Yields:
            httpx2.Response: The HTTP response stream.

        Raises:
            HttpStatusError: If a non-retryable error status is received.
        """
        if self._async_client is not None:
            async with self._async_client.stream(
                method, url, params=params, headers=headers, data=data
            ) as response:  # noqa: E501
                try:
                    response.raise_for_status()
                except httpx2.HTTPStatusError as exc:
                    url_str = str(response.url)
                    raise HttpStatusError(url_str, response.status_code) from exc
                yield response
        else:
            request_headers = dict(self.headers)
            if headers:
                request_headers.update(headers)
            async with httpx2.AsyncClient(
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=request_headers,
                verify=self.verify,
                transport=self.transport,
                cookies=self.cookies,
                limits=self.limits,
            ) as client:
                async with client.stream(method, url, params=params) as response:
                    try:
                        response.raise_for_status()
                    except httpx2.HTTPStatusError as exc:
                        url_str = str(response.url)
                        raise HttpStatusError(url_str, response.status_code) from exc
                    yield response


def _open_atomic_temp(target: Path) -> tuple[int, Path]:
    """Create a temp file alongside ``target`` for atomic write."""
    ensure_parent(target)
    fd, raw_temp_path = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=target.parent,
    )
    return fd, Path(raw_temp_path)


def _sync_mtime_from_last_modified(target: Path, header_value: str | None) -> None:
    """Set ``target``'s mtime from an HTTP ``Last-Modified`` header."""
    if not header_value:
        return
    try:
        dt = email.utils.parsedate_to_datetime(header_value)
        os.utime(target, (time.time(), dt.timestamp()))
    except (ValueError, TypeError, OSError):
        pass


def _write_manifest(
    target: Path,
    *,
    source_id: str,
    dataset_id: str,
    url: str,
    sha256: str,
    size_bytes: int,
    producer: str | None,
    etag: str | None = None,
    last_modified: str | None = None,
) -> None:
    manifest = DownloadManifest.from_digest(
        source_id=source_id,
        dataset_id=dataset_id,
        url=url,
        sha256=sha256,
        size_bytes=size_bytes,
        path=str(target.absolute()),
        producer=producer,
    )
    if etag or last_modified:
        manifest = dataclasses.replace(
            manifest,
            source_meta=SourceMetadata(etag=etag, last_modified=last_modified),
        )
    write_manifest_sidecar(target, manifest)


def _parse_http_datetime(value: str | None) -> dt.datetime | None:
    """Parse an RFC 1123/ISO-8601 date string into an aware datetime."""
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (ValueError, TypeError):
        parsed = None
    if parsed is None:
        try:
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed


def _is_up_to_date_via_manifest(
    response: httpx2.Response,
    manifest: DownloadManifest,
    *,
    check_size: bool = True,
) -> bool:
    """Check up-to-date status using a sidecar manifest instead of file stat.

    Used when the raw file was deleted (ephemeral retention) but its
    ``<file>.manifest.json`` sidecar survives. A match on either remote
    ``ETag`` or (size + Last-Modified) means the remote content is
    unchanged since the manifest was written.
    """
    source_meta = manifest.source_meta

    # (a) Strong signal: matching ETag means remote content is identical.
    remote_etag = response.headers.get("ETag")
    if (
        remote_etag
        and source_meta is not None
        and source_meta.etag
        and remote_etag == source_meta.etag
    ):
        return True

    # (b) Weaker signal: same advertised size and Last-Modified not newer
    # than what we recorded at fetch time.
    content_length = response.headers.get("Content-Length")
    remote_size: int | None = None
    if content_length:
        with contextlib.suppress(ValueError, TypeError):
            remote_size = int(content_length)
    remote_lm = _parse_http_datetime(response.headers.get("Last-Modified"))
    if remote_lm is None:
        return False
    local_lm: dt.datetime | None = None
    if source_meta is not None and source_meta.last_modified:
        local_lm = _parse_http_datetime(source_meta.last_modified)
    if local_lm is None and manifest.fetched_at:
        local_lm = _parse_http_datetime(manifest.fetched_at)
    if local_lm is None:
        return False
    if remote_lm > local_lm:
        return False
    if check_size:
        return remote_size is not None and remote_size == manifest.size_bytes
    return True


def _head_fresh(
    response: httpx2.Response,
    target: Path,
    *,
    check_size: bool = True,
) -> tuple[bool, bool]:
    """Return ``(up_to_date, ephemeral)`` for the download freshness check.

    ``ephemeral`` is ``True`` when the raw local file is missing but the
    manifest sidecar attests the remote content is still current — the
    download is then skipped without re-creating the file.
    """
    if target.exists():
        return (
            not _is_remote_more_recent(response, target, check_size=check_size),
            False,
        )
    sidecar = manifest_sidecar_path(target)
    if not sidecar.exists():
        return (False, False)
    try:
        manifest = DownloadManifest.read_json(sidecar)
    except (AttributeError, OSError, TypeError, ValueError):
        # Corrupted/unexpected sidecar payload -> fail-safe: redownload.
        return (False, False)
    fresh = _is_up_to_date_via_manifest(response, manifest, check_size=check_size)
    return (bool(fresh), bool(fresh))


def is_remote_more_recent(
    response: httpx2.Response,
    local_path: Path,
    *,
    check_size: bool = True,
) -> bool:
    """Check if the remote resource is more recent than the local file.

    Shared freshness predicate used by both ``download`` (sync) and
    ``check`` (verification-only) flows so they always agree.

    When the local file was deleted (ephemeral retention) but the
    ``<file>.manifest.json`` sidecar survives, freshness is judged against
    the manifest metadata instead of the missing file stat.
    """
    if not local_path.exists():
        sidecar = manifest_sidecar_path(local_path)
        if not sidecar.exists():
            return True
        try:
            manifest = DownloadManifest.read_json(sidecar)
        except (AttributeError, OSError, TypeError, ValueError):
            # Corrupted/unexpected sidecar payload -> fail-safe: refetch.
            return True
        if not _is_up_to_date_via_manifest(response, manifest, check_size=check_size):
            return True
        return False

    # 1. Check size if requested
    if check_size:
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                remote_size = int(content_length)
                if local_path.stat().st_size != remote_size:
                    return True
            except (ValueError, TypeError):
                pass

    # 2. Check Last-Modified
    last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        # If we can't check modification date and size was OK (or not checked),
        # we assume it's NOT more recent (up to date).
        return False

    try:
        dt = email.utils.parsedate_to_datetime(last_modified)
        remote_mtime = dt.timestamp()
        # 1s buffer for precision
        return local_path.stat().st_mtime < (remote_mtime - 1)
    except (ValueError, TypeError, OSError):
        return True


_is_remote_more_recent = is_remote_more_recent
"""Backward-compatible alias for the pre-1.0 private name."""
