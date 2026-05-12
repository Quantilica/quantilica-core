"""HTTP helpers for data clients and ingestion jobs."""

from __future__ import annotations

import datetime as dt
import contextlib
import email.utils
import hashlib
import logging
import os
import tempfile
import time
from collections.abc import AsyncGenerator, Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from .exceptions import FetchError, StorageError
from .files import ensure_parent, write_bytes_atomic
from .logging import bind_context, get_logger, log_step
from .manifests import DownloadManifest
from .retry import async_retry_call, retry_call

ProgressCallback = Callable[[int, int], None]
"""Callback invoked as ``(downloaded_bytes, total_bytes)`` during a stream.

``total_bytes`` is ``0`` when the remote does not advertise ``Content-Length``.
"""

DEFAULT_STREAM_CHUNK_SIZE = 64 * 1024

DEFAULT_USER_AGENT = "quantilica-core"
DEFAULT_TIMEOUT = 60.0
RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}

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
        super().__init__(f"HTTP {status_code} while fetching {url}")
        self.url = url
        self.status_code = status_code


class RetryableHttpStatusError(HttpStatusError):
    """Raised for HTTP status codes that are safe to retry."""


DEFAULT_RETRY_EXCEPTIONS = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    RetryableHttpStatusError,
)


class HttpClient:
    """Small synchronous HTTP client wrapper around ``httpx``."""

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
        attempts: int = 3,
        retry_base_delay: float = 1.0,
        verify: bool = True,
        transport: httpx.BaseTransport | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
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

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
        json: Any | None = None,
    ) -> httpx.Response:
        """Perform an HTTP request with retries."""

        def do_request() -> httpx.Response:
            request_headers = dict(self.headers)
            if headers:
                request_headers.update(headers)
            start = time.perf_counter()
            with httpx.Client(
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=request_headers,
                verify=self.verify,
                transport=self.transport,
            ) as client:
                response = client.request(
                    method, url, params=params, content=content, json=json
                )
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
            except httpx.HTTPStatusError as exc:
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
    ) -> httpx.Response:
        """Fetch a URL and return the response."""
        return self.request("GET", url, params=params, headers=headers)

    def head(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Perform a HEAD request and return the response."""
        return self.request("HEAD", url, params=params, headers=headers)

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

    def get_bytes(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        """Fetch a URL and return response bytes."""
        return self.get(url, params=params, headers=headers).content

    def get_text(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        encoding: str | None = None,
    ) -> str:
        """Fetch a URL and return response text."""
        response = self.get(url, params=params, headers=headers)
        if encoding:
            response.encoding = encoding
        return response.text

    def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        """Fetch a URL and parse JSON."""
        response = self.get(url, params=params, headers=headers)
        try:
            return response.json()
        except ValueError as exc:
            raise FetchError(f"Invalid JSON while fetching {response.url}") from exc

    def download(
        self,
        url: str,
        target_path: str | Path,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Path:
        """Download a URL to a file using atomic write."""
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

        ``progress`` is invoked as ``(downloaded_bytes, total_bytes)`` after
        each chunk is written. ``total_bytes`` is ``0`` when the remote does
        not advertise ``Content-Length``.
        """
        target = Path(target_path)
        with log_step(
            self.logger, "download-with-manifest", url=url, target=target.name
        ):
            try:
                head = self.head(url, params=params, headers=headers)
                if not force and target.exists():
                    if not _is_remote_more_recent(head, target, check_size=check_size):
                        self.logger.debug(f"File is up to date: {target.name}")
                        return target
            except FetchError:
                if not force and target.exists():
                    raise

            outcome: dict[str, Any] = {}

            def _stream_attempt() -> None:
                if progress is not None:
                    progress(0, 0)
                downloaded = 0
                digest = hashlib.sha256()
                with self.stream(
                    "GET", url, params=params, headers=headers
                ) as response:
                    total = int(response.headers.get("Content-Length", 0) or 0)
                    outcome["last_modified"] = response.headers.get("Last-Modified")
                    outcome["final_url"] = str(response.url)

                    fd, temp_path = _open_atomic_temp(target)
                    try:
                        with os.fdopen(fd, "wb") as stream:
                            for chunk in response.iter_bytes(chunk_size=chunk_size):
                                if not chunk:
                                    continue
                                stream.write(chunk)
                                digest.update(chunk)
                                downloaded += len(chunk)
                                if progress is not None:
                                    progress(downloaded, total)
                            stream.flush()
                            os.fsync(stream.fileno())
                        temp_path.replace(target)
                    except OSError as exc:
                        raise StorageError(
                            f"Could not stream download to {target}"
                        ) from exc
                    finally:
                        if temp_path.exists():
                            with contextlib.suppress(OSError):
                                temp_path.unlink()
                outcome["sha256"] = digest.hexdigest()
                outcome["size_bytes"] = downloaded

            retry_call(
                _stream_attempt,
                attempts=self.attempts,
                base_delay=self.retry_base_delay,
                retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
            )

            _sync_mtime_from_last_modified(target, outcome["last_modified"])
            _write_manifest(
                target,
                source_id=source_id,
                dataset_id=dataset_id,
                url=outcome["final_url"],
                sha256=outcome["sha256"],
                size_bytes=outcome["size_bytes"],
                producer=producer,
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
    ):
        """Open a streaming HTTP request."""
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)
        with httpx.Client(
            timeout=self.timeout,
            follow_redirects=self.follow_redirects,
            headers=request_headers,
            verify=self.verify,
            transport=self.transport,
        ) as client:
            with client.stream(method, url, params=params) as response:
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    url_str = str(response.url)
                    raise HttpStatusError(url_str, response.status_code) from exc
                yield response


class AsyncHttpClient:
    """Small asynchronous HTTP client wrapper around ``httpx``."""

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
        attempts: int = 3,
        retry_base_delay: float = 1.0,
        verify: bool = True,
        transport: httpx.AsyncBaseTransport | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
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

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
        json: Any | None = None,
    ) -> httpx.Response:
        """Perform an HTTP request with retries."""

        async def do_request() -> httpx.Response:
            request_headers = dict(self.headers)
            if headers:
                request_headers.update(headers)
            start = time.perf_counter()
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=self.follow_redirects,
                headers=request_headers,
                verify=self.verify,
                transport=self.transport,
            ) as client:
                response = await client.request(
                    method, url, params=params, content=content, json=json
                )
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
            except httpx.HTTPStatusError as exc:
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
    ) -> httpx.Response:
        """Fetch a URL and return the response."""
        return await self.request("GET", url, params=params, headers=headers)

    async def head(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Perform a HEAD request and return the response."""
        return await self.request("HEAD", url, params=params, headers=headers)

    async def head_metadata(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Async version of head_metadata.

        Returns ``{"size": int, "last_modified": datetime | None}``.
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

    async def get_bytes(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        """Fetch a URL and return response bytes."""
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
        """Fetch a URL and return response text."""
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
        """Fetch a URL and parse JSON."""
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
        """Download a URL to a file using atomic write."""
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
        """
        target = Path(target_path)
        with log_step(
            self.logger, "download-with-manifest-async", url=url, target=target.name
        ):
            try:
                head = await self.head(url, params=params, headers=headers)
                if not force and target.exists():
                    if not _is_remote_more_recent(head, target, check_size=check_size):
                        self.logger.debug(f"File is up to date: {target.name}")
                        return target
            except FetchError:
                if not force and target.exists():
                    raise

            outcome: dict[str, Any] = {}

            async def _stream_attempt() -> None:
                if progress is not None:
                    progress(0, 0)
                downloaded = 0
                digest = hashlib.sha256()
                async with self.stream(
                    "GET", url, params=params, headers=headers
                ) as response:
                    total = int(response.headers.get("Content-Length", 0) or 0)
                    outcome["last_modified"] = response.headers.get("Last-Modified")
                    outcome["final_url"] = str(response.url)

                    fd, temp_path = _open_atomic_temp(target)
                    try:
                        with os.fdopen(fd, "wb") as stream:
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
                        temp_path.replace(target)
                    except OSError as exc:
                        raise StorageError(
                            f"Could not stream download to {target}"
                        ) from exc
                    finally:
                        if temp_path.exists():
                            with contextlib.suppress(OSError):
                                temp_path.unlink()
                outcome["sha256"] = digest.hexdigest()
                outcome["size_bytes"] = downloaded

            await async_retry_call(
                _stream_attempt,
                attempts=self.attempts,
                base_delay=self.retry_base_delay,
                retry_exceptions=DEFAULT_RETRY_EXCEPTIONS,
            )

            _sync_mtime_from_last_modified(target, outcome["last_modified"])
            _write_manifest(
                target,
                source_id=source_id,
                dataset_id=dataset_id,
                url=outcome["final_url"],
                sha256=outcome["sha256"],
                size_bytes=outcome["size_bytes"],
                producer=producer,
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
    ) -> AsyncGenerator[httpx.Response, None]:
        """Open a streaming HTTP request asynchronously."""
        request_headers = dict(self.headers)
        if headers:
            request_headers.update(headers)
        async with httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=self.follow_redirects,
            headers=request_headers,
            verify=self.verify,
            transport=self.transport,
        ) as client:
            async with client.stream(method, url, params=params) as response:
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
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
    manifest_path = target.with_suffix(target.suffix + ".manifest.json")
    manifest.write_json(manifest_path)


def _is_remote_more_recent(
    response: httpx.Response,
    local_path: Path,
    *,
    check_size: bool = True,
) -> bool:
    """Check if the remote resource is more recent than the local file."""
    if not local_path.exists():
        return True

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
