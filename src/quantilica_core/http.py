"""HTTP helpers for data clients and ingestion jobs."""

from __future__ import annotations

import contextlib
import logging
import time
from collections.abc import AsyncGenerator, Mapping
from pathlib import Path
from typing import Any

import httpx

from .exceptions import FetchError
from .files import write_bytes_atomic
from .logging import bind_context, get_logger
from .retry import async_retry_call, retry_call

DEFAULT_USER_AGENT = "quantilica-core"
DEFAULT_TIMEOUT = 60.0
RETRY_STATUS_CODES = {408, 429, 500, 502, 503, 504}


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
            transport=self.transport,
        ) as client:
            with client.stream(method, url, params=params) as response:
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise HttpStatusError(str(response.url), response.status_code) from exc
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
            transport=self.transport,
        ) as client:
            async with client.stream(method, url, params=params) as response:
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise HttpStatusError(str(response.url), response.status_code) from exc
                yield response
