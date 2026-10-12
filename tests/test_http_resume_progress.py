import asyncio
import hashlib

import httpx2
import pytest

from quantilica.core.http import AsyncHttpClient, HttpClient
from quantilica.core.retry import RetryError

PIECE = 100  # matches chunk_size so buffering can't swallow data
PAYLOAD = bytes(range(250)) * 40  # 10000 bytes
LM = "Wed, 01 Jan 2025 00:00:00 GMT"


class _FlakyStream(httpx2.SyncByteStream):
    def __init__(self, data: bytes) -> None:
        self.data = data

    def __iter__(self):
        for i in range(0, len(self.data), PIECE):
            yield self.data[i : i + PIECE]
        raise httpx2.ReadError("Connection reset by peer")


class _AsyncFlakyStream(httpx2.AsyncByteStream):
    def __init__(self, data: bytes) -> None:
        self.data = data

    async def __aiter__(self):
        for i in range(0, len(self.data), PIECE):
            yield self.data[i : i + PIECE]
        raise httpx2.ReadError("Connection reset by peer")


def _handler(
    *,
    per_attempt: int,
    honor_range: bool = True,
    unsatisfiable: bool = False,
    is_async: bool = False,
):
    """Fake server that sends ``per_attempt`` bytes then resets."""
    calls: list[str | None] = []
    stream_cls = _AsyncFlakyStream if is_async else _FlakyStream

    def handler(request: httpx2.Request) -> httpx2.Response:
        headers = {"Content-Length": str(len(PAYLOAD)), "Last-Modified": LM}
        if request.method == "HEAD":
            return httpx2.Response(200, headers=headers)
        rng = request.headers.get("Range")
        calls.append(rng)
        start, status = 0, 200
        if rng and unsatisfiable:
            return httpx2.Response(416, headers={"Content-Range": "bytes */1"})
        if rng and honor_range:
            start = int(rng.removeprefix("bytes=").rstrip("-"))
            status = 206
        body = PAYLOAD[start:]
        headers["Content-Length"] = str(len(body))
        if status == 206:
            headers["Content-Range"] = (
                f"bytes {start}-{len(PAYLOAD) - 1}/{len(PAYLOAD)}"
            )
        if len(body) <= per_attempt:
            return httpx2.Response(status, headers=headers, content=body)
        return httpx2.Response(
            status, headers=headers, stream=stream_cls(body[:per_attempt])
        )

    handler.calls = calls  # type: ignore[attr-defined]
    return handler


def _client(handler, **kw) -> HttpClient:
    return HttpClient(
        attempts=kw.pop("attempts", 3),
        retry_base_delay=0.0,
        transport=httpx2.MockTransport(handler),
        **kw,
    )


def _dl(client, tmp_path):
    return client.download_with_manifest(
        "https://x.test/f.csv",
        tmp_path / "f.csv",
        source_id="s",
        dataset_id="d",
        producer="t",
        chunk_size=PIECE,
    )


def test_resume_with_progress_exceeds_attempts(tmp_path):
    handler = _handler(per_attempt=1000)  # needs 10 requests > attempts=3
    path = _dl(_client(handler), tmp_path)
    assert path.read_bytes() == PAYLOAD
    manifest = (tmp_path / "f.csv.manifest.json").read_text()
    assert hashlib.sha256(PAYLOAD).hexdigest() in manifest
    assert len(handler.calls) == 10


def test_no_progress_fails_after_attempts(tmp_path):
    handler = _handler(per_attempt=0)
    with pytest.raises(RetryError, match=r"failed after 3 attempt"):
        _dl(_client(handler), tmp_path)
    assert len(handler.calls) == 3


def test_range_ignored_does_not_loop(tmp_path):
    handler = _handler(per_attempt=1000, honor_range=False)
    with pytest.raises(RetryError, match=r"failed after 3 attempt"):
        _dl(_client(handler), tmp_path)
    # 1st attempt progresses (free); the restarts consume the budget.
    assert len(handler.calls) == 4


def test_max_resumes_reached(tmp_path):
    handler = _handler(per_attempt=100)
    with pytest.raises(RetryError, match=r"exceeded 5 resume"):
        _dl(_client(handler, max_resumes=5), tmp_path)
    assert len(handler.calls) == 6


def test_max_resumes_default_and_override():
    assert HttpClient().max_resumes == 1000
    assert AsyncHttpClient(max_resumes=7).max_resumes == 7


def test_async_resume_with_progress(tmp_path):
    handler = _handler(per_attempt=1000, is_async=True)
    client = AsyncHttpClient(
        attempts=3,
        retry_base_delay=0.0,
        transport=httpx2.MockTransport(handler),
    )

    async def run():
        return await client.download_with_manifest(
            "https://x.test/f.csv",
            tmp_path / "f.csv",
            source_id="s",
            dataset_id="d",
            producer="t",
            chunk_size=PIECE,
        )

    path = asyncio.run(run())
    assert path.read_bytes() == PAYLOAD


def test_async_max_resumes_and_no_progress(tmp_path):
    def mk(handler, **kw):
        return AsyncHttpClient(
            attempts=3,
            retry_base_delay=0.0,
            transport=httpx2.MockTransport(handler),
            **kw,
        )

    async def dl(client):
        return await client.download_with_manifest(
            "https://x.test/f.csv",
            tmp_path / "f.csv",
            source_id="s",
            dataset_id="d",
            producer="t",
            chunk_size=PIECE,
        )

    with pytest.raises(RetryError, match=r"exceeded 2 resume"):
        asyncio.run(dl(mk(_handler(per_attempt=100, is_async=True), max_resumes=2)))
    h = _handler(per_attempt=0, is_async=True)
    with pytest.raises(RetryError, match=r"failed after 3 attempt"):
        asyncio.run(dl(mk(h)))
    assert len(h.calls) == 3


def test_range_unsatisfiable_restarts_and_consumes_attempt(tmp_path):
    handler = _handler(per_attempt=1000, unsatisfiable=True)
    with pytest.raises(RetryError, match=r"failed after 3 attempt"):
        _dl(_client(handler), tmp_path)
    # Each 416 restarts from zero and consumes an attempt: bounded, no loop.
    assert len(handler.calls) == 6


def test_async_range_ignored_does_not_loop(tmp_path):
    handler = _handler(per_attempt=1000, honor_range=False, is_async=True)
    client = AsyncHttpClient(
        attempts=3,
        retry_base_delay=0.0,
        transport=httpx2.MockTransport(handler),
    )

    async def run():
        return await client.download_with_manifest(
            "https://x.test/f.csv",
            tmp_path / "f.csv",
            source_id="s",
            dataset_id="d",
            producer="t",
            chunk_size=PIECE,
        )

    with pytest.raises(RetryError, match=r"failed after 3 attempt"):
        asyncio.run(run())
    assert len(handler.calls) == 4
