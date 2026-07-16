import asyncio
import hashlib
import json
from pathlib import Path

import httpx
import pytest
from quantilica.core.exceptions import FetchError
from quantilica.core.http import (
    BROWSER_HEADERS,
    AsyncHttpClient,
    HttpClient,
    HttpStatusError,
)


def test_http_client_get_json():
    def handler(request):
        assert request.headers["user-agent"] == "quantilica-core"
        return httpx.Response(200, json={"ok": True})

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    assert client.get_json("https://example.test/data") == {"ok": True}


def test_http_client_get_text_with_encoding():
    def handler(request):
        return httpx.Response(200, content="olá".encode("latin-1"))

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    assert client.get_text("https://example.test/data", encoding="latin-1") == "olá"


def test_http_client_download(tmp_path):
    def handler(request):
        return httpx.Response(200, content=b"abc")

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))
    path = client.download("https://example.test/data", tmp_path / "data.bin")

    assert path.read_bytes() == b"abc"


def test_http_client_raises_status_error_for_404():
    def handler(request):
        return httpx.Response(404)

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    with pytest.raises(HttpStatusError) as exc_info:
        client.get_bytes("https://example.test/missing")

    assert exc_info.value.status_code == 404


def test_http_client_retries_retryable_status():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=b"ok")

    client = HttpClient(
        attempts=2,
        retry_base_delay=0,
        transport=httpx.MockTransport(handler),
    )

    assert client.get_bytes("https://example.test/data") == b"ok"
    assert calls == 2


def test_http_client_invalid_json():
    def handler(request):
        return httpx.Response(200, content=b"not json")

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    with pytest.raises(FetchError):
        client.get_json("https://example.test/data")


_DEFAULT_LAST_MODIFIED = "Wed, 21 Oct 2026 07:28:00 GMT"


def _download_handler_factory(
    payload: bytes,
    *,
    last_modified: str = _DEFAULT_LAST_MODIFIED,
):
    """Build a handler that answers HEAD with size + Last-Modified, GET with payload."""

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {
            "Content-Length": str(len(payload)),
            "Last-Modified": last_modified,
        }
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        return httpx.Response(200, content=payload, headers=headers)

    return handler


def test_download_with_manifest_streams_and_writes_manifest(tmp_path):
    payload = b"x" * (200 * 1024)  # > 1 chunk at 64KB
    handler = _download_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    target = tmp_path / "data.bin"
    out = client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )

    assert out == target
    assert target.read_bytes() == payload

    manifest_path = target.with_suffix(".bin.manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["size_bytes"] == len(payload)
    assert manifest["source_id"] == "src"


def test_download_with_manifest_invokes_progress(tmp_path):
    payload = b"y" * (150 * 1024)
    handler = _download_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    seen: list[tuple[int, int]] = []
    client.download_with_manifest(
        "https://example.test/data",
        tmp_path / "out.bin",
        source_id="src",
        dataset_id="ds",
        producer="test",
        progress=lambda done, total: seen.append((done, total)),
        chunk_size=64 * 1024,
    )

    # Drop the (0, 0) retry-reset signal emitted at the start of each attempt.
    progressed = [(done, total) for done, total in seen if total]
    assert progressed, "progress callback should report real progress"
    assert all(total == len(payload) for _, total in progressed)
    assert progressed[-1][0] == len(payload)
    # Monotonically increasing downloaded counter
    assert all(
        progressed[i][0] <= progressed[i + 1][0]
        for i in range(len(progressed) - 1)
    )


def test_download_with_manifest_skips_when_up_to_date(tmp_path):
    payload = b"abc"
    target = tmp_path / "cached.bin"
    target.write_bytes(payload)

    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        headers = {"Content-Length": str(len(payload))}
        if request.method == "HEAD":
            return httpx.Response(200, headers=headers)
        # GET should not be called when freshness matches
        return httpx.Response(200, content=payload, headers=headers)

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))
    client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )

    assert calls == ["HEAD"]
    assert not target.with_suffix(".bin.manifest.json").exists()


def test_download_with_manifest_redownloads_when_head_fails_and_file_exists(
    tmp_path,
):
    """Regression: some servers (e.g. ANP's gov.br) always 403 on HEAD, even
    though GET works fine. A failed freshness check must not abort the
    download just because the target already exists on disk from a previous
    run — it should fall through and redownload via GET."""
    payload = b"fresh-content"
    target = tmp_path / "existing.bin"
    target.write_bytes(b"stale-content")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(403)
        return httpx.Response(200, content=payload)

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))
    out = client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )

    assert out == target
    assert target.read_bytes() == payload


def test_async_download_with_manifest_redownloads_when_head_fails_and_file_exists(
    tmp_path,
):
    payload = b"fresh-content"
    target = tmp_path / "existing.bin"
    target.write_bytes(b"stale-content")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(403)
        return httpx.Response(200, content=payload)

    client = AsyncHttpClient(attempts=1, transport=httpx.MockTransport(handler))

    async def run() -> Path:
        return await client.download_with_manifest(
            "https://example.test/data",
            target,
            source_id="src",
            dataset_id="ds",
            producer="test",
        )

    out = asyncio.run(run())

    assert out == target
    assert target.read_bytes() == payload


def test_async_download_with_manifest_streams_and_reports_progress(tmp_path):
    payload = b"z" * (100 * 1024)
    handler = _download_handler_factory(payload)
    client = AsyncHttpClient(attempts=1, transport=httpx.MockTransport(handler))

    seen: list[tuple[int, int]] = []
    target = tmp_path / "async.bin"

    async def run() -> None:
        await client.download_with_manifest(
            "https://example.test/data",
            target,
            source_id="src",
            dataset_id="ds",
            producer="test",
            progress=lambda d, t: seen.append((d, t)),
        )

    asyncio.run(run())

    assert target.read_bytes() == payload
    assert seen[-1][0] == len(payload)
    manifest = json.loads(
        target.with_suffix(".bin.manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()


def test_head_last_modified_date_returns_date():
    from datetime import date

    handler = _download_handler_factory(b"abc")
    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    # _DEFAULT_LAST_MODIFIED == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert client.head_last_modified_date(
        "https://example.test/data"
    ) == date(2026, 10, 21)


def test_head_last_modified_date_none_on_failure():
    def handler(request):
        return httpx.Response(500)

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    assert client.head_last_modified_date("https://example.test/data") is None


def test_head_last_modified_date_none_when_header_absent():
    def handler(request):
        return httpx.Response(200)

    client = HttpClient(attempts=1, transport=httpx.MockTransport(handler))

    assert client.head_last_modified_date("https://example.test/data") is None


def test_async_head_last_modified_date_returns_date():
    from datetime import date

    handler = _download_handler_factory(b"abc")
    client = AsyncHttpClient(attempts=1, transport=httpx.MockTransport(handler))

    async def run() -> object:
        return await client.head_last_modified_date("https://example.test/data")

    assert asyncio.run(run()) == date(2026, 10, 21)


def test_browser_headers_sent_when_configured():
    seen: dict[str, str] = {}

    def handler(request):
        seen["user-agent"] = request.headers["user-agent"]
        seen["accept-language"] = request.headers["accept-language"]
        return httpx.Response(200, content=b"ok")

    client = HttpClient(
        attempts=1,
        headers=BROWSER_HEADERS,
        transport=httpx.MockTransport(handler),
    )
    client.get_bytes("https://example.test/data")

    assert "Chrome" in seen["user-agent"]
    assert seen["accept-language"].startswith("pt-BR")
