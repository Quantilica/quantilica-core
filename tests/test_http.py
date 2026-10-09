import asyncio
import hashlib
import json
import threading
import time
from pathlib import Path
from tempfile import mkdtemp

import httpx2
import pytest

import quantilica.core.http as http_mod
from quantilica.core.exceptions import FetchError
from quantilica.core.http import (
    BROWSER_HEADERS,
    AsyncHttpClient,
    HttpClient,
    HttpStatusError,
    RateLimiter,
    is_remote_more_recent,
)


def test_http_client_get_json():
    def handler(request):
        assert request.headers["user-agent"] == "quantilica-core"
        return httpx2.Response(200, json={"ok": True})

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    assert client.get_json("https://example.test/data") == {"ok": True}


def test_http_client_get_text_with_encoding():
    def handler(request):
        return httpx2.Response(200, content="olá".encode("latin-1"))

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    assert client.get_text("https://example.test/data", encoding="latin-1") == "olá"


def test_http_client_download(tmp_path):
    def handler(request):
        return httpx2.Response(200, content=b"abc")

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    path = client.download("https://example.test/data", tmp_path / "data.bin")

    assert path.read_bytes() == b"abc"


def test_http_client_raises_status_error_for_404():
    def handler(request):
        return httpx2.Response(404)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    with pytest.raises(HttpStatusError) as exc_info:
        client.get_bytes("https://example.test/missing")

    assert exc_info.value.status_code == 404


def test_http_client_retries_retryable_status():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx2.Response(503)
        return httpx2.Response(200, content=b"ok")

    client = HttpClient(
        attempts=2,
        retry_base_delay=0,
        transport=httpx2.MockTransport(handler),
    )

    assert client.get_bytes("https://example.test/data") == b"ok"
    assert calls == 2


def test_http_client_invalid_json():
    def handler(request):
        return httpx2.Response(200, content=b"not json")

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    with pytest.raises(FetchError):
        client.get_json("https://example.test/data")


_DEFAULT_LAST_MODIFIED = "Wed, 21 Oct 2026 07:28:00 GMT"


def _download_handler_factory(
    payload: bytes,
    *,
    last_modified: str = _DEFAULT_LAST_MODIFIED,
):
    """Build a handler that answers HEAD with size + Last-Modified, GET with payload."""

    def handler(request: httpx2.Request) -> httpx2.Response:
        headers = {
            "Content-Length": str(len(payload)),
            "Last-Modified": last_modified,
        }
        if request.method == "HEAD":
            return httpx2.Response(200, headers=headers)
        return httpx2.Response(200, content=payload, headers=headers)

    return handler


def test_download_with_manifest_streams_and_writes_manifest(tmp_path):
    payload = b"x" * (200 * 1024)  # > 1 chunk at 64KB
    handler = _download_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

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
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

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
        progressed[i][0] <= progressed[i + 1][0] for i in range(len(progressed) - 1)
    )


def test_download_with_manifest_skips_when_up_to_date(tmp_path):
    payload = b"abc"
    target = tmp_path / "cached.bin"
    target.write_bytes(payload)

    calls: list[str] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request.method)
        headers = {"Content-Length": str(len(payload))}
        if request.method == "HEAD":
            return httpx2.Response(200, headers=headers)
        # GET should not be called when freshness matches
        return httpx2.Response(200, content=payload, headers=headers)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
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
    payload = b"fresh-content-updated"
    target = tmp_path / "existing.bin"
    target.write_bytes(b"stale-content")

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "HEAD":
            return httpx2.Response(403)
        return httpx2.Response(200, content=payload)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
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
    payload = b"fresh-content-updated"
    target = tmp_path / "existing.bin"
    target.write_bytes(b"stale-content")

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "HEAD":
            return httpx2.Response(403)
        return httpx2.Response(200, content=payload)

    client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))

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
    client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))

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
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    # _DEFAULT_LAST_MODIFIED == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert client.head_last_modified_date("https://example.test/data") == date(
        2026, 10, 21
    )


def test_head_last_modified_date_none_on_failure():
    def handler(request):
        return httpx2.Response(500)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    assert client.head_last_modified_date("https://example.test/data") is None


def test_head_last_modified_date_none_when_header_absent():
    def handler(request):
        return httpx2.Response(200)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    assert client.head_last_modified_date("https://example.test/data") is None


def test_async_head_last_modified_date_returns_date():
    from datetime import date

    handler = _download_handler_factory(b"abc")
    client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))

    async def run() -> object:
        return await client.head_last_modified_date("https://example.test/data")

    assert asyncio.run(run()) == date(2026, 10, 21)


def test_browser_headers_sent_when_configured():
    seen: dict[str, str] = {}

    def handler(request):
        seen["user-agent"] = request.headers["user-agent"]
        seen["accept-language"] = request.headers["accept-language"]
        return httpx2.Response(200, content=b"ok")

    client = HttpClient(
        attempts=1,
        headers=BROWSER_HEADERS,
        transport=httpx2.MockTransport(handler),
    )
    client.get_bytes("https://example.test/data")

    assert "Chrome" in seen["user-agent"]
    assert seen["accept-language"].startswith("pt-BR")


def test_http_client_emulate_browser_uses_browser_headers():
    seen: dict[str, str] = {}

    def handler(request):
        seen["user-agent"] = request.headers["user-agent"]
        seen["accept"] = request.headers["accept"]
        seen["accept-language"] = request.headers.get("accept-language", "")
        seen["accept-encoding"] = request.headers.get("accept-encoding", "")
        return httpx2.Response(200, content=b"ok")

    client = HttpClient(
        attempts=1, emulate_browser=True, transport=httpx2.MockTransport(handler)
    )
    client.get_bytes("https://example.test/data")

    assert "Chrome" in seen["user-agent"]
    assert seen["accept-language"].startswith("pt-BR")
    # Accept-Encoding seguro — nunca br/zstd
    assert "gzip" in seen["accept-encoding"]
    assert "br" not in seen["accept-encoding"]
    assert "zstd" not in seen["accept-encoding"]


def test_http_client_context_manager_reuses_pool():
    calls: list[str] = []

    def handler(request):
        calls.append(request.url.path)
        return httpx2.Response(200, content=b"ok")

    transport = httpx2.MockTransport(handler)
    client = HttpClient(attempts=1, transport=transport)

    # Fora do with — modo efêmero (backward compat)
    client.get_bytes("https://example.test/a")
    assert calls == ["/a"]
    assert client._client is None  # efêmero não mantém sessão

    # Dentro do with — pooling
    with client as c:
        assert c is client
        assert c._client is not None
        inner = c._client
        c.get_bytes("https://example.test/b")
        c.get_bytes("https://example.test/c")
        # Mesma instância de httpx2.Client reutilizada
        assert c._client is inner
    # Ao sair, pool fechado
    assert client._client is None
    assert calls == ["/a", "/b", "/c"]


def test_http_client_close_explicit():
    def handler(request):
        return httpx2.Response(200, content=b"ok")

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    client.__enter__()
    assert client._client is not None
    client.close()
    assert client._client is None
    # Após close, modo efêmero ainda funciona
    client.get_bytes("https://example.test/data")


def test_http_client_limits_configurable():
    limits = httpx2.Limits(max_connections=10, max_keepalive_connections=5)
    client = HttpClient(attempts=1, limits=limits)
    assert client.limits is limits
    # Default quando não informado
    c2 = HttpClient(attempts=1)
    assert c2.limits.max_connections == 50


def test_async_http_client_context_manager():
    async def run():
        calls: list[str] = []

        def handler(request):
            calls.append(request.url.path)
            return httpx2.Response(200, content=b"ok")

        client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))
        # Fora do contexto — efêmero
        await client.get_bytes("https://example.test/a")
        assert client._async_client is None

        async with client as c:
            assert c._async_client is not None
            inner = c._async_client
            await c.get_bytes("https://example.test/b")
            await c.get_bytes("https://example.test/c")
            assert c._async_client is inner
        assert client._async_client is None
        assert calls == ["/a", "/b", "/c"]

    asyncio.run(run())


def test_async_http_client_aclose():
    async def run():
        def handler(request):
            return httpx2.Response(200, content=b"ok")

        client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))
        await client.__aenter__()
        assert client._async_client is not None
        await client.aclose()
        assert client._async_client is None

    asyncio.run(run())


def test_http_client_min_interval_defaults_to_zero():
    client = HttpClient(attempts=1)
    assert client.min_interval == 0.0


def test_rate_limiter_disabled_for_zero_interval(monkeypatch):
    def _fail_sleep(seconds):
        raise AssertionError(f"sleep({seconds}) should not be called")

    monkeypatch.setattr(http_mod.time, "sleep", _fail_sleep)
    limiter = RateLimiter(0.0)
    limiter.acquire()
    limiter.acquire()


def test_rate_limiter_schedules_spaced_slots(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(http_mod.time, "sleep", lambda seconds: sleeps.append(seconds))

    limiter = RateLimiter(0.1)
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()
    limiter.acquire()

    sorted_sleeps = sorted(sleeps)
    assert sorted_sleeps == pytest.approx([0.1, 0.2, 0.3], abs=0.02)


def test_rate_limiter_thread_safe_spacing(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(http_mod.time, "sleep", lambda seconds: sleeps.append(seconds))

    limiter = RateLimiter(0.1)
    threads = [threading.Thread(target=limiter.acquire) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    sorted_sleeps = sorted(sleeps)
    assert sorted_sleeps == pytest.approx([0.1, 0.2, 0.3], abs=0.02)


def test_rate_limiter_private_alias_kept_for_compat():
    assert http_mod._RateLimiter is RateLimiter


def test_rate_limiter_exported_from_package_root():
    import quantilica.core

    assert quantilica.core.RateLimiter is http_mod.RateLimiter
    assert "RateLimiter" in quantilica.core.__all__


def test_http_client_rate_limits_sequential_requests():
    count = 0

    def handler(request):
        nonlocal count
        count += 1
        return httpx2.Response(200, content=b"ok")

    client = HttpClient(
        attempts=1,
        min_interval=0.05,
        transport=httpx2.MockTransport(handler),
    )
    started = time.monotonic()
    client.get("https://example.test/a")
    client.get("https://example.test/b")
    client.get("https://example.test/c")
    elapsed = time.monotonic() - started

    assert count == 3
    # Três requisições => dois intervalos mínimos entre slots.
    assert elapsed >= 0.095


def test_http_client_rate_limits_stream_requests():
    payload = b"x" * (100 * 1024)
    handler = _download_handler_factory(payload)
    client = HttpClient(
        attempts=1,
        min_interval=0.05,
        transport=httpx2.MockTransport(handler),
    )
    target = Path(mkdtemp()) / "data.bin"
    started = time.monotonic()
    out = client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    elapsed = time.monotonic() - started

    assert out == target
    assert out.read_bytes() == payload
    # HEAD (freshness) + GET (stream) => pelo menos um intervalo.
    assert elapsed >= 0.049


def test_resolve_verify_from_env_defaults_true(monkeypatch):
    monkeypatch.delenv("QUANTILICA_CA_BUNDLE", raising=False)
    monkeypatch.delenv("QUANTILICA_SSL_VERIFY", raising=False)
    assert http_mod.resolve_verify_from_env() is True


def test_resolve_verify_from_env_disable(monkeypatch):
    monkeypatch.delenv("QUANTILICA_CA_BUNDLE", raising=False)
    monkeypatch.setenv("QUANTILICA_SSL_VERIFY", "0")
    assert http_mod.resolve_verify_from_env() is False


def test_resolve_verify_from_env_ca_bundle(tmp_path, monkeypatch):
    bundle = tmp_path / "ca.pem"
    bundle.write_text("dummy")
    monkeypatch.setenv("QUANTILICA_CA_BUNDLE", str(bundle))
    assert http_mod.resolve_verify_from_env() == str(bundle)


def test_http_client_accepts_ca_bundle_path():
    client = HttpClient(attempts=1, verify="/tmp/ca.pem")
    assert client.verify == "/tmp/ca.pem"


def test_download_with_manifest_resumes_after_midstream_failure(tmp_path):
    # Pieces match chunk_size exactly so buffering can't swallow them:
    # attempt 1 persists 4 x 64 B, then the stream dies mid-body.
    piece, total_pieces, fail_after = 64, 8, 4
    payload = b"x" * (piece * total_pieces)
    calls = []

    def failing_content():
        for _ in range(fail_after):
            yield b"x" * piece
        raise httpx2.ReadError("connection reset")

    def handler(request):
        calls.append((request.method, request.headers.get("range")))
        if request.method == "HEAD":
            return httpx2.Response(200, headers={"Content-Length": str(len(payload))})
        if request.headers.get("range") == f"bytes={piece * fail_after}-":
            rest = payload[piece * fail_after :]
            return httpx2.Response(
                206,
                headers={
                    "Content-Length": str(len(rest)),
                    "Content-Range": (
                        f"bytes {piece * fail_after}-{len(payload) - 1}/{len(payload)}"
                    ),
                },
                content=rest,
            )
        return httpx2.Response(
            200,
            headers={"Content-Length": str(len(payload))},
            content=failing_content(),
        )

    client = HttpClient(
        attempts=2,
        retry_base_delay=0.01,
        transport=httpx2.MockTransport(handler),
    )
    out = client.download_with_manifest(
        "https://example.test/data.bin",
        tmp_path / "data.bin",
        source_id="src",
        dataset_id="ds",
        producer="test",
        chunk_size=piece,
    )

    assert out.read_bytes() == payload
    get_ranges = [r for m, r in calls if m == "GET"]
    assert get_ranges[0] is None
    assert get_ranges[1] == f"bytes={piece * fail_after}-"
    manifest = json.loads((tmp_path / "data.bin.manifest.json").read_text())
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["size_bytes"] == len(payload)


def test_download_with_manifest_restarts_when_server_ignores_range(tmp_path):
    piece, fail_after = 64, 1
    payload = b"z" * (piece * 4)
    seen_ranges = []

    def failing_content():
        yield payload[:piece]
        raise httpx2.ReadError("connection reset")

    def handler(request):
        if request.method == "HEAD":
            return httpx2.Response(200, headers={"Content-Length": str(len(payload))})
        seen_ranges.append(request.headers.get("range"))
        # Server ignores Range: always 200 full body.
        if len(seen_ranges) == 1:
            return httpx2.Response(
                200,
                headers={"Content-Length": str(len(payload))},
                content=failing_content(),
            )
        return httpx2.Response(
            200,
            headers={"Content-Length": str(len(payload))},
            content=payload,
        )

    client = HttpClient(
        attempts=2,
        retry_base_delay=0.01,
        transport=httpx2.MockTransport(handler),
    )
    out = client.download_with_manifest(
        "https://example.test/data.bin",
        tmp_path / "data.bin",
        source_id="src",
        dataset_id="ds",
        producer="test",
        chunk_size=piece,
    )

    assert out.read_bytes() == payload
    assert seen_ranges == [None, f"bytes={piece * fail_after}-"]


def _ephemeral_handler_factory(
    payload: bytes,
    *,
    etag: str = '"v1"',
    last_modified: str = "Mon, 01 Jan 2024 00:00:00 GMT",
):
    """HEAD/GET handler recording calls, with ETag and Last-Modified."""
    calls: list[str] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        calls.append(request.method)
        headers = {
            "Content-Length": str(len(payload)),
            "Last-Modified": last_modified,
            "ETag": etag,
        }
        if request.method == "HEAD":
            return httpx2.Response(200, headers=headers)
        return httpx2.Response(200, content=payload, headers=headers)

    return handler, calls


def test_download_with_manifest_records_source_meta(tmp_path):
    payload = b"abc"
    handler, _ = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "meta.bin"
    client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    manifest = json.loads(target.with_suffix(".bin.manifest.json").read_text())
    assert manifest["source_meta"]["etag"] == '"v1"'
    assert manifest["source_meta"]["last_modified"] == "Mon, 01 Jan 2024 00:00:00 GMT"


def test_download_with_manifest_ephemeral_skip_via_manifest(tmp_path):
    """File deleted (ephemeral) but sidecar survives -> skip via manifest."""
    payload = b"abc"
    handler, calls = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "eph.bin"
    client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    sidecar = target.with_suffix(".bin.manifest.json")
    assert sidecar.exists()
    target.unlink()

    out = client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    assert out == target
    assert not target.exists()
    # Only the first download performed the streaming GET.
    assert calls.count("GET") == 1


def test_download_with_manifest_ephemeral_redownloads_when_stale(tmp_path):
    """Sidecar with stale metadata must not block a real re-download."""
    payload = b"abc"
    handler, calls = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "stale.bin"
    client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    target.unlink()

    # Simulate a changed remote: different ETag and newer Last-Modified.
    handler2, calls2 = _ephemeral_handler_factory(
        payload, etag='"v2"', last_modified="Tue, 02 Jan 2024 00:00:00 GMT"
    )
    client2 = HttpClient(attempts=1, transport=httpx2.MockTransport(handler2))
    out = client2.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    assert out == target
    assert target.exists()
    assert calls2.count("GET") == 1


@pytest.mark.anyio
async def test_async_download_with_manifest_ephemeral_skip_via_manifest(tmp_path):
    payload = b"abc"
    handler, calls = _ephemeral_handler_factory(payload)
    client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "ephy.bin"
    await client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    target.unlink()
    out = await client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    assert out == target
    assert not target.exists()
    assert calls.count("GET") == 1


def test_is_remote_more_recent_manifest_fresh_without_file(tmp_path):
    payload = b"abc"
    handler, _ = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "fresh.bin"
    client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    target.unlink()
    head = client.head("https://example.test/data")
    assert is_remote_more_recent(head, target) is False


def test_is_remote_more_recent_manifest_stale_without_file(tmp_path):
    payload = b"abc"
    handler, _ = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "rotten.bin"
    client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )
    target.unlink()
    head = client.head("https://example.test/data")
    # Interrupted sidecar (bogus manifest) -> must treat as not fresh.
    target.with_suffix(".bin.manifest.json").write_text("{}")
    assert is_remote_more_recent(head, target) is True


def _head_404_get_200_handler(payload: bytes, *, record: list[str]):
    """Handler that answers HEAD with 404 and GET with 200 + source metadata."""

    def handler(request: httpx2.Request) -> httpx2.Response:
        record.append(request.method)
        headers = {
            "Content-Length": str(len(payload)),
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            "ETag": '"v1"',
        }
        if request.method == "HEAD":
            return httpx2.Response(404)
        return httpx2.Response(200, content=payload, headers=headers)

    return handler


def test_download_with_manifest_head_404_then_get_200_writes_manifest(tmp_path):
    """Regression: HEAD 404 (FetchError) must not leak an unbound ``head``
    variable into the manifest write — headers come from the GET outcome."""
    payload = b"head-404-payload"
    calls: list[str] = []
    handler = _head_404_get_200_handler(payload, record=calls)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
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
    assert calls == ["HEAD", "GET"]
    manifest = json.loads(target.with_suffix(".bin.manifest.json").read_text())
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()
    # Source metadata is captured from the GET stream, not the failed HEAD.
    assert manifest["source_meta"]["etag"] == '"v1"'
    assert manifest["source_meta"]["last_modified"] == "Mon, 01 Jan 2024 00:00:00 GMT"


@pytest.mark.anyio
async def test_async_download_with_manifest_head_404_then_get_200_writes_manifest(
    tmp_path,
):
    """Async regression: HEAD 404 then GET 200 still records the sidecar."""
    payload = b"async-head-404-payload"
    calls: list[str] = []
    handler = _head_404_get_200_handler(payload, record=calls)
    client = AsyncHttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "data.bin"

    out = await client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )

    assert out == target
    assert target.read_bytes() == payload
    assert calls == ["HEAD", "GET"]
    manifest = json.loads(target.with_suffix(".bin.manifest.json").read_text())
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["source_meta"]["etag"] == '"v1"'


@pytest.mark.parametrize("bad_payload", ["[]", '"just-a-string"', "3", "null"])
def test_download_with_manifest_corrupt_sidecar_failsafe_downloads(
    tmp_path, bad_payload
):
    """Corrupt sidecar (non-object JSON) must fail safe: (re)download the file
    instead of crashing (e.g. AttributeError/TypeError on the payload)."""
    payload = b"abc"
    handler, calls = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "corrupt.bin"
    sidecar = target.with_suffix(".bin.manifest.json")
    sidecar.write_text(bad_payload)

    out = client.download_with_manifest(
        "https://example.test/data",
        target,
        source_id="src",
        dataset_id="ds",
        producer="test",
    )

    assert out == target
    assert target.read_bytes() == payload
    # Freshness check failed safely -> streaming GET was performed once.
    assert calls.count("GET") == 1
    # The rewritten sidecar is a valid manifest again.
    manifest = json.loads(sidecar.read_text())
    assert manifest["sha256"] == hashlib.sha256(payload).hexdigest()


@pytest.mark.parametrize("bad_payload", ["[]", '"just-a-string"'])
def test_is_remote_more_recent_corrupt_sidecar_failsafe(tmp_path, bad_payload):
    """Corrupt sidecar with no local file must be treated as stale (refetch)."""
    payload = b"abc"
    handler, _ = _ephemeral_handler_factory(payload)
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    target = tmp_path / "no-file.bin"
    target.with_suffix(".bin.manifest.json").write_text(bad_payload)

    head = client.head("https://example.test/data")
    assert is_remote_more_recent(head, target) is True
