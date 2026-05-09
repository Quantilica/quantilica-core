import httpx
import pytest

from quantilica_core.exceptions import FetchError
from quantilica_core.http import HttpClient, HttpStatusError


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
