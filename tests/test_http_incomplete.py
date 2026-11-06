"""Download sem Content-Length: tamanho esperado vem do Content-Range/ETag."""

import asyncio
import gzip

import httpx2
import pytest

from quantilica.core.exceptions import CorruptPayloadError
from quantilica.core.files import check_payload
from quantilica.core.http import (
    AsyncHttpClient,
    HttpClient,
    IncompleteDownloadError,
)

PIECE = 100
FULL = (b"a;b\n" + b"1;2\n" * 2499)[:10000]
FULL = FULL[: FULL.rfind(b"\n") + 1]  # termina em \n
TOTAL = len(FULL)
CUT = 4000


class _Chunks(httpx2.SyncByteStream, httpx2.AsyncByteStream):
    """Corpo chunked (sem Content-Length)."""

    def __init__(self, data: bytes) -> None:
        self.data = data

    def __iter__(self):
        for i in range(0, len(self.data), PIECE):
            yield self.data[i : i + PIECE]

    async def __aiter__(self):
        for i in range(0, len(self.data), PIECE):
            yield self.data[i : i + PIECE]


def _server(
    *, extra: dict | None = None, cut: int = CUT, honor_range: bool = False, full=FULL
):
    """Servidor que corta a 1a resposta em ``cut`` bytes, sem Content-Length."""
    calls: list[str | None] = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        if request.method == "HEAD":
            return httpx2.Response(200, headers={"Transfer-Encoding": "chunked"})
        rng = request.headers.get("Range")
        calls.append(rng)
        headers = {"Transfer-Encoding": "chunked", **(extra or {})}
        if rng and honor_range:
            start = int(rng.removeprefix("bytes=").rstrip("-"))
            body = full[start:]
            headers["Content-Range"] = f"bytes {start}-{len(full) - 1}/{len(full)}"
            return httpx2.Response(206, headers=headers, stream=_Chunks(body))
        return httpx2.Response(200, headers=headers, stream=_Chunks(full[:cut]))

    handler.calls = calls  # type: ignore[attr-defined]
    return handler


def _dl(handler, tmp_path, *, is_async=False, name="f.csv"):
    kw = dict(attempts=2, retry_base_delay=0.0, transport=httpx2.MockTransport(handler))
    args = ("https://x.test/" + name, tmp_path / name)
    opts = dict(source_id="s", dataset_id="d", producer="t", chunk_size=PIECE)
    if is_async:
        return asyncio.run(AsyncHttpClient(**kw).download_with_manifest(*args, **opts))
    return HttpClient(**kw).download_with_manifest(*args, **opts)


def _nothing_left(tmp_path):
    assert not (tmp_path / "f.csv").exists()
    assert not (tmp_path / "f.csv.manifest.json").exists()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("is_async", [False, True])
@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Range": f"bytes 0-{TOTAL - 1}/{TOTAL}"},
        {"ETag": f'"1793725707.93-{TOTAL}"'},
        {"ETag": f'W/"1793725707-{TOTAL}"'},
    ],
)
def test_truncado_levanta_erro(tmp_path, headers, is_async):
    handler = _server(extra=headers)
    with pytest.raises(IncompleteDownloadError):
        _dl(handler, tmp_path, is_async=is_async)
    assert len(handler.calls) >= 2  # tentou de novo
    _nothing_left(tmp_path)


@pytest.mark.parametrize("is_async", [False, True])
def test_retomada_que_completa(tmp_path, is_async):
    handler = _server(extra={"ETag": f'"1.5-{TOTAL}"'}, honor_range=True)
    # 1a resposta (sem Range) corta; a retomada via Range entrega o resto.
    path = _dl(handler, tmp_path, is_async=is_async)
    assert path.read_bytes() == FULL
    assert handler.calls[1] == f"bytes={CUT}-"


def test_etag_menor_que_baixado_nao_falha(tmp_path):
    handler = _server(extra={"ETag": '"1.5-100"'}, cut=TOTAL)
    assert _dl(handler, tmp_path).read_bytes() == FULL


def test_content_range_maior_que_baixado_e_erro_mesmo_se_baixado_maior(tmp_path):
    handler = _server(extra={"Content-Range": f"bytes 0-1/{TOTAL - 10}"}, cut=TOTAL)
    with pytest.raises(IncompleteDownloadError):
        _dl(handler, tmp_path)
    _nothing_left(tmp_path)


def test_etag_opaco_sem_verificacao(tmp_path):
    handler = _server(extra={"ETag": '"abc123-def"'})
    # truncado, mas sem como saber: termina em \n (cut multiplo de 4 linhas)
    path = _dl(handler, tmp_path)
    assert path.stat().st_size == CUT


def test_content_encoding_gzip_sem_verificacao(tmp_path):
    gz = gzip.compress(FULL)
    handler = _server(
        extra={"Content-Encoding": "gzip", "Content-Range": "bytes 0-1/12345"},
        full=gz,
        cut=len(gz),
    )
    # tamanho decodificado difere do Content-Range: regra nao se aplica
    assert _dl(handler, tmp_path).read_bytes() == FULL


def test_check_payload_exige_newline_final(tmp_path):
    p = tmp_path / "x.csv"
    p.write_bytes(b"a;b\n1;2")
    check_payload(p)  # padrao: sem regra
    with pytest.raises(CorruptPayloadError, match="quebra de linha"):
        check_payload(p, require_final_newline=True)
    p.write_bytes(b"a;b\r\n1;2\r\n")
    check_payload(p, require_final_newline=True)
    t = tmp_path / "x.tsv"
    t.write_bytes(b"a\tb\n1\t2")
    with pytest.raises(CorruptPayloadError):
        check_payload(t, require_final_newline=True)


def test_download_sem_tamanho_conhecido_exige_newline(tmp_path):
    handler = _server(cut=TOTAL - 1)  # sem headers de tamanho, corta o \n final
    with pytest.raises(CorruptPayloadError):
        _dl(handler, tmp_path)
    _nothing_left(tmp_path)
