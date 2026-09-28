"""Testes para quantilica.core.ftp (contrato de progresso e QUIT resiliente).

Contexto: sync DATASUS 2026-09-27 — todo download FTP com progresso morria no
primeiro chunk (`update_cb()` exige `(downloaded, total)`, mas o FTP chamava
`progress(n)`), e 550 no `QUIT` pós-RETR invalidava downloads íntegros.
"""

from __future__ import annotations

import ftplib
from pathlib import Path

import pytest

from quantilica.core.ftp import FtpClient


class FakeFTP:
    """ftplib.FTP fake: SIZE/RETR/MDTM configuráveis, QUIT pode falhar."""

    def __init__(
        self,
        chunks: list[bytes] | None = None,
        size: int | None = 10,
        quit_error: Exception | None = None,
        retr_error: Exception | None = None,
    ) -> None:
        self._chunks = chunks if chunks is not None else [b"hello", b"world"]
        self._size = size
        self._quit_error = quit_error
        self._retr_error = retr_error
        self.quit_called = False
        self.close_called = False

    def size(self, url: str):
        if isinstance(self._size, Exception):
            raise self._size
        return self._size

    def retrbinary(self, cmd: str, cb, blocksize: int = 8192, rest=None) -> str:
        if self._retr_error is not None:
            raise self._retr_error
        for chunk in self._chunks:
            cb(chunk)
        return "226 Transfer complete"

    def sendcmd(self, cmd: str) -> str:
        return "213 20240101120000"

    def quit(self) -> str:
        self.quit_called = True
        if self._quit_error is not None:
            raise self._quit_error
        return "221 Bye"

    def close(self) -> None:
        self.close_called = True


def _client(fake: FakeFTP, **kwargs) -> FtpClient:
    client = FtpClient("ftp.example.com", attempts=1, **kwargs)
    client._open = lambda: fake  # type: ignore[method-assign]
    return client


def _download(client: FtpClient, tmp_path: Path, progress=None) -> Path:
    return client.download_with_manifest(
        "pub/x.dbc",
        tmp_path / "x.dbc",
        source_id="datasus",
        dataset_id="sih-rd",
        producer="test",
        force=True,
        progress=progress,
    )


def test_progress_contract_two_args(tmp_path: Path):
    """Contrato canônico: progress(downloaded, total) — nunca progress(n)."""
    calls: list[tuple[int, int]] = []
    target = _download(
        _client(FakeFTP(chunks=[b"12345", b"67890"], size=10)),
        tmp_path,
        progress=lambda d, t: calls.append((d, t)),
    )
    assert target.read_bytes() == b"1234567890"
    assert calls == [(5, 10), (10, 10)]


def test_progress_total_zero_when_size_unsupported(tmp_path: Path):
    """SIZE ausente: total=0 (degradado, mas funcional)."""
    calls: list[tuple[int, int]] = []
    _download(
        _client(FakeFTP(size=ftplib.error_perm("500 SIZE not supported"))),
        tmp_path,
        progress=lambda d, t: calls.append((d, t)),
    )
    assert calls == [(5, 0), (10, 0)]


def test_quit_550_does_not_invalidate_download(tmp_path: Path):
    """550 no QUIT pós-RETR não pode virar FetchError (caso IIS)."""
    fake = FakeFTP(quit_error=ftplib.error_perm("550 network name unavailable"))
    target = _download(_client(fake), tmp_path)
    assert target.read_bytes() == b"helloworld"
    assert fake.quit_called
    manifest = Path(str(target) + ".manifest.json")
    assert manifest.is_file()


def test_body_exception_propagates_not_masked(tmp_path: Path):
    """Erro no corpo (RETR) propaga embrulhado — o suppress é só p/ quit()."""
    from quantilica.core.exceptions import StorageError

    fake = FakeFTP(retr_error=OSError("connection reset"))
    with pytest.raises(StorageError, match="Could not write stream"):
        _download(_client(fake), tmp_path)
