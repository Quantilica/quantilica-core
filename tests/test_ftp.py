"""Testes para quantilica.core.ftp (contrato de progresso e QUIT resiliente).

Contexto: sync DATASUS 2026-09-27 — todo download FTP com progresso morria no
primeiro chunk (`update_cb()` exige `(downloaded, total)`, mas o FTP chamava
`progress(n)`), e 550 no `QUIT` pós-RETR invalidava downloads íntegros.
"""

from __future__ import annotations

import datetime as dt
import ftplib
import json
from pathlib import Path

import pytest

from quantilica.core.ftp import FtpClient, parse_ftp_list_line
from quantilica.core.manifests import DownloadManifest


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


# ---------------------------------------------------------------------------
# Retenção só do sidecar (arquivo apagado, manifesto sobrevive)
# ---------------------------------------------------------------------------


class ProbeFTP(FakeFTP):
    """FakeFTP cujo SIZE/MDTM são configuráveis e que conta os RETR."""

    def __init__(
        self,
        *,
        size: int | Exception | None = 10,
        mdtm: str | Exception = "213 20240101120000",
        chunks: list[bytes] | None = None,
    ) -> None:
        super().__init__(chunks=chunks, size=size)  # type: ignore[arg-type]
        self._mdtm = mdtm
        self.retr_calls = 0

    def retrbinary(self, cmd: str, cb, blocksize: int = 8192, rest=None) -> str:
        self.retr_calls += 1
        return super().retrbinary(cmd, cb, blocksize, rest)

    def sendcmd(self, cmd: str) -> str:
        if isinstance(self._mdtm, Exception):
            raise self._mdtm
        return self._mdtm


def _fetch(client: FtpClient, target: Path, *, force: bool = False) -> Path:
    return client.download_with_manifest(
        "pub/x.dbc",
        target,
        source_id="datasus",
        dataset_id="sih-rd",
        producer="test",
        force=force,
    )


def _sidecar_only(tmp_path: Path, *, mdtm: str = "213 20240101120000") -> Path:
    """Baixa, apaga o arquivo e deixa só o manifesto lateral."""
    target = tmp_path / "x.dbc"
    _fetch(_client(ProbeFTP(mdtm=mdtm)), target)
    target.unlink()
    assert Path(str(target) + ".manifest.json").is_file()
    return target


def test_download_records_remote_mdtm_in_manifest(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    manifest = DownloadManifest.read_json(Path(str(target) + ".manifest.json"))
    assert manifest.source_meta is not None
    assert manifest.source_meta.last_modified == "2024-01-01T12:00:00+00:00"
    assert manifest.size_bytes == 10


def test_sidecar_only_same_size_and_date_skips(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10)
    assert _fetch(_client(fake), target) == target
    assert fake.retr_calls == 0
    assert not target.exists()  # sidecar-only: o arquivo não é recriado


def test_sidecar_only_older_remote_date_skips(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10, mdtm="213 20231231000000")
    _fetch(_client(fake), target)
    assert fake.retr_calls == 0


def test_sidecar_only_different_size_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=11, chunks=[b"hello", b"world", b"!"])
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1
    assert target.read_bytes() == b"helloworld!"


def test_sidecar_only_newer_remote_date_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10, mdtm="213 20240102000000")
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1
    assert target.exists()


def test_sidecar_only_unreadable_manifest_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    Path(str(target) + ".manifest.json").write_text("[1, 2]", encoding="utf-8")
    fake = ProbeFTP(size=10)
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_garbage_manifest_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    Path(str(target) + ".manifest.json").write_text("{nao e json", encoding="utf-8")
    fake = ProbeFTP(size=10)
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_without_sidecar_downloads(tmp_path: Path):
    target = tmp_path / "x.dbc"
    fake = ProbeFTP(size=10)
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_force_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10)
    _fetch(_client(fake), target, force=True)
    assert fake.retr_calls == 1
    assert target.exists()


def test_sidecar_only_server_without_size_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=ftplib.error_perm("500 SIZE not supported"))
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_server_without_mdtm_downloads(tmp_path: Path):
    """Sem data o HTTP também baixa (sem Last-Modified): mesma conservadorice."""
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10, mdtm=ftplib.error_perm("500 MDTM not supported"))
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_malformed_mdtm_downloads(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10, mdtm="213 not-a-date")
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_legacy_manifest_uses_fetched_at(tmp_path: Path):
    """Manifestos antigos (sem source_meta) comparam a data com ``fetched_at``."""
    target = _sidecar_only(tmp_path)
    sidecar = Path(str(target) + ".manifest.json")
    data = json.loads(sidecar.read_text(encoding="utf-8"))
    data.pop("source_meta", None)
    data["fetched_at"] = "2026-11-01T00:00:00+00:00"
    sidecar.write_text(json.dumps(data), encoding="utf-8")

    fake = ProbeFTP(size=10, mdtm="213 20260101000000")  # antes do fetched_at
    _fetch(_client(fake), target)
    assert fake.retr_calls == 0

    fake = ProbeFTP(size=10, mdtm="213 20261201000000")  # depois do fetched_at
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


def test_sidecar_only_mdtm_with_fraction_is_parsed(tmp_path: Path):
    target = _sidecar_only(tmp_path)
    fake = ProbeFTP(size=10, mdtm="213 20240101120000.250")
    _fetch(_client(fake), target)
    assert fake.retr_calls == 0


def test_present_file_keeps_mdtm_check(tmp_path: Path):
    """Arquivo presente: comportamento de antes (compara mtime local com MDTM)."""
    target = tmp_path / "x.dbc"
    _fetch(_client(ProbeFTP()), target)
    fake = ProbeFTP(mdtm="213 20200101000000")  # remoto bem mais velho
    _fetch(_client(fake), target)
    assert fake.retr_calls == 0
    fake = ProbeFTP(mdtm="213 20990101000000")  # remoto no futuro
    _fetch(_client(fake), target)
    assert fake.retr_calls == 1


# ---------------------------------------------------------------------------
# parse_ftp_list_line
# ---------------------------------------------------------------------------


def test_parse_ftp_list_line_iis_file_with_am():
    line = "04-21-26  09:00AM       123456789 arquivo.csv"
    result = parse_ftp_list_line(line)
    assert result is not None
    name, size, modified = result
    assert name == "arquivo.csv"
    assert size == 123456789
    assert modified == dt.datetime(2026, 4, 21, 9, 0)


def test_parse_ftp_list_line_iis_file_with_pm():
    line = "10-02-02  03:31PM       21850 PARIS17.TXT"
    result = parse_ftp_list_line(line)
    assert result is not None
    name, size, modified = result
    assert name == "PARIS17.TXT" or name == "PARIS17"
    assert size == 21850
    assert modified == dt.datetime(2002, 10, 2, 15, 31)


def test_parse_ftp_list_line_iis_dir_returns_none():
    line = "10-02-02  03:31PM       <DIR>          docs"
    assert parse_ftp_list_line(line) is None


def test_parse_ftp_list_line_iis_name_with_spaces():
    line = "01-05-25  11:59AM             1024 meu relatorio final.xlsx"
    result = parse_ftp_list_line(line)
    assert result is not None
    name, size, modified = result
    assert name == "meu relatorio final.xlsx"
    assert size == 1024
    assert modified == dt.datetime(2025, 1, 5, 11, 59)


def test_parse_ftp_list_line_unix_regular_file_recent_time():
    line = "-rw-r--r-- 1 ftp ftp 123456789 Apr 21 09:00 arquivo.csv"
    result = parse_ftp_list_line(line, now=dt.datetime(2026, 11, 26, 12, 0))
    assert result is not None
    name, size, modified = result
    assert name == "arquivo.csv"
    assert size == 123456789
    assert modified == dt.datetime(2026, 4, 21, 9, 0)


def test_parse_ftp_list_line_unix_time_slot_before_now_keeps_year():
    line = "-rw-r--r-- 1 ftp ftp 10 Apr 21 09:00 a.csv"
    # um minuto depois do instante listado: ainda é do mesmo ano
    result = parse_ftp_list_line(line, now=dt.datetime(2026, 4, 21, 9, 1))
    assert result is not None
    assert result[2] == dt.datetime(2026, 4, 21, 9, 0)


def test_parse_ftp_list_line_unix_time_slot_after_now_goes_back_one_year():
    line = "-rw-r--r-- 1 ftp ftp 10 Apr 21 09:00 a.csv"
    # um minuto antes do instante listado: no futuro, logo do ano passado
    result = parse_ftp_list_line(line, now=dt.datetime(2026, 4, 21, 8, 59))
    assert result is not None
    assert result[2] == dt.datetime(2025, 4, 21, 9, 0)


@pytest.mark.parametrize(
    ("now", "expected_year"),
    [
        (dt.datetime(2027, 1, 1, 0, 0), 2026),  # janeiro: "Apr 21" é do ano anterior
        (dt.datetime(2027, 4, 20, 23, 59), 2026),  # véspera do dia listado
        (dt.datetime(2027, 4, 21, 9, 0), 2027),  # no instante exato
        (dt.datetime(2027, 12, 31, 23, 59), 2027),
    ],
)
def test_parse_ftp_list_line_unix_year_turn_with_fixed_clock(now, expected_year):
    line = "-rw-r--r-- 1 ftp ftp 10 Apr 21 09:00 a.csv"
    result = parse_ftp_list_line(line, now=now)
    assert result is not None
    assert result[2] == dt.datetime(expected_year, 4, 21, 9, 0)


def test_parse_ftp_list_line_unix_leap_day_in_non_leap_year_goes_back():
    line = "-rw-r--r-- 1 ftp ftp 10 Feb 29 10:00 bissexto.csv"
    # 2027 não é bissexto: o 29/02 listado só pode ser de 2024
    result = parse_ftp_list_line(line, now=dt.datetime(2027, 3, 1, 12, 0))
    assert result is not None
    assert result[2] == dt.datetime(2024, 2, 29, 10, 0)


def test_parse_ftp_list_line_unix_time_slot_without_now_uses_local_clock():
    line = "-rw-r--r-- 1 ftp ftp 10 Jan  1 00:00 a.csv"
    before = dt.datetime.now()
    result = parse_ftp_list_line(line)
    after = dt.datetime.now()
    assert result is not None
    assert result[2] <= after
    assert result[2].year in {before.year, before.year - 1, after.year}


def test_parse_ftp_list_line_unix_file_with_explicit_year():
    result = parse_ftp_list_line("-rw-r--r-- 1 root root 999 Jul  4 2023 old-data.json")
    assert result is not None
    name, size, modified = result
    assert name == "old-data.json"
    assert size == 999
    assert modified == dt.datetime(2023, 7, 4, 0, 0)


def test_parse_ftp_list_line_unix_directory_returns_none():
    assert parse_ftp_list_line("drwxr-xr-x 2 ftp ftp 4096 Apr 21 09:00 pub") is None


def test_parse_ftp_list_line_unix_symlink_returns_none():
    assert (
        parse_ftp_list_line("lrwxrwxrwx 1 ftp ftp 7 Apr 21 09:00 link -> target")
        is None
    )


def test_parse_ftp_list_line_empty_and_garbage_return_none():
    assert parse_ftp_list_line("") is None
    assert parse_ftp_list_line("   ") is None
    assert parse_ftp_list_line("total 3") is None
    assert parse_ftp_list_line("random garbage without structure") is None


def test_parse_ftp_list_line_unix_recent_file_not_in_future():
    """Arquivo com hora futura de hoje deve recuar o ano (heurística do ls)."""
    now = dt.datetime(2026, 11, 26, 10, 30)
    line = "-rw-r--r-- 1 ftp ftp 10 Nov 26 13:30 recent.csv"
    result = parse_ftp_list_line(line, now=now)
    assert result is not None
    _, _, modified = result
    assert modified == dt.datetime(2025, 11, 26, 13, 30)


def test_parse_ftp_list_line_unix_recent_file_new_year_eve_not_ambiguous():
    """Com o relógio fixo, a virada do ano deixa de ser ambígua."""
    now = dt.datetime(2026, 12, 31, 23, 59)
    line = "-rw-r--r-- 1 ftp ftp 10 Dec 31 23:30 recent.csv"
    result = parse_ftp_list_line(line, now=now)
    assert result is not None
    assert result[2] == dt.datetime(2026, 12, 31, 23, 30)


def test_parse_ftp_list_line_exported_from_package_root():
    import quantilica.core

    assert "parse_ftp_list_line" in quantilica.core.__all__
    assert quantilica.core.parse_ftp_list_line is parse_ftp_list_line
