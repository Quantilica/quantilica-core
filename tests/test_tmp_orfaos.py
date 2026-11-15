"""Limpeza de ``.tmp`` órfãos do mesmo alvo antes do download atômico."""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path

import pytest

from quantilica.core import files as files_mod
from quantilica.core import http as http_mod
from quantilica.core.files import (
    DEFAULT_STALE_TMP_HOURS,
    MIN_STALE_TMP_HOURS,
    STALE_TMP_ENV,
    remove_stale_atomic_temps,
    stale_tmp_max_age_hours,
    write_bytes_atomic,
    write_stream_atomic,
)

HORA = 3600


def _tmp(pasta: Path, alvo: str, sufixo: str, *, idade_h: float) -> Path:
    p = pasta / f".{alvo}.{sufixo}.tmp"
    p.write_bytes(b"x" * 10)
    t = time.time() - idade_h * HORA
    os.utime(p, (t, t))
    return p


@pytest.fixture(autouse=True)
def _sem_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(STALE_TMP_ENV, raising=False)


def test_padrao_e_48_horas() -> None:
    assert DEFAULT_STALE_TMP_HOURS == 48.0
    assert stale_tmp_max_age_hours() == 48.0


def test_env_configura_e_invalido_cai_no_padrao(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv(STALE_TMP_ENV, "6")
    assert stale_tmp_max_age_hours() == 6.0
    monkeypatch.setenv(STALE_TMP_ENV, "abc")
    with caplog.at_level(logging.WARNING):
        assert stale_tmp_max_age_hours() == DEFAULT_STALE_TMP_HOURS
    assert "inválido" in caplog.text
    monkeypatch.setenv(STALE_TMP_ENV, "0")
    assert stale_tmp_max_age_hours() is None


def test_remove_velho_preserva_recente_e_outro_alvo(tmp_path: Path) -> None:
    velho = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=72)
    recente = _tmp(tmp_path, "a.csv", "kq8k160d", idade_h=1)
    outro_alvo = _tmp(tmp_path, "b.csv", "932jot5p", idade_h=72)
    # nome parecido, mas é de outro alvo ("a.csv.gz"), nunca casa
    parecido = _tmp(tmp_path, "a.csv.gz", "beox3r7x", idade_h=72)
    nao_tmp = tmp_path / ".a.csv.dn4hhbty.part"
    nao_tmp.write_bytes(b"x")
    os.utime(nao_tmp, (0, 0))

    removidos = remove_stale_atomic_temps(tmp_path / "a.csv")

    assert removidos == [velho]
    assert not velho.exists()
    assert recente.exists()
    assert outro_alvo.exists()
    assert parecido.exists()
    assert nao_tmp.exists()


@pytest.mark.parametrize("valor", [float("nan"), float("inf"), float("-inf"), 0.5, 5.9])
def test_nan_inf_e_abaixo_do_piso_voltam_ao_padrao_e_nao_apagam_recente(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, valor: float
) -> None:
    recente = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=1)
    meio = _tmp(tmp_path, "a.csv", "kq8k160d", idade_h=20)
    velho = _tmp(tmp_path, "a.csv", "932jot5p", idade_h=72)
    with caplog.at_level(logging.WARNING, logger=files_mod.__name__):
        if valor == float("-inf"):
            # -inf não é finito: padrão de 48 h, não "desliga"
            assert stale_tmp_max_age_hours(valor) == DEFAULT_STALE_TMP_HOURS
        assert stale_tmp_max_age_hours(valor) == DEFAULT_STALE_TMP_HOURS
        assert remove_stale_atomic_temps(tmp_path / "a.csv", max_age_hours=valor) == [
            velho
        ]
    assert recente.exists() and meio.exists() and not velho.exists()
    assert "recusada" in caplog.text


def test_negativo_desliga_e_piso_e_inclusivo() -> None:
    assert stale_tmp_max_age_hours(-3) is None
    assert stale_tmp_max_age_hours(0) is None
    assert stale_tmp_max_age_hours(MIN_STALE_TMP_HOURS) == MIN_STALE_TMP_HOURS


@pytest.mark.parametrize("env", ["nan", "inf", "-inf", "3"])
def test_env_nan_inf_ou_abaixo_do_piso_cai_no_padrao(
    monkeypatch: pytest.MonkeyPatch, env: str
) -> None:
    monkeypatch.setenv(STALE_TMP_ENV, env)
    assert stale_tmp_max_age_hours() == DEFAULT_STALE_TMP_HOURS


def test_limite_configuravel(tmp_path: Path) -> None:
    p = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=10)
    assert remove_stale_atomic_temps(tmp_path / "a.csv") == []
    assert p.exists()
    assert remove_stale_atomic_temps(tmp_path / "a.csv", max_age_hours=8) == [p]


def test_idade_zero_desliga(tmp_path: Path) -> None:
    p = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=500)
    assert remove_stale_atomic_temps(tmp_path / "a.csv", max_age_hours=0) == []
    assert p.exists()


def test_pasta_inexistente_nao_falha(tmp_path: Path) -> None:
    assert remove_stale_atomic_temps(tmp_path / "nada" / "a.csv") == []


@pytest.mark.skipif(
    os.utime not in os.supports_follow_symlinks,
    reason="utime sem follow_symlinks nesta plataforma (Windows)",
)
def test_nao_segue_symlink(tmp_path: Path) -> None:
    fora = tmp_path / "fora.bin"
    fora.write_bytes(b"importante")
    pasta = tmp_path / "dados"
    pasta.mkdir()
    link = pasta / ".a.csv.dn4hhbty.tmp"
    link.symlink_to(fora)
    old = time.time() - 100 * HORA
    os.utime(link, (old, old), follow_symlinks=False)
    assert remove_stale_atomic_temps(pasta / "a.csv") == []
    assert link.is_symlink()
    assert fora.read_bytes() == b"importante"


def test_falha_de_permissao_vira_aviso(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    p = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=72)

    def _nega(path: str) -> None:
        raise PermissionError(13, "Permission denied", path)

    monkeypatch.setattr(files_mod.os, "unlink", _nega)
    with caplog.at_level(logging.WARNING, logger=files_mod.__name__):
        assert remove_stale_atomic_temps(tmp_path / "a.csv") == []
    assert p.exists()
    assert "Não foi possível remover" in caplog.text


def test_falha_inesperada_nao_propaga(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _quebra(path: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(files_mod.os, "scandir", _quebra)
    with caplog.at_level(logging.WARNING, logger=files_mod.__name__):
        assert remove_stale_atomic_temps(tmp_path / "a.csv") == []
    assert "Falha ao limpar" in caplog.text


def test_write_bytes_atomic_nao_varre_a_pasta(tmp_path: Path) -> None:
    """Gravações de bytes (manifests, texto) não pagam o ``scandir`` por arquivo."""
    velho = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=72)
    write_bytes_atomic(tmp_path / "a.csv", b"ok")
    assert (tmp_path / "a.csv").read_bytes() == b"ok"
    assert velho.exists()


def test_write_bytes_atomic_nao_chama_a_limpeza(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _falha(*a: object, **k: object) -> None:
        raise AssertionError("write_bytes_atomic não deve limpar .tmp")

    monkeypatch.setattr(files_mod, "remove_stale_atomic_temps", _falha)
    write_bytes_atomic(tmp_path / "a.csv", b"ok")


def test_write_stream_atomic_limpa_so_o_velho_do_mesmo_alvo(tmp_path: Path) -> None:
    velho = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=72)
    recente = _tmp(tmp_path, "a.csv", "kq8k160d", idade_h=1)
    outro = _tmp(tmp_path, "b.csv", "932jot5p", idade_h=72)
    write_stream_atomic(tmp_path / "a.csv", lambda cb: cb(b"ok"))
    assert not velho.exists()
    assert recente.exists()
    assert outro.exists()


def test_open_atomic_temp_do_http_limpa_o_velho(tmp_path: Path) -> None:
    velho = _tmp(tmp_path, "a.csv", "dn4hhbty", idade_h=72)
    recente = _tmp(tmp_path, "a.csv", "kq8k160d", idade_h=1)
    fd, temp = http_mod._open_atomic_temp(tmp_path / "a.csv")
    os.close(fd)
    assert temp.exists()
    assert not velho.exists()
    assert recente.exists()


def test_caso_capes_quatro_tmp_de_267_mb_apagados(tmp_path: Path) -> None:
    """Os quatro ``.tmp`` reais do capes (7 a 8 dias) saem; o arquivo final vive."""
    nomes = {
        "teses_2012@20230126.csv": "dn4hhbty",
        "teses_2013@20230126.csv": "kq8k160d",
        "teses_2014@20230126.csv": "932jot5p",
        "teses_2015@20230126.csv": "beox3r7x",
    }
    orfaos = [_tmp(tmp_path, n, s, idade_h=7.5 * 24) for n, s in nomes.items()]
    for nome in nomes:
        write_stream_atomic(tmp_path / nome, lambda cb: cb(b"dados"))
    assert all(not o.exists() for o in orfaos)
    assert all((tmp_path / n).read_bytes() == b"dados" for n in nomes)


def test_dois_downloads_do_mesmo_arquivo_nao_se_apagam(tmp_path: Path) -> None:
    """O segundo processo nunca apaga o ``.tmp`` (recente) do primeiro."""
    alvo = tmp_path / "a.csv"
    primeiro_gravando = threading.Event()
    segundo_terminou = threading.Event()
    estado: dict[str, bool] = {}

    def _primeiro(cb) -> None:  # type: ignore[no-untyped-def]
        cb(b"parte-1")
        primeiro_gravando.set()
        assert segundo_terminou.wait(10)
        estado["tmp_sobreviveu"] = len(list(tmp_path.glob(".a.csv.*.tmp"))) >= 1
        cb(b"parte-2")

    def _roda_primeiro() -> None:
        write_stream_atomic(alvo, _primeiro)

    t = threading.Thread(target=_roda_primeiro)
    t.start()
    assert primeiro_gravando.wait(10)
    # o .tmp do primeiro está em uso e recente; o segundo download não pode tocá-lo
    em_voo = list(tmp_path.glob(".a.csv.*.tmp"))
    assert len(em_voo) == 1
    write_stream_atomic(alvo, lambda cb: cb(b"segundo"))
    assert em_voo[0].exists()
    segundo_terminou.set()
    t.join(10)
    assert estado["tmp_sobreviveu"] is True
    assert alvo.read_bytes() == b"parte-1parte-2"
    assert list(tmp_path.glob(".a.csv.*.tmp")) == []
