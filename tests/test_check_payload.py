"""check_payload: download que não é o dado que diz ser."""

import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

from quantilica.core.exceptions import CorruptPayloadError
from quantilica.core.files import check_payload, write_stream_atomic


def test_pagina_html_salva_como_csv(tmp_path: Path) -> None:
    src = tmp_path / "vendas.csv"
    src.write_bytes(
        b'\xef\xbb\xbf\n<!DOCTYPE html>\n<html><head><meta http-equiv="Pragma"/>'
    )
    with pytest.raises(CorruptPayloadError, match="HTML"):
        check_payload(src)


def test_html_legitimo_passa(tmp_path: Path) -> None:
    src = tmp_path / "GruposPrincipais.html"
    src.write_text("<html></html>")
    check_payload(src)


def test_csv_normal_passa(tmp_path: Path) -> None:
    src = tmp_path / "dados.csv"
    src.write_text("a;b\n1;2\n")
    check_payload(src)


def test_zip_com_crc_invalido(tmp_path: Path) -> None:
    src = tmp_path / "dados.zip"
    with zipfile.ZipFile(src, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("dados.csv", "a;b\n" + "1;2\n" * 1000)
    data = bytearray(src.read_bytes())
    data[100] ^= 0xFF  # corrompe o conteúdo, não o diretório central
    src.write_bytes(bytes(data))
    with pytest.raises(CorruptPayloadError):
        check_payload(src)


def test_zip_truncado(tmp_path: Path) -> None:
    src = tmp_path / "dados.zip"
    with zipfile.ZipFile(src, "w") as zf:
        zf.writestr("dados.csv", "a;b\n1;2\n")
    src.write_bytes(src.read_bytes()[:30])
    with pytest.raises(CorruptPayloadError, match="ZIP"):
        check_payload(src)


@pytest.mark.skipif(shutil.which("7z") is None, reason="requer 7z")
def test_7z_corrompido(tmp_path: Path) -> None:
    txt = tmp_path / "CAGEDEST_052008.txt"
    txt.write_text("linha;de;dados\n" * 20000)
    src = tmp_path / "caged.7z"
    subprocess.run(
        ["7z", "a", "-bd", str(src), str(txt)], check=True, capture_output=True
    )
    check_payload(src)  # íntegro
    data = bytearray(src.read_bytes())
    for i in range(64, 96):
        data[i] ^= 0xFF
    src.write_bytes(bytes(data))
    with pytest.raises(CorruptPayloadError, match="7z"):
        check_payload(src)


def test_write_stream_atomic_nao_publica_payload_invalido(tmp_path: Path) -> None:
    target = tmp_path / "taxas.csv"
    target.write_text("versão anterior")

    def stream(write) -> None:
        write(b"<html><body>erro</body></html>")

    with pytest.raises(CorruptPayloadError):
        write_stream_atomic(
            target, stream, validate=lambda p: check_payload(p, target.name)
        )
    assert target.read_text() == "versão anterior"
    assert list(tmp_path.iterdir()) == [target]  # sem temporário sobrando
