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


def test_parquet_valido_e_invalido(tmp_path: Path) -> None:
    # 1. Valid parquet (minimal dummy structure >= 12 bytes with PAR1 at start and end)
    valid_parquet = tmp_path / "valid.parquet"
    valid_parquet.write_bytes(b"PAR1" + b"\x00" * 4 + b"\x00\x00\x00\x00" + b"PAR1")
    check_payload(valid_parquet)

    # 2. Too small
    too_small = tmp_path / "short.parquet"
    too_small.write_bytes(b"PAR1")
    with pytest.raises(CorruptPayloadError, match="< 12 bytes"):
        check_payload(too_small)

    # 3. Bad magic
    bad_magic = tmp_path / "bad.parquet"
    bad_magic.write_bytes(b"PAR1" + b"\x00" * 8 + b"XXXX")
    with pytest.raises(CorruptPayloadError, match="PAR1 ausente"):
        check_payload(bad_magic)


def test_expect_header_validation(tmp_path: Path) -> None:
    csv_file = tmp_path / "dataset.csv"
    csv_file.write_text("id;nome;valor\n1;teste;100\n")

    # Header matches
    check_payload(csv_file, expect_header=lambda h: h.startswith(b"id;nome"))

    # Header does not match
    with pytest.raises(CorruptPayloadError, match="cabeçalho não confere"):
        check_payload(
            csv_file,
            expect_header=lambda h: h.startswith(b"codigo;descricao"),
        )


def test_empty_and_header_only_payload(tmp_path: Path) -> None:
    from quantilica.core.exceptions import EmptyPayloadError

    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")
    with pytest.raises(EmptyPayloadError, match="vazio"):
        check_payload(empty_csv)

    header_only_csv = tmp_path / "header_only.csv"
    header_only_csv.write_text("col1;col2;col3\n")
    with pytest.raises(EmptyPayloadError, match="apenas cabeçalho"):
        check_payload(header_only_csv)

    # Allowed when allow_header_only=True
    check_payload(header_only_csv, allow_header_only=True)
