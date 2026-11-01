"""Tests for payload signature validation, errors and FreshnessPolicy."""

import io
import zipfile
from pathlib import Path

import pytest

from quantilica.core.exceptions import InvalidPayloadSignatureError
from quantilica.core.storage import expected_format_for_path, validate_payload_signature
from quantilica.core.sync import FreshnessPolicy


def test_expected_format_for_path_recognizes_extensions():
    assert expected_format_for_path("ca-combustiveis_2024-S1@20240810.csv") == "csv"
    assert expected_format_for_path("empresas_2025-07@20250710.zip") == "zip"
    assert expected_format_for_path("capacidade-geracao@20261020.parquet") == "parquet"
    assert expected_format_for_path("planilha.xlsx") == "xlsx"
    assert expected_format_for_path("dados_N1.ndjson") == "ndjson"
    assert expected_format_for_path("lote.jsonl") == "ndjson"
    assert expected_format_for_path("arquivo.7z") == "7z"
    assert expected_format_for_path("arquivo.pdf") == "pdf"
    assert expected_format_for_path("microdado.dbc") == "dbc"


def test_expected_format_for_path_unknown_returns_none():
    assert expected_format_for_path("data.bin") is None
    assert expected_format_for_path("sem-extensao") is None
    assert expected_format_for_path("data.tar.gz").lower() == "gz"


def test_expected_format_for_path_case_insensitive():
    assert expected_format_for_path("DATA.CSV") == "csv"
    assert expected_format_for_path("file.Parquet") == "parquet"


def test_validate_payload_signature_min_bytes():
    with pytest.raises(InvalidPayloadSignatureError, match="inferior ao mínimo"):
        validate_payload_signature(b"", "csv", min_bytes=10)


def test_validate_payload_signature_soft_200_html(tmp_path: Path):
    html_content = (
        b"<!DOCTYPE html><html><head><title>Error</title></head>"
        b"<body>404 Not Found</body></html>"
    )
    # Should reject when expected_format is not html
    with pytest.raises(InvalidPayloadSignatureError, match="recebida página HTML"):
        validate_payload_signature(html_content, "csv")

    f = tmp_path / "test.zip"
    f.write_bytes(html_content)
    with pytest.raises(InvalidPayloadSignatureError, match="recebida página HTML"):
        validate_payload_signature(f, "zip")


def test_validate_payload_signature_waf_block():
    waf_content = (
        b"The requested URL was rejected. Please consult with your administrator."
    )
    with pytest.raises(InvalidPayloadSignatureError, match="bloqueio WAF"):
        validate_payload_signature(waf_content, "csv")

    cf_content = b"<html><title>Attention Required! | Cloudflare</title></html>"
    with pytest.raises(InvalidPayloadSignatureError, match="recebida página HTML"):
        validate_payload_signature(cf_content, "json")


def test_validate_payload_signature_zip_valid_and_truncated(tmp_path: Path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("test.txt", "hello world")
    valid_zip = buf.getvalue()

    # Valid zip passes
    validate_payload_signature(valid_zip, "zip")

    # Valid xlsx passes (xlsx is a zip)
    validate_payload_signature(valid_zip, "xlsx")

    # Truncated zip (only header PK\x03\x04 without central directory)
    truncated = valid_zip[:30]
    with pytest.raises(InvalidPayloadSignatureError, match="corrompido ou truncado"):
        validate_payload_signature(truncated, "zip")

    # Invalid header
    with pytest.raises(InvalidPayloadSignatureError, match="Assinatura inválida"):
        validate_payload_signature(b"NOT_A_ZIP_CONTENT_AT_ALL_12345678", "zip")


def test_validate_payload_signature_parquet(tmp_path: Path):
    valid_parquet = b"PAR1" + b"\x00" * 20 + b"PAR1"
    validate_payload_signature(valid_parquet, "parquet")

    # Bad header
    with pytest.raises(InvalidPayloadSignatureError, match="header Parquet"):
        validate_payload_signature(b"BAD1" + b"\x00" * 20 + b"PAR1", "parquet")

    # Bad footer
    with pytest.raises(InvalidPayloadSignatureError, match="footer Parquet"):
        validate_payload_signature(b"PAR1" + b"\x00" * 20 + b"BAD1", "parquet")


def test_validate_payload_signature_other_formats():
    # 7z
    validate_payload_signature(b"7z\xbc\xaf\x27\x1c\x00\x00", "7z")
    with pytest.raises(InvalidPayloadSignatureError, match="7z"):
        validate_payload_signature(b"BAD_7Z_SIG", "7z")

    # gz
    validate_payload_signature(b"\x1f\x8b\x08\x00\x00", "gz")
    with pytest.raises(InvalidPayloadSignatureError, match="gz"):
        validate_payload_signature(b"BAD_GZ", "gz")

    # zst
    validate_payload_signature(b"\x28\xb5\x2f\xfd\x00\x00", "zst")
    with pytest.raises(InvalidPayloadSignatureError, match="zst"):
        validate_payload_signature(b"BAD_ZST", "zst")

    # pdf
    validate_payload_signature(b"%PDF-1.4\ncontent...", "pdf")
    with pytest.raises(InvalidPayloadSignatureError, match="PDF"):
        validate_payload_signature(b"BAD_PDF", "pdf")

    # json / ndjson
    validate_payload_signature(b'{"key": "value"}', "json")
    validate_payload_signature(b"[1, 2, 3]", "ndjson")
    validate_payload_signature(b'\xef\xbb\xbf{"key": "bom"}', "json")
    with pytest.raises(InvalidPayloadSignatureError, match="json"):
        validate_payload_signature(b"plain text not json", "json")


def test_freshness_policy_enum():
    assert FreshnessPolicy.HEAD_OR_MANIFEST == "head_or_manifest"
    assert FreshnessPolicy.HEAD_FALLBACK_GET == "head_fallback_get"
    assert FreshnessPolicy.REMOTE_ONLY == "remote_only"
