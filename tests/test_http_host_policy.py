"""Tests for HostPolicy, Retry-After, max_bytes, and remote_zip_members."""

import io
import time
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from quantilica.core.exceptions import StorageError, TooManyRedirects
from quantilica.core.http import (
    HostPolicy,
    HttpClient,
    RetryableHttpStatusError,
    ZipMember,
    _parse_retry_after,
    remote_zip_members,
)


def test_parse_retry_after():
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("") is None
    assert _parse_retry_after("invalid") is None
    assert _parse_retry_after("120") == 120.0
    assert _parse_retry_after(" 45 ") == 45.0


def test_retry_after_in_retryable_status_error():
    err = RetryableHttpStatusError("https://example.com", 429, retry_after=30.0)
    assert err.status_code == 429
    assert err.retry_after == 30.0


def test_host_policy_rate_limit():
    policy = HostPolicy(min_interval=0.1)
    client = HttpClient(host_policies={"api.example.com": policy})

    t0 = time.monotonic()
    client._acquire_rate_limits("https://api.example.com/data")
    client._acquire_rate_limits("https://api.example.com/data")
    t1 = time.monotonic()
    assert (t1 - t0) >= 0.08  # Tolerates small timer jitter


def test_download_with_manifest_max_bytes(tmp_path):
    client = HttpClient()
    target = tmp_path / "test.csv"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Length": "100"}
    mock_resp.url = "https://example.com/test.csv"
    mock_resp.iter_bytes.return_value = [b"a" * 60, b"b" * 60]  # total 120 bytes

    with patch.object(client, "head_or_get", return_value=mock_resp):
        with patch.object(client, "stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_resp
            with pytest.raises(StorageError, match="exceeded max_bytes limit"):
                client.download_with_manifest(
                    "https://example.com/test.csv",
                    target,
                    source_id="test",
                    dataset_id="test",
                    producer="test",
                    max_bytes=100,
                )


def test_download_with_manifest_incomplete_content_length(tmp_path):
    client = HttpClient()
    target = tmp_path / "test.csv"

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Length": "200"}
    mock_resp.url = "https://example.com/test.csv"
    mock_resp.iter_bytes.return_value = [b"a;b\n1;2\n"]  # only 8 bytes, expected 200

    with patch.object(client, "head_or_get", return_value=mock_resp):
        with patch.object(client, "stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_resp
            with pytest.raises(StorageError, match="Incomplete download"):
                client.download_with_manifest(
                    "https://example.com/test.csv",
                    target,
                    source_id="test",
                    dataset_id="test",
                    producer="test",
                )


def test_remote_zip_members_inspection():
    # Build a real ZIP in memory
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("arquivo1.csv", "id;valor\n1;100\n")
        zf.writestr("pasta/arquivo2.txt", "conteudo de teste\n")
    zip_bytes = buf.getvalue()
    zip_size = len(zip_bytes)

    client_mock = MagicMock()
    head_mock = MagicMock()
    head_mock.headers = {"Content-Length": str(zip_size)}
    client_mock.head_or_get.return_value = head_mock

    def fake_get_bytes(url, headers=None):
        if headers and "Range" in headers:
            range_header = headers["Range"]
            parts = range_header.replace("bytes=", "").split("-")
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else zip_size - 1
            return zip_bytes[start : end + 1]
        return zip_bytes

    client_mock.get_bytes.side_effect = fake_get_bytes

    members = remote_zip_members(client_mock, "https://example.com/dados.zip")
    assert len(members) == 2
    assert members[0].name == "arquivo1.csv"
    assert members[0].size == len("id;valor\n1;100\n")
    assert members[1].name == "pasta/arquivo2.txt"
    assert isinstance(members[0], ZipMember)


def test_too_many_redirects_raised():
    import httpx2

    client = HttpClient()
    with patch.object(client, "_get_client") as mock_get:
        mock_c = MagicMock()
        mock_c.request.side_effect = httpx2.TooManyRedirects("Exceeded redirect loop")
        mock_get.return_value = (mock_c, True)

        with pytest.raises(TooManyRedirects, match="Too many redirects"):
            client.request("GET", "https://example.com/loop")
