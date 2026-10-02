import datetime as dt
import json
import os
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import httpx2

from quantilica.core import (
    FreshnessProbe,
    FtpFreshnessProbe,
    HttpFreshnessProbe,
    RemoteStat,
    is_manifest_valid,
    should_skip,
    write_manifest_sidecar,
)
from quantilica.core.ftp import FtpClient
from quantilica.core.http import HttpClient
from quantilica.core.manifests import (
    DownloadManifest,
    SourceMetadata,
)
from quantilica.core.sync import _ftp_path, manifest_target_path

UTC = dt.UTC


def _make_target_with_manifest(
    tmp_path,
    name: str = "data.bin",
    content: bytes = b"abc",
    **manifest_kwargs,
):
    target = tmp_path / name
    target.write_bytes(content)
    manifest = DownloadManifest.from_file(
        source_id="src",
        dataset_id="ds",
        url="https://example.test/data",
        file_path=target,
        **manifest_kwargs,
    )
    write_manifest_sidecar(target, manifest)
    return target


# ---------------------------------------------------------------------------
# RemoteStat


def test_remote_stat_defaults_all_none():
    stat = RemoteStat()
    assert stat.size is None
    assert stat.last_modified is None
    assert stat.etag is None


def test_remote_stat_holds_values():
    stamp = dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    stat = RemoteStat(size=10, last_modified=stamp, etag="v1")
    assert stat.size == 10
    assert stat.last_modified == stamp
    assert stat.etag == "v1"


# ---------------------------------------------------------------------------
# HttpFreshnessProbe


def test_http_freshness_probe_parses_headers():
    headers = {
        "Content-Length": "123",
        "Last-Modified": "Mon, 01 Jan 2024 12:00:00 GMT",
        "ETag": 'W/"abc123"',
    }

    def handler(request):
        if request.method == "HEAD":
            return httpx2.Response(200, headers=headers)
        return httpx2.Response(200, content=b"x" * 123, headers=headers)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    stat = HttpFreshnessProbe(client).probe("https://example.test/data")

    assert stat == RemoteStat(
        size=123,
        last_modified=dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        etag='W/"abc123"',
    )


def test_http_freshness_probe_get_fallback_when_head_rejected():
    headers = {
        "Content-Length": "3",
        "Last-Modified": "Mon, 01 Jan 2024 12:00:00 GMT",
    }

    def handler(request):
        if request.method == "HEAD":
            return httpx2.Response(405)
        return httpx2.Response(200, content=b"abc", headers=headers)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    stat = HttpFreshnessProbe(client).probe("https://example.test/data")

    assert stat is not None
    assert stat.size == 3
    assert stat.last_modified == dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC)


def test_http_freshness_probe_none_on_http_error():
    def handler(request):
        return httpx2.Response(500)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    assert HttpFreshnessProbe(client).probe("https://example.test/data") is None


def test_http_freshness_probe_none_when_no_metadata():
    def handler(request):
        return httpx2.Response(200)

    client = HttpClient(attempts=1, transport=httpx2.MockTransport(handler))
    assert HttpFreshnessProbe(client).probe("https://example.test/data") is None


# ---------------------------------------------------------------------------
# FtpFreshnessProbe


class _FakeFTP:
    def __init__(self, size=None, mdtm="213 20240101120000"):
        self._size = size
        self._mdtm = mdtm
        self.size_calls: list[str] = []
        self.mdtm_calls: list[str] = []

    def size(self, path):
        self.size_calls.append(path)
        if isinstance(self._size, Exception):
            raise self._size
        return self._size

    def sendcmd(self, cmd):
        self.mdtm_calls.append(cmd)
        if cmd.startswith("MDTM"):
            return self._mdtm
        return "200 ok"


class _FakeFtpClient(FtpClient):
    def __init__(self, ftp):
        self.host = "ftp.test"
        self._ftp = ftp

    @contextmanager
    def _connected(self):
        yield self._ftp


def test_ftp_freshness_probe_parses_size_and_mdtm():
    ftp = _FakeFTP(size=456)
    client = _FakeFtpClient(ftp)
    stat = FtpFreshnessProbe(client).probe("/dir/file.csv")

    assert stat == RemoteStat(
        size=456,
        last_modified=dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        etag=None,
    )
    assert ftp.size_calls == ["/dir/file.csv"]
    assert ftp.mdtm_calls == ["MDTM /dir/file.csv"]


def test_ftp_freshness_probe_strips_ftp_url_prefix():
    ftp = _FakeFTP(size=1)
    client = _FakeFtpClient(ftp)
    stat = FtpFreshnessProbe(client).probe("ftp://ftp.test/dir/file.csv")

    assert stat is not None
    assert ftp.size_calls == ["/dir/file.csv"]


def test_ftp_freshness_probe_partial_when_one_command_fails():
    ftp = _FakeFTP(size=None)
    client = _FakeFtpClient(ftp)
    stat = FtpFreshnessProbe(client).probe("/dir/file.csv")

    assert stat == RemoteStat(
        size=None,
        last_modified=dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        etag=None,
    )


def test_ftp_freshness_probe_none_when_no_metadata():
    ftp = _FakeFTP(size=None, mdtm="213 not-a-timestamp")
    client = _FakeFtpClient(ftp)
    assert FtpFreshnessProbe(client).probe("/dir/file.csv") is None


def test_ftp_freshness_probe_none_when_connection_fails():
    class BrokenClient(_FakeFtpClient):
        @contextmanager
        def _connected(self):
            raise ConnectionError("boom")
            yield  # pragma: no cover

    probe = FtpFreshnessProbe(BrokenClient(_FakeFTP()))
    assert probe.probe("/dir/file.csv") is None


# ---------------------------------------------------------------------------
# FreshnessProbe protocol


def test_freshness_probe_protocol_runtime_checkable():
    client = HttpClient(attempts=1, transport=httpx2.MockTransport(lambda r: None))
    assert isinstance(HttpFreshnessProbe(client), FreshnessProbe)

    class _Broken:
        pass

    assert not isinstance(_Broken(), FreshnessProbe)


# ---------------------------------------------------------------------------
# should_skip


def test_should_skip_never_policy_returns_false(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, RemoteStat(size=3), policy="never") is False


def test_should_skip_force_true_returns_false(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, RemoteStat(size=3), force=True) is False


def test_should_skip_missing_target_always_false(tmp_path):
    target = tmp_path / "missing.bin"
    for policy in ("freshness", "strict_manifest", "exists", "never"):
        assert should_skip(target, RemoteStat(size=3), policy) is False


def test_should_skip_exists_policy(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, None, policy="exists") is True


def test_should_skip_strict_manifest_valid(tmp_path):
    target = _make_target_with_manifest(tmp_path)
    assert should_skip(target, None, policy="strict_manifest") is True


def test_should_skip_strict_manifest_missing_manifest(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, None, policy="strict_manifest") is False


def test_should_skip_strict_manifest_corrupted_file(tmp_path):
    target = _make_target_with_manifest(tmp_path)
    target.write_bytes(b"tampered")
    assert should_skip(target, None, policy="strict_manifest") is False


def test_should_skip_freshness_none_stat(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, None, policy="freshness") is False


def test_should_skip_freshness_size_mismatch(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, RemoteStat(size=999)) is False


def test_should_skip_freshness_remote_newer(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    remote = dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    os.utime(target, (remote.timestamp() - 60, remote.timestamp() - 60))
    assert should_skip(target, RemoteStat(size=3, last_modified=remote)) is False


def test_should_skip_freshness_remote_not_newer(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    remote = dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC)
    os.utime(target, (remote.timestamp() + 60, remote.timestamp() + 60))
    assert should_skip(target, RemoteStat(size=3, last_modified=remote)) is True


def test_should_skip_freshness_etag_matches_manifest(tmp_path):
    target = _make_target_with_manifest(tmp_path)
    base = DownloadManifest.from_file(
        source_id="src",
        dataset_id="ds",
        url="https://example.test/data",
        file_path=target,
    )
    manifest = replace(base, source_meta=SourceMetadata(etag="v42"))
    write_manifest_sidecar(target, manifest)

    assert should_skip(target, RemoteStat(etag="v42")) is True
    assert should_skip(target, RemoteStat(etag="v43")) is False


def test_should_skip_freshness_etag_without_manifest(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, RemoteStat(etag="v42")) is False


def test_should_skip_freshness_no_info(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")
    assert should_skip(target, RemoteStat()) is False


# ---------------------------------------------------------------------------
# is_manifest_valid


def test_is_manifest_valid_round_trip(tmp_path):
    target = _make_target_with_manifest(tmp_path, content=b"payload")
    sidecar = target.with_suffix(target.suffix + ".manifest.json")

    assert is_manifest_valid(sidecar) is True


def test_is_manifest_valid_false_for_missing_manifest(tmp_path):
    assert is_manifest_valid(tmp_path / "nope.manifest.json") is False


def test_is_manifest_valid_false_for_missing_artifact(tmp_path):
    content = b"abc"
    manifest = DownloadManifest.from_content(
        source_id="src",
        dataset_id="ds",
        url="https://example.test/data",
        content=content,
    )
    sidecar = write_manifest_sidecar(tmp_path / "ghost.bin", manifest)
    assert not (tmp_path / "ghost.bin").exists()
    assert is_manifest_valid(sidecar) is False


def test_is_manifest_valid_false_for_size_mismatch(tmp_path):
    target = _make_target_with_manifest(tmp_path)
    sidecar = target.with_suffix(target.suffix + ".manifest.json")
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["size_bytes"] = 9999
    sidecar.write_text(json.dumps(payload), encoding="utf-8")

    assert is_manifest_valid(sidecar) is False


def test_is_manifest_valid_false_for_sha_mismatch(tmp_path):
    target = _make_target_with_manifest(tmp_path)
    sidecar = target.with_suffix(target.suffix + ".manifest.json")
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    payload["sha256"] = "deadbeef" * 8
    sidecar.write_text(json.dumps(payload), encoding="utf-8")

    assert is_manifest_valid(sidecar) is False


def test_is_manifest_valid_false_for_corrupt_json(tmp_path):
    sidecar = tmp_path / "data.bin.manifest.json"
    sidecar.write_text("{not json", encoding="utf-8")

    assert is_manifest_valid(sidecar) is False


def test_manifest_target_path_round_trip():
    manifest_path = Path("a/b/data.bin.manifest.json")
    assert manifest_target_path(manifest_path).name == "data.bin"


# ---------------------------------------------------------------------------
# helpers


def test_ftp_path_helper_handles_plain_paths():
    assert _ftp_path("/dir/file.csv") == "/dir/file.csv"
    assert _ftp_path("ftp://host/dir/file.csv") == "/dir/file.csv"
    assert _ftp_path("ftp://host") == "/"
