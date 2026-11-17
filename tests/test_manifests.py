import json
import os
from dataclasses import replace

import pytest

from quantilica.core.files import sha256_bytes
from quantilica.core.manifests import (
    MANIFEST_VERSION,
    ContentFingerprint,
    DatasetManifest,
    DownloadManifest,
    ExecutionInfo,
    Lineage,
    QualitySignals,
    RunManifest,
    SourceMetadata,
    read_manifest_sidecar,
    trusted_sha256,
    write_manifest_sidecar,
)


def test_download_manifest_from_content():
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        resource_id="7060",
        url="https://example.test/data.json",
        content=b"abc",
        producer="ibge-sidra-fetcher",
        producer_version="0.1.0",
        metadata={"format": "json"},
    )

    assert manifest.source_id == "ibge"
    assert manifest.dataset_id == "sidra-ipca"
    assert manifest.sha256 == sha256_bytes(b"abc")
    assert manifest.size_bytes == 3
    assert manifest.fetched_at.endswith("Z")
    assert manifest.metadata["format"] == "json"


def test_download_manifest_from_digest():
    manifest = DownloadManifest.from_digest(
        source_id="tesouro-direto",
        dataset_id="vendas",
        url="https://example.test/data.csv",
        sha256="deadbeef",
        size_bytes=1024,
        producer="tesouro-direto-fetcher",
        path="/tmp/x.csv",
        metadata={"chunked": True},
    )

    assert manifest.sha256 == "deadbeef"
    assert manifest.size_bytes == 1024
    assert manifest.path == "/tmp/x.csv"
    assert manifest.fetched_at.endswith("Z")
    assert manifest.metadata == {"chunked": True}


def test_download_manifest_from_file(tmp_path):
    file_path = tmp_path / "data.bin"
    file_path.write_bytes(b"abc")

    manifest = DownloadManifest.from_file(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.bin",
        file_path=file_path,
    )

    assert manifest.path == str(file_path)
    assert manifest.sha256 == sha256_bytes(b"abc")
    assert manifest.size_bytes == 3


def test_manifest_json_round_trip(tmp_path):
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )

    path = manifest.write_json(tmp_path / "manifest.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["source_id"] == "ibge"
    assert payload["sha256"] == sha256_bytes(b"abc")


@pytest.mark.parametrize("bad_payload", ["[]", '"just-a-string"', "3", "null", "true"])
def test_download_manifest_read_json_rejects_non_object(tmp_path, bad_payload):
    """A manifest file must be a JSON object — anything else raises ``ValueError``."""
    malformed = tmp_path / "bad.manifest.json"
    malformed.write_text(bad_payload, encoding="utf-8")

    with pytest.raises(ValueError, match="must be an object"):
        DownloadManifest.read_json(malformed)


def test_dataset_manifest_serializes_nested_resources():
    resource = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )

    manifest = DatasetManifest.create(
        source_id="ibge",
        dataset_id="sidra-ipca",
        resources=[resource],
        title="IPCA",
    )
    payload = manifest.to_dict()

    assert payload["title"] == "IPCA"
    assert payload["resources"][0]["url"] == "https://example.test/data.json"


def test_download_manifest_version_default():
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )

    assert manifest.manifest_version == MANIFEST_VERSION


def test_rich_field_groups_default_to_none():
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )

    assert manifest.fingerprint is None
    assert manifest.source_meta is None
    assert manifest.lineage is None
    assert manifest.quality is None
    assert manifest.execution is None


def test_rich_field_groups_round_trip(tmp_path):
    base = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )
    manifest = replace(
        base,
        fingerprint=ContentFingerprint(
            content_type="application/json",
            row_count=120,
            temporal_extent=["2020-01", "2025-04"],
        ),
        source_meta=SourceMetadata(expected_cadence="monthly"),
        lineage=Lineage(derived_from=["deadbeef"], pipeline_id="ipca"),
        quality=QualitySignals(validation_status="passed", null_ratio=0.0),
        execution=ExecutionInfo(duration_ms=842, retry_count=1),
    )

    path = manifest.write_json(tmp_path / "manifest.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["fingerprint"]["row_count"] == 120
    assert payload["fingerprint"]["temporal_extent"] == ["2020-01", "2025-04"]
    assert payload["source_meta"]["expected_cadence"] == "monthly"
    assert payload["lineage"]["derived_from"] == ["deadbeef"]
    assert payload["quality"]["validation_status"] == "passed"
    assert payload["execution"]["duration_ms"] == 842


def test_run_manifest_start_and_finish():
    run = RunManifest.start(
        run_id="run-1",
        source_id="ibge",
        dataset_id="sidra-ipca",
    )
    finished = run.finish()

    assert run.status == "running"
    assert finished.status == "success"
    assert finished.finished_at is not None
    assert finished.started_at == run.started_at


def test_write_manifest_sidecar_naming_and_payload(tmp_path):
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.bin",
        content=b"abc",
    )
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")

    sidecar = write_manifest_sidecar(target, manifest)

    assert sidecar == tmp_path / "data.bin.manifest.json"
    assert sidecar.exists()
    payload = json.loads(sidecar.read_text(encoding="utf-8"))
    assert payload["sha256"] == sha256_bytes(b"abc")
    assert payload["size_bytes"] == 3


def test_write_manifest_sidecar_accepts_str_target_and_no_suffix(tmp_path):
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data",
        content=b"abc",
    )
    target = tmp_path / "data"

    sidecar = write_manifest_sidecar(str(target), manifest)

    assert sidecar == tmp_path / "data.manifest.json"
    assert sidecar.exists()


def test_execution_manifest_alias_is_run_manifest():
    from quantilica.core.manifests import ExecutionManifest, RunManifest

    assert ExecutionManifest is RunManifest


def test_read_manifest_sidecar_roundtrip(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    manifest = DownloadManifest.from_file(
        source_id="s", dataset_id="d", url="http://x", file_path=target
    )
    write_manifest_sidecar(target, manifest)
    loaded = read_manifest_sidecar(target)
    assert loaded is not None
    assert loaded.source_id == "s"
    assert loaded.sha256 == manifest.sha256
    assert loaded.size_bytes == len(b"payload")


def test_read_manifest_sidecar_fills_missing_size(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"12345")
    sidecar = tmp_path / "data.bin.manifest.json"
    sidecar.write_text(json.dumps({"source_id": "s", "dataset_id": "d"}))
    loaded = read_manifest_sidecar(target)
    assert loaded is not None
    assert loaded.size_bytes == 5


def test_read_manifest_sidecar_missing_returns_none(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"x")
    assert read_manifest_sidecar(target) is None


def test_read_manifest_sidecar_corrupt_warns_and_returns_none(tmp_path, caplog):
    target = tmp_path / "data.bin"
    target.write_bytes(b"x")
    (tmp_path / "data.bin.manifest.json").write_text("{not json")
    with caplog.at_level("WARNING"):
        assert read_manifest_sidecar(target) is None
    assert "Invalid manifest" in caplog.text


# --- trusted_sha256 -------------------------------------------------------

FAKE_SHA = "ab" * 32  # not the real digest: proves the sidecar was believed


@pytest.fixture
def reads(monkeypatch):
    """Records every file the fallback hashes (``sha256_file`` is the only reader)."""
    import quantilica.core.manifests as mod

    calls: list[str] = []
    real = mod.sha256_file

    def spy(path, *args, **kwargs):
        calls.append(str(path))
        return real(path, *args, **kwargs)

    monkeypatch.setattr(mod, "sha256_file", spy)
    monkeypatch.delenv("QUANTILICA_LAKE_REHASH", raising=False)
    return calls


def _sidecar(target, *, sha=FAKE_SHA, size=None, mtime_offset=10):
    """Write ``<target>.manifest.json`` and set its mtime after the file's."""
    sidecar = target.parent / (target.name + ".manifest.json")
    payload = {
        "sha256": sha,
        "size_bytes": target.stat().st_size if size is None else size,
    }
    sidecar.write_text(json.dumps(payload), encoding="utf-8")
    base = target.stat().st_mtime
    os.utime(sidecar, (base + mtime_offset, base + mtime_offset))
    return sidecar


def test_trusted_sha256_valid_sidecar_does_not_read_file(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target)
    assert trusted_sha256(target) == FAKE_SHA
    assert reads == []


def test_trusted_sha256_accepts_str_and_uppercase_digest(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target, sha=FAKE_SHA.upper())
    assert trusted_sha256(str(target)) == FAKE_SHA
    assert reads == []


def test_trusted_sha256_without_sidecar_reads_file(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    assert trusted_sha256(target) == sha256_bytes(b"payload")
    assert reads == [str(target)]


def test_trusted_sha256_size_mismatch_reads_file(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target, size=3)
    assert trusted_sha256(target) == sha256_bytes(b"payload")
    assert reads == [str(target)]


def test_trusted_sha256_newer_file_reads_file(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target, mtime_offset=-60)  # file is a minute newer than the sidecar
    assert trusted_sha256(target) == sha256_bytes(b"payload")
    assert reads == [str(target)]


def test_trusted_sha256_tolerates_two_seconds_of_skew(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target, mtime_offset=-1)
    assert trusted_sha256(target) == FAKE_SHA
    assert reads == []


@pytest.mark.parametrize(
    "content",
    [
        "{not json",
        "[]",
        "null",
        json.dumps({"sha256": "abc", "size_bytes": 7}),
        json.dumps({"sha256": FAKE_SHA, "size_bytes": 0}),
        json.dumps({"sha256": FAKE_SHA}),
        json.dumps({"sha256": FAKE_SHA, "size_bytes": "x"}),
        json.dumps({"sha256": 5, "size_bytes": 7}),
    ],
)
def test_trusted_sha256_broken_sidecar_reads_file(tmp_path, reads, content):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    sidecar = _sidecar(target)
    sidecar.write_text(content, encoding="utf-8")
    assert trusted_sha256(target) == sha256_bytes(b"payload")
    assert reads == [str(target)]


def test_trusted_sha256_rehash_env_forces_read(tmp_path, reads, monkeypatch):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target)
    monkeypatch.setenv("QUANTILICA_LAKE_REHASH", "1")
    assert trusted_sha256(target) == sha256_bytes(b"payload")
    assert reads == [str(target)]


@pytest.mark.parametrize("value", ["", "0", "false", "off"])
def test_trusted_sha256_rehash_env_falsey_values_keep_shortcut(
    tmp_path, reads, monkeypatch, value
):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    _sidecar(target)
    monkeypatch.setenv("QUANTILICA_LAKE_REHASH", value)
    assert trusted_sha256(target) == FAKE_SHA
    assert reads == []


def test_trusted_sha256_symlink_with_sidecar_next_to_link(tmp_path, reads):
    store = tmp_path / "store"
    view = tmp_path / "view"
    store.mkdir()
    view.mkdir()
    real = store / "data.bin"
    real.write_bytes(b"payload")
    link = view / "data.bin"
    link.symlink_to(real)
    _sidecar(link)  # sidecar sits beside the link, none beside the target
    assert not (store / "data.bin.manifest.json").exists()
    assert trusted_sha256(link) == FAKE_SHA
    assert reads == []


def test_trusted_sha256_symlink_size_is_the_targets(tmp_path, reads):
    store = tmp_path / "store"
    view = tmp_path / "view"
    store.mkdir()
    view.mkdir()
    real = store / "data.bin"
    real.write_bytes(b"payload")
    link = view / "data.bin"
    link.symlink_to(real)
    # a link's own lstat size is the length of its target path, never 7
    _sidecar(link, size=len(str(real)))
    assert trusted_sha256(link) == sha256_bytes(b"payload")
    assert reads == [str(link)]


def test_trusted_sha256_missing_file_raises(tmp_path, reads):
    from quantilica.core.exceptions import StorageError

    with pytest.raises(StorageError):
        trusted_sha256(tmp_path / "absent.bin")


def test_trusted_sha256_matches_manifest_written_by_core(tmp_path, reads):
    target = tmp_path / "data.bin"
    target.write_bytes(b"payload")
    manifest = DownloadManifest.from_file(
        source_id="s", dataset_id="d", url="http://x", file_path=target
    )
    write_manifest_sidecar(target, manifest)
    sidecar = tmp_path / "data.bin.manifest.json"
    os.utime(sidecar, (target.stat().st_mtime + 5,) * 2)
    reads.clear()  # from_file hashed the file to build the manifest
    assert trusted_sha256(target) == sha256_bytes(b"payload")
    assert reads == []
