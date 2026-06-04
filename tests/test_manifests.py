import json
from dataclasses import replace

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
)


def test_download_manifest_from_content():
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        resource_id="7060",
        url="https://example.test/data.json",
        content=b"abc",
        producer="sidra-fetcher",
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
