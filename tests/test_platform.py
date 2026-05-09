import pytest

from quantilica_core.exceptions import MetadataError
from quantilica_core.manifests import DownloadManifest
from quantilica_core.metadata import (
    Dataset,
    IngestionRun,
    MetadataCatalog,
    Resource,
    Source,
)
from quantilica_core.platform import (
    CatalogIndex,
    CatalogRepository,
    catalog_from_dict,
    download_manifest_from_dict,
    ingestion_run_from_dict,
)
from quantilica_core.storage import LocalStorage


def sample_catalog() -> MetadataCatalog:
    return MetadataCatalog(
        sources=[Source(id="ibge", name="IBGE")],
        datasets=[
            Dataset(
                id="sidra-ipca",
                source_id="ibge",
                name="IPCA",
                description="Índice de preços ao consumidor",
            )
        ],
        resources=[
            Resource(
                id="sidra-7060",
                dataset_id="sidra-ipca",
                name="Tabela 7060",
                format="json",
            )
        ],
    )


def test_catalog_index_lookups_and_search():
    index = CatalogIndex(sample_catalog())

    assert index.sources_by_id["ibge"].name == "IBGE"
    assert index.datasets_by_id["sidra-ipca"].name == "IPCA"
    assert index.resources_by_id["sidra-7060"].format == "json"
    assert index.datasets_for_source("ibge")[0].id == "sidra-ipca"
    assert index.resources_for_dataset("sidra-ipca")[0].id == "sidra-7060"
    assert index.search_datasets("preços")[0].id == "sidra-ipca"
    assert index.search_datasets("") == []


def test_catalog_index_rejects_duplicate_ids():
    catalog = MetadataCatalog(
        sources=[Source(id="ibge", name="IBGE")],
        datasets=[
            Dataset(id="sidra-ipca", source_id="ibge", name="IPCA"),
            Dataset(id="sidra-ipca", source_id="ibge", name="IPCA duplicado"),
        ],
    )

    with pytest.raises(MetadataError):
        CatalogIndex(catalog)


def test_catalog_repository_publish_and_load_catalog(tmp_path):
    repository = CatalogRepository(LocalStorage(tmp_path))
    catalog = sample_catalog()

    stat = repository.publish_catalog(catalog)
    loaded = repository.load_catalog()
    index = repository.load_index()

    assert stat.key == "catalog/catalog.json"
    assert loaded.datasets[0].id == "sidra-ipca"
    assert index.resources_for_dataset("sidra-ipca")[0].id == "sidra-7060"


def test_catalog_repository_write_and_list_runs(tmp_path):
    repository = CatalogRepository(LocalStorage(tmp_path))
    run = IngestionRun.start(
        id="run-1",
        source_id="ibge",
        dataset_id="sidra-ipca",
        resource_ids=["sidra-7060"],
    ).finish()

    stat = repository.write_run(run)
    runs = repository.list_runs()

    assert stat.key == "runs/run-1.json"
    assert len(runs) == 1
    assert runs[0].id == "run-1"
    assert runs[0].status == "success"


def test_catalog_repository_write_and_list_download_manifests(tmp_path):
    repository = CatalogRepository(LocalStorage(tmp_path))
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        resource_id="sidra-7060",
        url="https://example.test/data.json",
        content=b"abc",
    )

    stat = repository.write_download_manifest(manifest)
    manifests = repository.list_download_manifests()

    assert stat.key == "manifests/downloads/ibge/sidra-ipca/sidra-7060.json"
    assert len(manifests) == 1
    assert manifests[0].resource_id == "sidra-7060"


def test_catalog_repository_write_download_manifest_with_custom_key(tmp_path):
    repository = CatalogRepository(LocalStorage(tmp_path))
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )

    stat = repository.write_download_manifest(
        manifest,
        key="custom/download.json",
    )

    assert stat.key == "custom/download.json"
    assert repository.list_download_manifests() == []


def test_catalog_from_dict_round_trip():
    catalog = sample_catalog()
    loaded = catalog_from_dict(catalog.to_dict())

    assert loaded.sources[0].id == "ibge"
    assert loaded.datasets[0].id == "sidra-ipca"


def test_run_and_manifest_from_dict_round_trip():
    run = IngestionRun.start(id="run-1")
    manifest = DownloadManifest.from_content(
        source_id="ibge",
        dataset_id="sidra-ipca",
        url="https://example.test/data.json",
        content=b"abc",
    )

    assert ingestion_run_from_dict(run.to_dict()).id == "run-1"
    assert download_manifest_from_dict(manifest.to_dict()).sha256 == manifest.sha256
