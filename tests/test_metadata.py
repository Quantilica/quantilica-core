import json

import pytest

from quantilica_core.exceptions import MetadataError
from quantilica_core.metadata import (
    Dataset,
    Dimension,
    IngestionRun,
    MetadataCatalog,
    Period,
    Resource,
    Series,
    Source,
    Territory,
    Variable,
    build_simple_catalog,
    validate_id,
)


def test_validate_id_accepts_stable_identifiers():
    assert validate_id("ibge.sidra:7060") == "ibge.sidra:7060"


@pytest.mark.parametrize("value", ["", "with space", "../x", "/x"])
def test_validate_id_rejects_invalid_identifiers(value):
    with pytest.raises(MetadataError):
        validate_id(value)


def test_source_serializes_to_json(tmp_path):
    source = Source(
        id="ibge",
        name="IBGE",
        homepage_url="https://www.ibge.gov.br",
        metadata={"country": "BR"},
    )

    path = source.write_json(tmp_path / "source.json")
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["id"] == "ibge"
    assert payload["metadata"]["country"] == "BR"


def test_dataset_resource_variable_dimension_series_models():
    dataset = Dataset(id="sidra-ipca", source_id="ibge", name="IPCA")
    resource = Resource(
        id="7060",
        dataset_id="sidra-ipca",
        name="Tabela 7060",
        format="json",
    )
    variable = Variable(
        id="variacao_mensal",
        dataset_id="sidra-ipca",
        name="Variação mensal",
        unit="%",
    )
    dimension = Dimension(
        id="grupo",
        dataset_id="sidra-ipca",
        name="Grupo",
        values=["alimentacao"],
    )
    territory = Territory(id="br", name="Brasil", level="country")
    series = Series(
        id="ipca.br.variacao_mensal",
        dataset_id="sidra-ipca",
        name="IPCA Brasil - variação mensal",
        variable_id="variacao_mensal",
        territory_id="br",
        dimensions={"grupo": "alimentacao"},
        unit="%",
    )
    period = Period(id="2026-05", label="Maio de 2026", frequency="monthly")

    assert dataset.source_id == "ibge"
    assert resource.dataset_id == dataset.id
    assert variable.unit == "%"
    assert dimension.values == ["alimentacao"]
    assert territory.level == "country"
    assert series.dimensions["grupo"] == "alimentacao"
    assert period.frequency == "monthly"


def test_ingestion_run_start_and_finish():
    run = IngestionRun.start(
        id="run-1",
        source_id="ibge",
        dataset_id="sidra-ipca",
        resource_ids=["7060"],
    )
    finished = run.finish(status="success")

    assert run.status == "running"
    assert run.started_at.endswith("Z")
    assert finished.status == "success"
    assert finished.finished_at is not None
    assert finished.resource_ids == ["7060"]


def test_ingestion_run_rejects_invalid_status():
    with pytest.raises(MetadataError):
        IngestionRun(id="run-1", started_at="2026-05-09T00:00:00Z", status="done")


def test_metadata_catalog_validates_references():
    catalog = MetadataCatalog(
        sources=[Source(id="ibge", name="IBGE")],
        datasets=[Dataset(id="sidra-ipca", source_id="ibge", name="IPCA")],
        resources=[Resource(id="7060", dataset_id="sidra-ipca", name="Tabela 7060")],
        variables=[
            Variable(
                id="variacao_mensal",
                dataset_id="sidra-ipca",
                name="Variação mensal",
            )
        ],
        dimensions=[Dimension(id="grupo", dataset_id="sidra-ipca", name="Grupo")],
        territories=[Territory(id="br", name="Brasil")],
        series=[
            Series(
                id="ipca.br",
                dataset_id="sidra-ipca",
                name="IPCA Brasil",
                variable_id="variacao_mensal",
                territory_id="br",
            )
        ],
    )

    catalog.validate_references()


def test_metadata_catalog_rejects_unknown_source_reference():
    catalog = MetadataCatalog(
        datasets=[Dataset(id="sidra-ipca", source_id="ibge", name="IPCA")],
    )

    with pytest.raises(MetadataError):
        catalog.validate_references()


def test_build_simple_catalog_returns_validated_catalog():
    source = Source(id="tesouro-nacional", name="Tesouro Nacional")
    dataset = Dataset(
        id="rtn",
        source_id="tesouro-nacional",
        name="Resultado do Tesouro Nacional",
    )
    resources = [
        Resource(id=f"rtn-{n}", dataset_id="rtn", name=f"file-{n}.xlsx")
        for n in range(3)
    ]

    catalog = build_simple_catalog(source, dataset, resources)

    assert catalog.sources == [source]
    assert catalog.datasets == [dataset]
    assert len(catalog.resources) == 3


def test_build_simple_catalog_rejects_dataset_with_wrong_source():
    source = Source(id="ibge", name="IBGE")
    dataset = Dataset(id="rtn", source_id="tesouro-nacional", name="RTN")

    with pytest.raises(MetadataError):
        build_simple_catalog(source, dataset, [])


def test_build_simple_catalog_rejects_resource_with_wrong_dataset():
    source = Source(id="ibge", name="IBGE")
    dataset = Dataset(id="sidra", source_id="ibge", name="SIDRA")
    bad = Resource(id="r1", dataset_id="other", name="r1")

    with pytest.raises(MetadataError):
        build_simple_catalog(source, dataset, [bad])


def test_metadata_catalog_rejects_unknown_series_variable_reference():
    catalog = MetadataCatalog(
        sources=[Source(id="ibge", name="IBGE")],
        datasets=[Dataset(id="sidra-ipca", source_id="ibge", name="IPCA")],
        series=[
            Series(
                id="ipca.br",
                dataset_id="sidra-ipca",
                name="IPCA Brasil",
                variable_id="missing",
            )
        ],
    )

    with pytest.raises(MetadataError):
        catalog.validate_references()
