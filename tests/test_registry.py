"""Testes do modelo do registro de fetchers."""

from __future__ import annotations

import json

import pytest

from quantilica.core.registry import (
    CADENCIAS,
    DatasetEntry,
    FetcherInfo,
    GroupInfo,
    RegistryError,
    build_registry,
    check_unique,
    resolve_alias,
)


def _info(id_: str = "tesouro-direto", **kw) -> FetcherInfo:
    kw.setdefault("nome", "Tesouro Direto")
    return FetcherInfo(id_, **kw)


def test_minimo_valido():
    info = _info()
    assert info.aliases == ()
    assert info.missing_fields() == ["orgao", "licenca", "homepage", "cadencia"]


@pytest.mark.parametrize("bad", ["", "TD", "a_b", "-a", "a-", "a--b", "a b", "á"])
def test_id_invalido(bad):
    with pytest.raises(RegistryError, match="id inválido"):
        _info(bad)


def test_nome_obrigatorio():
    with pytest.raises(RegistryError, match="nome"):
        FetcherInfo("x", nome=" ")


def test_apelidos_validacao():
    assert _info(aliases=["td", "bcb_microdados"]).aliases == ("td", "bcb_microdados")
    with pytest.raises(RegistryError, match="repete o id"):
        _info(aliases=("tesouro-direto",))
    with pytest.raises(RegistryError, match="repetido"):
        _info(aliases=("td", "td"))
    with pytest.raises(RegistryError, match="apelido inválido"):
        _info(aliases=("TD",))


def test_cadencia_e_homepage():
    for c in CADENCIAS:
        assert _info(cadencia=c).cadencia == c
    with pytest.raises(RegistryError, match="cadência"):
        _info(cadencia="hourly")
    with pytest.raises(RegistryError, match="http"):
        _info(homepage="www.x.gov.br")
    with pytest.raises(RegistryError, match="orgao"):
        _info(orgao="")


def test_ida_e_volta_json():
    info = _info(
        aliases=("td", "tddata"),
        orgao="STN",
        licenca="CC-BY-4.0",
        homepage="https://www.tesourotransparente.gov.br",
        cadencia="monthly",
        dominio="fiscal",
    )
    raw = json.dumps(info.to_dict(), sort_keys=True)
    assert FetcherInfo.from_dict(json.loads(raw)) == info
    assert list(info.to_dict()) == [
        "id",
        "nome",
        "aliases",
        "orgao",
        "licenca",
        "homepage",
        "cadencia",
        "dominio",
    ]


def test_from_dict_campo_desconhecido():
    with pytest.raises(RegistryError, match="desconhecidos"):
        FetcherInfo.from_dict({"id": "x", "nome": "X", "foo": 1})
    with pytest.raises(RegistryError):
        FetcherInfo.from_dict({"id": "x"})


def test_resolve_alias():
    td = _info(aliases=("td",))
    bcb = _info("bcb-sgs", nome="SGS", aliases=("bcb",))
    assert resolve_alias([td, bcb], "td") is td
    assert resolve_alias([td, bcb], "bcb-sgs") is bcb
    assert resolve_alias([td, bcb], "nada") is None


def test_check_unique():
    a = _info("a", nome="A", aliases=("x",))
    b = _info("b", nome="B", aliases=("y",))
    check_unique([a, b])
    with pytest.raises(RegistryError, match="'a' em a, c"):
        check_unique([a, b, _info("c", nome="C", aliases=("a",))])
    with pytest.raises(RegistryError, match="'x' em a, b"):
        check_unique([a, _info("b", nome="B", aliases=("x",))])
    with pytest.raises(RegistryError, match="'a' em a, a"):
        check_unique([a, a])


def test_build_registry_ordenado_e_json():
    z = _info("z", nome="Z")
    a = _info("a", nome="A", aliases=("aa",))
    reg = build_registry([z, a])
    assert reg["version"] == 1
    assert [f["id"] for f in reg["fetchers"]] == ["a", "z"]
    assert json.loads(json.dumps(reg)) == reg
    with pytest.raises(RegistryError):
        build_registry([a, a])


def test_dataset_entry_e_group():
    e = DatasetEntry(
        id="x_2024", base_id="x", name="X", url="u", ext="csv", group="g", year=2024
    )
    d = e.to_dict()
    assert d["year"] == 2024 and d["semester"] is None
    assert DatasetEntry.from_dict({**d, "extra": 1}) == e
    g = GroupInfo("g", [e])
    assert g.to_dict() == {"name": "g", "entries": [d]}
