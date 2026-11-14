"""Testes do modelo do registro de fetchers."""

from __future__ import annotations

import hashlib
import json

import pytest

from quantilica.core.registry import (
    CADENCIAS,
    DOCS_PASTAS,
    REDISTRIBUICOES,
    REGISTRY_VERSION,
    DatasetEntry,
    FetcherInfo,
    GroupInfo,
    IdentidadePacote,
    RegistroEcossistema,
    RegistryError,
    Renome,
    build_registry,
    carrega_registro_ecossistema,
    check_identidade,
    check_unique,
    distribuicao_esperada,
    is_redistributable,
    registry_sha256,
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
        "redistribuicao",
        "restricao_nota",
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
    assert reg["version"] == 2
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


def test_cadencia_irregular_e_aceita():
    from quantilica.core.registry import CADENCIAS, FetcherInfo

    assert "irregular" in CADENCIAS
    assert (
        FetcherInfo(id="loterias", nome="Loterias", cadencia="irregular").cadencia
        == "irregular"
    )


def test_redistribuicao_vocabulario_e_validacao():
    assert REDISTRIBUICOES == ("aberta", "restrita")
    assert _info().redistribuicao is None
    for valor in REDISTRIBUICOES:
        assert _info(redistribuicao=valor).redistribuicao == valor
    with pytest.raises(RegistryError, match="redistribuição"):
        _info(redistribuicao="fechada")
    with pytest.raises(RegistryError, match="restricao_nota"):
        _info(restricao_nota=" ")


def test_is_redistributable():
    assert is_redistributable(_info()) is True
    assert is_redistributable(_info(redistribuicao="aberta")) is True
    assert is_redistributable(_info(redistribuicao="restrita")) is False


def test_redistribuicao_ida_e_volta_e_dict_antigo():
    info = _info(redistribuicao="restrita", restricao_nota="termos vedam coleta")
    data = json.loads(json.dumps(info.to_dict()))
    assert data["redistribuicao"] == "restrita"
    assert FetcherInfo.from_dict(data) == info
    antigo = {"id": "x", "nome": "X", "aliases": []}
    assert FetcherInfo.from_dict(antigo).redistribuicao is None


# --- Registro v2: identidade de empacotamento, ecossistema e sha256 -----------


def _ident(dist="ibge-sidra-fetcher", **kw) -> IdentidadePacote:
    kw.setdefault("module", dist.replace("-", "_"))
    kw.setdefault("repo", f"https://github.com/Quantilica/{dist}")
    kw.setdefault("entry_point", "ibge-sidra")
    kw.setdefault("version", "0.12.0")
    return IdentidadePacote(distribution=dist, **kw)


def _escreve(tmp_path, texto: str):
    p = tmp_path / "registro.toml"
    p.write_text(texto, encoding="utf-8")
    return p


TOML_OK = """
[ibge-sidra]
docs_pasta = "ibge"
vitrine = true
ci_integracao = true
json_leve = false
mascote = "gata"
health_url = "https://servicodados.ibge.gov.br/api/v3/calendario"

[[ibge-sidra.historico]]
distribution = "sidra-fetcher"
repo = "Quantilica/sidra-fetcher"
id = "sidra"
data = 2026-11-13

[anp]
docs_pasta = "petroleo"
"""


def test_versao_do_registro_e_2():
    assert REGISTRY_VERSION == 2
    assert build_registry([_info()])["version"] == 2


def test_identidade_to_dict_ordem_e_campos():
    assert list(_ident().to_dict()) == [
        "distribution",
        "module",
        "repo",
        "entry_point",
        "lake_entry_point",
        "source_id",
        "lake_source_id",
        "version",
    ]
    assert IdentidadePacote().to_dict()["distribution"] is None


def test_carrega_registro_ecossistema_ok(tmp_path):
    reg = carrega_registro_ecossistema(_escreve(tmp_path, TOML_OK))
    assert set(reg) == {"ibge-sidra", "anp"}
    sidra = reg["ibge-sidra"]
    assert sidra.docs_pasta == "ibge" and sidra.vitrine and sidra.ci_integracao
    assert sidra.json_leve is False and sidra.mascote == "gata"
    assert sidra.historico == (
        Renome("sidra-fetcher", "Quantilica/sidra-fetcher", "sidra", "2026-11-13"),
    )
    anp = reg["anp"]
    assert (anp.vitrine, anp.ci_integracao, anp.json_leve) == (False, False, False)
    assert anp.mascote is None and anp.health_url is None and anp.historico == ()


def test_carrega_aceita_ids_conhecidos(tmp_path):
    p = _escreve(tmp_path, TOML_OK)
    assert carrega_registro_ecossistema(p, ids=["anp", "ibge-sidra", "x"])
    with pytest.raises(RegistryError, match="não está no export.*ibge-sidra"):
        carrega_registro_ecossistema(p, ids=["anp"])


@pytest.mark.parametrize(
    ("trecho", "msg"),
    [
        ('docs_pasta = "nao-existe"', "fora do vocabulário"),
        ('docs_pasta = "ibge"\nhealth_url = "ftp://x.org"', "URL http"),
        ('docs_pasta = "ibge"\nhealth_url = "http://"', "URL http"),
        ('docs_pasta = "ibge"\nhealth_url = "calendario"', "URL http"),
        ('docs_pasta = "ibge"\nvitrine = "sim"', "true ou false"),
        ('docs_pasta = "ibge"\nmascote = ""', "mascote"),
        ('docs_pasta = "ibge"\nfoo = 1', "campos desconhecidos"),
        ("vitrine = true", "docs_pasta é obrigatório"),
    ],
)
def test_carrega_valida_secao(tmp_path, trecho, msg):
    p = _escreve(tmp_path, f"[ibge-sidra]\n{trecho}\n")
    with pytest.raises(RegistryError, match=msg):
        carrega_registro_ecossistema(p)


def test_carrega_valida_historico(tmp_path):
    base = '[x]\ndocs_pasta = "ibge"\n[[x.historico]]\n'
    ok = (
        'distribution = "a-fetcher"\nrepo = "Quantilica/a"\nid = "a"\n'
        'data = "2026-01-02"\n'
    )
    assert (
        carrega_registro_ecossistema(_escreve(tmp_path, base + ok))["x"]
        .historico[0]
        .data
        == "2026-01-02"
    )
    with pytest.raises(RegistryError, match="data deve ser ISO"):
        carrega_registro_ecossistema(
            _escreve(tmp_path, base + ok.replace("2026-01-02", "13/11/2026"))
        )
    with pytest.raises(RegistryError, match="campos ausentes"):
        carrega_registro_ecossistema(
            _escreve(tmp_path, base + 'distribution = "a-fetcher"\n')
        )
    with pytest.raises(RegistryError, match="repo deve ser"):
        carrega_registro_ecossistema(
            _escreve(tmp_path, base + ok.replace("Quantilica/a", "sem barra"))
        )
    with pytest.raises(RegistryError, match="repetida"):
        carrega_registro_ecossistema(
            _escreve(tmp_path, base + ok + "[[x.historico]]\n" + ok)
        )


def test_carrega_toml_e_arquivo_invalidos(tmp_path):
    with pytest.raises(RegistryError, match="TOML inválido"):
        carrega_registro_ecossistema(_escreve(tmp_path, "[x\n"))
    with pytest.raises(RegistryError, match="ilegível"):
        carrega_registro_ecossistema(tmp_path / "nao-existe.toml")
    with pytest.raises(RegistryError, match="id inválido"):
        carrega_registro_ecossistema(
            _escreve(tmp_path, '[Bad_Id]\ndocs_pasta = "ibge"')
        )
    with pytest.raises(RegistryError, match="deve ser uma seção"):
        carrega_registro_ecossistema(_escreve(tmp_path, "x = 1"))


def test_vocabulario_de_docs_pasta_sem_duplicatas():
    assert len(set(DOCS_PASTAS)) == len(DOCS_PASTAS) and "ibge" in DOCS_PASTAS


def test_build_registry_junta_as_tres_camadas(tmp_path):
    eco = carrega_registro_ecossistema(_escreve(tmp_path, TOML_OK))
    infos = [_info("ibge-sidra", nome="IBGE SIDRA", aliases=("sidra",)), _info("anp")]
    ident = {"ibge-sidra": _ident(), "anp": _ident("anp-fetcher", entry_point="anp")}
    reg = build_registry(infos, ident, eco, extras={"anp": {"lake": True}})
    assert reg["version"] == 2
    anp, sidra = reg["fetchers"]
    assert (anp["id"], sidra["id"]) == ("anp", "ibge-sidra")
    # camada 1 (v1 intacta), camada 2 e camada 3 no mesmo nível
    assert sidra["aliases"] == ["sidra"] and sidra["nome"] == "IBGE SIDRA"
    assert sidra["distribution"] == "ibge-sidra-fetcher"
    assert sidra["module"] == "ibge_sidra_fetcher" and sidra["version"] == "0.12.0"
    assert sidra["repo"].endswith("/ibge-sidra-fetcher")
    assert sidra["docs_pasta"] == "ibge" and sidra["vitrine"] is True
    assert sidra["historico"] == [
        {
            "distribution": "sidra-fetcher",
            "repo": "Quantilica/sidra-fetcher",
            "id": "sidra",
            "data": "2026-11-13",
        }
    ]
    assert anp["lake"] is True and anp["historico"] == []
    assert json.loads(json.dumps(reg)) == reg


def test_build_registry_v1_continua_sendo_subconjunto_do_v2():
    infos = [_info("anp"), _info("b3", aliases=("bolsa",))]
    simples = build_registry(infos)
    completo = build_registry(infos, {"anp": _ident("anp-fetcher")}, {})
    for a, b in zip(simples["fetchers"], completo["fetchers"], strict=True):
        assert {k: b[k] for k in a} == a


def test_build_registry_fetcher_sem_secao_sai_com_campos_vazios(tmp_path):
    eco = carrega_registro_ecossistema(_escreve(tmp_path, TOML_OK))
    eco = {"anp": eco["anp"]}
    reg = build_registry([_info("anp"), _info("b3")], ecossistema=eco)
    b3 = reg["fetchers"][1]
    assert b3["docs_pasta"] is None and b3["vitrine"] is False
    assert b3["historico"] == [] and "distribution" not in b3


def test_build_registry_id_do_ecossistema_fora_do_export_e_erro(tmp_path):
    eco = carrega_registro_ecossistema(_escreve(tmp_path, TOML_OK))
    with pytest.raises(RegistryError, match="não está no export.*ibge-sidra"):
        build_registry([_info("anp")], ecossistema=eco)
    with pytest.raises(RegistryError, match="identidades com id"):
        build_registry([_info("anp")], identidades={"zzz": _ident()})


def test_build_registry_sha256_do_conteudo(tmp_path):
    infos = [_info("b3"), _info("anp")]
    a = build_registry(infos, {"anp": _ident("anp-fetcher")})
    b = build_registry(list(reversed(infos)), {"anp": _ident("anp-fetcher")})
    assert a["sha256"] == b["sha256"] == registry_sha256(a)
    assert len(a["sha256"]) == 64
    # o hash cobre o conteúdo; a própria chave sha256 fica de fora
    adulterado = json.loads(json.dumps(a))
    adulterado["fetchers"][0]["nome"] = "outro"
    assert registry_sha256(adulterado) != a["sha256"]
    mudou = build_registry(infos, {"anp": _ident("anp-fetcher", version="9.9.9")})
    assert mudou["sha256"] != a["sha256"]
    # independe da ordem das chaves
    embaralhado = {"fetchers": a["fetchers"], "version": a["version"]}
    assert registry_sha256(embaralhado) == a["sha256"]


def test_distribuicao_esperada():
    assert distribuicao_esperada("ibge-sidra") == "ibge-sidra-fetcher"


def test_check_identidade_regra_e_historico(tmp_path):
    infos = [_info("anp"), _info("foo")]
    ident = {"anp": _ident("anp-fetcher"), "foo": _ident("bar-fetcher")}
    problemas = check_identidade(infos, ident)
    assert (
        len(problemas) == 1 and "foo" in problemas[0] and "bar-fetcher" in problemas[0]
    )
    eco = {
        "foo": RegistroEcossistema(
            "ibge",
            historico=(
                Renome("bar-fetcher", "Quantilica/bar-fetcher", "bar", "2026-11-14"),
            ),
        )
    }
    assert check_identidade(infos, ident, eco) == []
    # histórico de outra distribuição não explica
    eco["foo"] = RegistroEcossistema(
        "ibge",
        historico=(Renome("baz-fetcher", "Quantilica/baz", "baz", "2026-11-14"),),
    )
    assert len(check_identidade(infos, ident, eco)) == 1
    # sem distribuição conhecida não há o que checar
    assert check_identidade([_info("zed")], {}) == []


def test_sha256_golden_do_json_canonico():
    """Trava o formato canônico: mudar a serialização muda o hash publicado."""
    doc = {
        "version": 2,
        "fetchers": [
            {
                "id": "anp",
                "nome": "ANP – Dados",
                "aliases": ["a"],
                "vitrine": True,
                "historico": [],
            }  # fmt: skip
        ],
    }
    canon = (
        '{"fetchers":[{"aliases":["a"],"historico":[],"id":"anp",'
        '"nome":"ANP – Dados","vitrine":true}],"version":2}'
    )
    assert hashlib.sha256(canon.encode()).hexdigest() == (
        "f2ec0f20bdda37e4ce7b1fc0cdc7ff40407fa614aee4b8210e05248aed818537"
    )
    assert registry_sha256(doc) == (
        "f2ec0f20bdda37e4ce7b1fc0cdc7ff40407fa614aee4b8210e05248aed818537"
    )
    # o campo sha256 do próprio documento não entra no hash
    assert registry_sha256({**doc, "sha256": "x"}) == registry_sha256(doc)
