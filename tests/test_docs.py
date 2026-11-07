"""Testes de quantilica.core.docs (norma de documentação dos dados)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from quantilica.core.docs import (
    DocFormatError,
    assert_fetcher_docs,
    check_fetcher_docs,
    parse_dataset_page,
    parse_variables_table,
    schema_from_lake,
    update_ficha,
    write_schema,
)

SECTIONS = [
    "Resumo",
    "Ficha técnica",
    "Origem e coleta",
    "Conteúdo",
    "Variáveis",
    "No lake",
    "Cuidados de uso",
    "Exemplos",
    "Referências",
]

HEADER = """\
---
dataset:
  id: {id}
  fonte: {fonte}
  titulo: Vendas
  resumo: Vendas mensais.
  orgao: Órgão X
  pagina_oficial: https://example.org/vendas
  licenca: CC-BY 4.0 (https://example.org/licenca)
  periodicidade: mensal
  cobertura: {{inicio: 2020-01, fim: {fim}}}
  unidade: uma venda
  granularidade: {{espacial: Brasil, temporal: mês}}
  formato_bruto: CSV, latin-1, ponto e vírgula
  lake: {{estado: {estado}, particoes: {particoes}, chave: [id_venda], colunas: 3}}
  unidades_de_medida: {unidades}
  dados_pessoais: nenhum
  eras:
    - {{desde: 2022-01, mudanca: "novo layout"}}
  revisado_em: 2026-11-07
  versao_fetcher: 1.2.3
---
"""

VARS = """\
| Coluna no lake | Na fonte | Tipo | Descrição | Unidade / códigos | Período |
|---|---|---|---|---|---|
| `id_venda` | ID | int | id | | |
| `valor` | VLR | double | valor | R$ | |
| `uf` | UF | string | estado | | |
"""


def page(
    *,
    id: str = "vendas",
    fonte: str = "alfa",
    fim: str = "em-curso",
    estado: str = "convertido",
    particoes: str = "[ano, mes]",
    unidades: str = "{valor: R$}",
    variables: str = VARS,
    sections: list[str] | None = None,
    marker: bool = True,
) -> str:
    text = HEADER.format(
        id=id,
        fonte=fonte,
        fim=fim,
        estado=estado,
        particoes=particoes,
        unidades=unidades,
    )
    text += "\n# Vendas\n\n"
    for title in sections or SECTIONS:
        text += f"## {title}\n\n"
        if title == "Ficha técnica" and marker:
            text += "<!-- gerado:ficha -->\n\n"
        if title == "Variáveis":
            text += variables + "\n"
        if title == "Exemplos":
            text += "```sql\n## nao e secao\nselect 1;\n```\n\n"
    return text


SCHEMA = {
    "fonte": "alfa",
    "dataset": "vendas",
    "gerado_em": "2026-11-07T00:00:00+00:00",
    "particoes": ["ano", "mes"],
    "colunas": [
        {"nome": "id_venda", "tipos": ["int64"]},
        {"nome": "valor", "tipos": ["double"]},
        {"nome": "uf", "tipos": ["string"]},
    ],
    "arquivos": 2,
}


def make_docs(
    root: Path,
    text: str | None = None,
    schema: dict | None = SCHEMA,
    readme: bool = True,
) -> Path:
    docs = root / "docs"
    (docs / "datasets").mkdir(parents=True)
    if readme:
        (docs / "README.md").write_text("# Alfa\n", encoding="utf-8")
    (docs / "datasets" / "vendas.md").write_text(text or page(), encoding="utf-8")
    if schema is not None:
        (docs / "datasets" / "vendas.schema.json").write_text(
            json.dumps(schema), encoding="utf-8"
        )
    return docs


def messages(problems, severity="error") -> str:
    return "\n".join(p.message for p in problems if p.severity == severity)


# --- caminho feliz -------------------------------------------------------------


def test_valid_docs_have_no_problems(tmp_path: Path) -> None:
    docs = make_docs(tmp_path)
    assert check_fetcher_docs(docs, ["vendas"], "alfa") == []
    assert_fetcher_docs(docs, ["vendas"], "alfa")


def test_parse_header_and_sections(tmp_path: Path) -> None:
    docs = make_docs(tmp_path)
    header, sections = parse_dataset_page(docs / "datasets" / "vendas.md")
    assert header.id == "vendas"
    assert header.cobertura_inicio == "2020-01"
    assert header.cobertura_fim == "em-curso"
    assert header.lake_particoes == ["ano", "mes"]
    assert header.revisado_em == "2026-11-07"
    assert header.dados_pessoais is None
    assert header.unidades_de_medida == {"valor": "R$"}
    assert sections == SECTIONS  # '## nao e secao' dentro do bloco de código é ignorado


def test_variables_table_strips_backticks(tmp_path: Path) -> None:
    assert parse_variables_table(page()) == ["id_venda", "valor", "uf"]


# --- regras de erro -------------------------------------------------------------


def test_missing_page_and_orphan_page(tmp_path: Path) -> None:
    docs = make_docs(tmp_path)
    out = messages(check_fetcher_docs(docs, ["vendas", "outro"], "alfa"))
    assert "dataset declarado sem página" in out
    out = messages(check_fetcher_docs(docs, [], "alfa"))
    assert "página sem dataset correspondente" in out


def test_missing_readme(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, readme=False)
    assert "página da fonte ausente" in messages(
        check_fetcher_docs(docs, ["vendas"], "alfa")
    )


def test_id_and_fonte_must_match(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(id="outro", fonte="beta"))
    out = messages(check_fetcher_docs(docs, ["vendas"], "alfa"))
    assert "id 'outro'" in out
    assert "fonte 'beta'" in out


@pytest.mark.parametrize(
    ("kwargs", "needle"),
    [
        ({"estado": "pronto"}, "lake.estado"),
        ({"fim": "ontem"}, "cobertura.fim"),
        ({"fim": "2024-13"}, "cobertura.fim"),
    ],
)
def test_invalid_header_values(tmp_path: Path, kwargs, needle) -> None:
    docs = make_docs(tmp_path, page(**kwargs))
    assert needle in messages(check_fetcher_docs(docs, ["vendas"], "alfa"))


def test_missing_required_field_and_bad_dates(tmp_path: Path) -> None:
    text = page().replace("  licenca: CC-BY 4.0 (https://example.org/licenca)\n", "")
    text = text.replace("revisado_em: 2026-11-07", "revisado_em: ontem")
    text = text.replace("dados_pessoais: nenhum", "dados_pessoais: [x]")
    docs = make_docs(tmp_path, text)
    out = messages(check_fetcher_docs(docs, ["vendas"], "alfa"))
    assert "campo obrigatório ausente: licenca" in out
    assert "revisado_em" in out
    assert "dados_pessoais" in out


def test_dados_pessoais_list_is_valid(tmp_path: Path) -> None:
    text = page().replace(
        "dados_pessoais: nenhum",
        "dados_pessoais:\n    - {coluna: nome, categoria: nome}",
    )
    path = tmp_path / "p.md"
    path.write_text(text, encoding="utf-8")
    header, _ = parse_dataset_page(path)
    assert header.dados_pessoais == [{"coluna": "nome", "categoria": "nome"}]


def test_no_front_matter(tmp_path: Path) -> None:
    path = tmp_path / "p.md"
    path.write_text("# sem cabeçalho\n", encoding="utf-8")
    with pytest.raises(DocFormatError):
        parse_dataset_page(path)


def test_missing_section(tmp_path: Path) -> None:
    secs = [s for s in SECTIONS if s != "Exemplos"]
    docs = make_docs(tmp_path, page(sections=secs))
    assert "seções ausentes: Exemplos" in messages(
        check_fetcher_docs(docs, ["vendas"], "alfa")
    )


def test_sections_out_of_order(tmp_path: Path) -> None:
    secs = SECTIONS.copy()
    secs[0], secs[2] = secs[2], secs[0]
    docs = make_docs(tmp_path, page(sections=secs))
    assert "fora de ordem" in messages(check_fetcher_docs(docs, ["vendas"], "alfa"))


def test_missing_marker(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(marker=False))
    assert "gerado:ficha" in messages(check_fetcher_docs(docs, ["vendas"], "alfa"))


def test_convertido_requires_schema(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, schema=None)
    assert "sem esquema" in messages(check_fetcher_docs(docs, ["vendas"], "alfa"))


def test_so_bruto_does_not_need_schema(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(estado="so-bruto", particoes="null"), schema=None)
    text = (docs / "datasets" / "vendas.md").read_text(encoding="utf-8")
    text = text.replace("chave: [id_venda], colunas: 3", "chave: null, colunas: null")
    (docs / "datasets" / "vendas.md").write_text(text, encoding="utf-8")
    assert check_fetcher_docs(docs, ["vendas"], "alfa") == []


def test_partitions_must_match_schema(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(particoes="[ano]"))
    assert "partições" in messages(check_fetcher_docs(docs, ["vendas"], "alfa"))


def test_dictionary_must_cover_schema_exactly(tmp_path: Path) -> None:
    variables = VARS.replace("| `uf` | UF | string | estado | | |\n", "")
    variables += "| `fantasma` | X | int | x | | |\n"
    docs = make_docs(tmp_path, page(variables=variables))
    out = messages(check_fetcher_docs(docs, ["vendas"], "alfa"))
    assert "fora do dicionário: uf" in out
    assert "não estão no esquema: fantasma" in out


def test_partition_columns_may_be_in_dictionary_or_not(tmp_path: Path) -> None:
    variables = VARS + "| `ano` | A | int | ano | | |\n"
    docs = make_docs(tmp_path, page(variables=variables))
    assert check_fetcher_docs(docs, ["vendas"], "alfa") == []


def test_dictionary_without_table(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(variables="sem tabela\n"))
    assert "sem tabela" in messages(check_fetcher_docs(docs, ["vendas"], "alfa"))


def test_units_warning_for_numeric_non_key_columns(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(unidades="null"))
    problems = check_fetcher_docs(docs, ["vendas"], "alfa")
    assert messages(problems) == ""
    assert "unidades_de_medida" in messages(problems, "warning")
    assert "valor" in messages(problems, "warning")
    assert "id_venda" not in messages(problems, "warning")  # é chave
    assert_fetcher_docs(docs, ["vendas"], "alfa")  # aviso não derruba


def test_assert_lists_problems(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, readme=False)
    with pytest.raises(AssertionError, match="página da fonte ausente"):
        assert_fetcher_docs(docs, ["vendas"], "alfa")


# --- ficha ----------------------------------------------------------------------


def test_update_ficha_is_idempotent_and_local(tmp_path: Path) -> None:
    docs = make_docs(tmp_path)
    path = docs / "datasets" / "vendas.md"
    assert update_ficha(path) is True
    once = path.read_text(encoding="utf-8")
    assert "| Dataset | `alfa/vendas` |" in once
    assert once.index("<!-- gerado:ficha -->") < once.index("## Origem e coleta")
    assert "## Origem e coleta" in once
    assert update_ficha(path) is False
    assert path.read_text(encoding="utf-8") == once
    assert check_fetcher_docs(docs, ["vendas"], "alfa") == []


def test_update_ficha_replaces_stale_block(tmp_path: Path) -> None:
    docs = make_docs(tmp_path)
    path = docs / "datasets" / "vendas.md"
    text = path.read_text(encoding="utf-8").replace(
        "<!-- gerado:ficha -->\n", "<!-- gerado:ficha -->\n\n| velho | x |\n"
    )
    path.write_text(text, encoding="utf-8")
    update_ficha(path)
    assert "velho" not in path.read_text(encoding="utf-8")


def test_update_ficha_without_marker(tmp_path: Path) -> None:
    docs = make_docs(tmp_path, page(marker=False))
    with pytest.raises(DocFormatError):
        update_ficha(docs / "datasets" / "vendas.md")


# --- schema_from_lake -----------------------------------------------------------


def test_schema_from_lake(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "vendas"
    for mes, valor_dtype in ((1, pl.Float64), (2, pl.Utf8)):
        d = base / "ano=2024" / f"mes={mes}"
        d.mkdir(parents=True)
        pl.DataFrame(
            {"id_venda": [1], "valor": pl.Series([1], dtype=valor_dtype)}
            if valor_dtype is pl.Float64
            else {"id_venda": [1], "valor": ["1"], "extra": ["x"]}
        ).write_parquet(d / "part.parquet")
    (base / "_dataset.json").write_text("{}", encoding="utf-8")
    ignored = base / "ano=2024" / "mes=2" / "x.parquet.tmp"
    ignored.write_bytes(b"lixo")
    hidden = base / "_staging"
    hidden.mkdir()
    pl.DataFrame({"zzz": [1]}).write_parquet(hidden / "a.parquet")

    schema = schema_from_lake(tmp_path, "alfa", "vendas")
    assert schema["fonte"] == "alfa"
    assert schema["dataset"] == "vendas"
    assert schema["particoes"] == ["ano", "mes"]
    assert schema["arquivos"] == 2
    cols = {c["nome"]: c["tipos"] for c in schema["colunas"]}
    assert cols["id_venda"] == ["int64"]
    assert cols["valor"] == ["double", "string"] or cols["valor"] == [
        "double",
        "large_string",
    ]
    assert "extra" in cols
    assert "zzz" not in cols
    assert "gerado_em" in schema


def test_schema_from_lake_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        schema_from_lake(tmp_path, "alfa", "nada")


def test_write_schema_keeps_file_when_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "a.schema.json"
    first = {**SCHEMA, "gerado_em": "2026-01-01T00:00:00+00:00"}
    assert write_schema(path, first) is True
    again = {**SCHEMA, "gerado_em": "2027-01-01T00:00:00+00:00"}
    assert write_schema(path, again) is False
    assert json.loads(path.read_text(encoding="utf-8"))["gerado_em"].startswith("2026")
    assert write_schema(path, {**again, "arquivos": 9}) is True
