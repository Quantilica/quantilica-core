"""Verificação da documentação dos dados dos fetchers.

Implementa a norma "Documentação dos dados". Cada fetcher tem::

    docs/
      README.md                  # página da fonte
      datasets/
        <dataset_id>.md          # uma página por dataset
        <dataset_id>.schema.json # esquema de referência do lake (gerado)

Este módulo lê o cabeçalho YAML das páginas, confere a estrutura (seções,
partições, dicionário de variáveis) e gera as partes automáticas (ficha técnica
e esquema de referência). Um fetcher o usa com o teste-padrão::

    from quantilica.core.docs import assert_fetcher_docs

    def test_docs():
        assert_fetcher_docs(DOCS_DIR, converter.datasets(), converter.source_id)

Dependências opcionais (extra ``docs``: ``pip install "quantilica-core[docs]"``):
``pyyaml`` lê o cabeçalho das páginas e ``pyarrow`` lê os rodapés dos Parquet em
:func:`schema_from_lake`. Ambos são importados só quando usados.

Formato do ``<dataset_id>.schema.json``::

    {
      "fonte": "bcb-sgs",
      "dataset": "series",
      "gerado_em": "2026-11-07T12:00:00+00:00",
      "particoes": ["ano", "mes"],
      "colunas": [
        {"nome": "valor", "tipos": ["double"]},
        {"nome": "codigo", "tipos": ["int32", "string"]}
      ],
      "arquivos": 24
    }

``tipos`` é uma lista (ordenada, sem repetição) porque o tipo de uma coluna
pode variar entre eras ou partições. Os nomes de tipo são os do Arrow
(``str(pyarrow.DataType)``). ``colunas`` segue a ordem em que as colunas
aparecem nos arquivos (ordenados por caminho); ``particoes`` são as chaves
``chave=valor`` dos diretórios, da mais externa para a mais interna.

Regras de verificação (seção 6 da norma), todas erros, exceto onde indicado:

- página sem dataset e dataset sem página; ``docs/README.md`` ausente;
- cabeçalho fora do esquema; ``id`` e ``fonte`` diferentes do nome do arquivo
  e do ``source_id``;
- as nove seções ausentes ou fora de ordem; marcador ``<!-- gerado:ficha -->``
  ausente;
- com ``lake.estado == convertido``: ``<id>.schema.json`` ausente, partições do
  cabeçalho diferentes das do esquema, dicionário de variáveis (tabela cuja
  primeira coluna é "Coluna no lake") que não cobre exatamente as colunas do
  esquema (colunas de partição não precisam constar);
- **aviso**: o esquema tem coluna numérica que não é chave nem partição e
  ``unidades_de_medida`` não a menciona (ou está vazio); e ``lake.colunas`` do
  cabeçalho diferente do número de colunas do esquema.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

__all__ = [
    "FICHA_MARKER",
    "LAKE_ESTADOS",
    "REQUIRED_SECTIONS",
    "DatasetDocHeader",
    "DocFormatError",
    "DocProblem",
    "assert_fetcher_docs",
    "check_fetcher_docs",
    "parse_dataset_page",
    "parse_variables_table",
    "render_ficha",
    "schema_from_lake",
    "update_ficha",
    "write_schema",
]

FICHA_MARKER = "<!-- gerado:ficha -->"
LAKE_ESTADOS = ("convertido", "parcial", "so-bruto")
REQUIRED_SECTIONS = (
    "Resumo",
    "Ficha técnica",
    "Origem e coleta",
    "Conteúdo",
    "Variáveis",
    "No lake",
    "Cuidados de uso",
    "Exemplos",
    "Referências",
)
VARIABLES_FIRST_COLUMN = "Coluna no lake"

_PERIOD = re.compile(r"^\d{4}(-(0[1-9]|1[0-2]))?$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NUMERIC_TYPE = re.compile(r"^(u?int\d*|float\d*|double|decimal.*)$", re.IGNORECASE)
_FENCE = re.compile(r"^\s*(```|~~~)")
_H2 = re.compile(r"^##\s+(.*?)\s*#*\s*$")


class DocFormatError(ValueError):
    """Página ilegível ou cabeçalho fora do esquema; ``problems`` dá os motivos."""

    def __init__(self, problems: list[str]) -> None:
        self.problems = problems
        super().__init__("; ".join(problems))


@dataclass(frozen=True)
class DocProblem:
    """Um achado da verificação. ``severity`` é ``"error"`` ou ``"warning"``."""

    path: str
    message: str
    severity: str = "error"

    def __str__(self) -> str:
        label = "AVISO" if self.severity == "warning" else "ERRO"
        return f"[{label}] {self.path}: {self.message}"


@dataclass
class DatasetDocHeader:
    """Cabeçalho YAML (chave ``dataset:``) de uma página de dataset."""

    id: str
    fonte: str
    titulo: str
    resumo: str
    orgao: str | None
    pagina_oficial: str | None
    licenca: str | None
    periodicidade: str | None
    cobertura_inicio: str | None
    cobertura_fim: str | None
    unidade: str | None
    granularidade_espacial: str | None
    granularidade_temporal: str | None
    formato_bruto: str | None
    lake_estado: str
    lake_particoes: list[str]
    lake_chave: list[str]
    lake_colunas: int | None
    unidades_de_medida: dict[str, str] | None
    dados_pessoais: list[dict[str, str]] | None  # None = "nenhum"
    eras: list[dict[str, str]]
    revisado_em: str
    versao_fetcher: str
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def convertido(self) -> bool:
        return self.lake_estado == "convertido"


# --- cabeçalho -----------------------------------------------------------------


def _import_yaml() -> Any:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise ImportError(
            "quantilica.core.docs requer o pyyaml. "
            'Instale com: uv add "quantilica-core[docs]"'
        ) from exc
    return yaml


def _split_front_matter(text: str) -> tuple[str, str]:
    lines = text.lstrip("\ufeff").splitlines()
    if not lines or lines[0].strip() != "---":
        raise DocFormatError(["a página não começa com o cabeçalho YAML (---)"])
    for i in range(1, len(lines)):
        if lines[i].strip() == "---":
            return "\n".join(lines[1:i]), "\n".join(lines[i + 1 :])
    raise DocFormatError(["o cabeçalho YAML não é fechado por ---"])


def section_titles(body: str) -> list[str]:
    """Títulos ``##`` do corpo, ignorando blocos de código."""
    titles: list[str] = []
    in_fence = False
    for line in body.splitlines():
        if _FENCE.match(line):
            in_fence = not in_fence
            continue
        if not in_fence:
            match = _H2.match(line)
            if match:
                titles.append(match.group(1))
    return titles


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10] if isinstance(value, date) else str(value)
    return str(value)


def _validate(data: Any) -> tuple[DatasetDocHeader | None, list[str]]:
    problems: list[str] = []
    if not isinstance(data, dict) or not isinstance(data.get("dataset"), dict):
        return None, ["o cabeçalho não tem a chave 'dataset:' com os campos"]
    d: dict[str, Any] = data["dataset"]

    required = (
        "id fonte titulo resumo orgao pagina_oficial licenca periodicidade "
        "cobertura unidade granularidade formato_bruto lake dados_pessoais "
        "eras revisado_em versao_fetcher"
    ).split()
    for key in required:
        if key not in d:
            problems.append(f"campo obrigatório ausente: {key}")

    def need_text(key: str, *, nullable: bool) -> str | None:
        if key not in d:
            return None
        value = d[key]
        if value is None:
            if not nullable:
                problems.append(f"{key}: não pode ser nulo")
            return None
        if isinstance(value, (dict, list)) or not str(value).strip():
            problems.append(f"{key}: deve ser um texto não vazio")
            return None
        return str(value).strip()

    id_ = need_text("id", nullable=False)
    fonte = need_text("fonte", nullable=False)
    titulo = need_text("titulo", nullable=False)
    resumo = need_text("resumo", nullable=False)
    orgao = need_text("orgao", nullable=True)
    pagina = need_text("pagina_oficial", nullable=True)
    licenca = need_text("licenca", nullable=True)
    periodicidade = need_text("periodicidade", nullable=True)
    unidade = need_text("unidade", nullable=True)
    formato = need_text("formato_bruto", nullable=True)
    versao = need_text("versao_fetcher", nullable=False)

    # cobertura
    inicio = fim = None
    cob = d.get("cobertura")
    if "cobertura" in d:
        if not isinstance(cob, dict):
            problems.append("cobertura: deve ter 'inicio' e 'fim'")
        else:
            for key in ("inicio", "fim"):
                if key not in cob:
                    problems.append(f"cobertura.{key}: ausente")
            inicio = _text(cob.get("inicio"))
            fim = _text(cob.get("fim"))
            if "inicio" in cob and (inicio is None or not _PERIOD.match(inicio)):
                problems.append(f"cobertura.inicio: {inicio!r} não é AAAA-MM ou AAAA")
            if "fim" in cob and (
                fim is None or not (fim == "em-curso" or _PERIOD.match(fim))
            ):
                problems.append(
                    f"cobertura.fim: {fim!r} não é AAAA-MM, AAAA nem 'em-curso'"
                )

    # granularidade
    espacial = temporal = None
    gran = d.get("granularidade")
    if "granularidade" in d:
        if not isinstance(gran, dict) or not {"espacial", "temporal"} <= gran.keys():
            problems.append("granularidade: deve ter 'espacial' e 'temporal'")
        else:
            espacial = _text(gran["espacial"])
            temporal = _text(gran["temporal"])

    # lake
    estado = ""
    particoes: list[str] = []
    chave: list[str] = []
    colunas: int | None = None
    lake = d.get("lake")
    if "lake" in d:
        if not isinstance(lake, dict):
            problems.append("lake: deve ter estado, particoes, chave e colunas")
        else:
            for key in ("estado", "particoes", "chave", "colunas"):
                if key not in lake:
                    problems.append(f"lake.{key}: ausente")
            estado = str(lake.get("estado") or "")
            if "estado" in lake and estado not in LAKE_ESTADOS:
                problems.append(
                    f"lake.estado: {estado!r} fora de {', '.join(LAKE_ESTADOS)}"
                )
            for key, target in (("particoes", particoes), ("chave", chave)):
                value = lake.get(key)
                if key not in lake:
                    continue
                if value is None:
                    if estado == "convertido":
                        problems.append(f"lake.{key}: deve ser uma lista")
                elif not isinstance(value, list) or not all(
                    isinstance(v, str) for v in value
                ):
                    problems.append(f"lake.{key}: deve ser uma lista de nomes")
                else:
                    target.extend(value)
            if "colunas" in lake:
                value = lake["colunas"]
                if value is None:
                    if estado == "convertido":
                        problems.append("lake.colunas: deve ser um inteiro")
                elif isinstance(value, bool) or not isinstance(value, int):
                    problems.append("lake.colunas: deve ser um inteiro")
                else:
                    colunas = value

    # unidades_de_medida (opcional)
    unidades: dict[str, str] | None = None
    um = d.get("unidades_de_medida")
    if um is not None:
        if not isinstance(um, dict):
            problems.append("unidades_de_medida: deve ser coluna -> unidade")
        else:
            unidades = {str(k): str(v) for k, v in um.items()}

    # dados_pessoais
    pessoais: list[dict[str, str]] | None = None
    dp = d.get("dados_pessoais")
    if "dados_pessoais" in d:
        if dp == "nenhum":
            pessoais = None
        elif isinstance(dp, list) and dp:
            pessoais = []
            for item in dp:
                if (
                    isinstance(item, dict)
                    and item.get("coluna")
                    and item.get("categoria")
                ):
                    pessoais.append(
                        {
                            "coluna": str(item["coluna"]),
                            "categoria": str(item["categoria"]),
                        }
                    )
                else:
                    problems.append(
                        "dados_pessoais: cada item deve ter 'coluna' e 'categoria'"
                    )
                    break
        else:
            problems.append(
                "dados_pessoais: use 'nenhum' ou a lista de {coluna, categoria}"
            )

    # eras
    eras: list[dict[str, str]] = []
    raw_eras = d.get("eras")
    if "eras" in d:
        if not isinstance(raw_eras, list):
            problems.append("eras: deve ser uma lista (vazia se não houver quebras)")
        else:
            for item in raw_eras:
                desde = _text(item.get("desde")) if isinstance(item, dict) else None
                mudanca = _text(item.get("mudanca")) if isinstance(item, dict) else None
                if not desde or not mudanca or not _PERIOD.match(desde):
                    problems.append(
                        "eras: cada item deve ter 'desde' (AAAA-MM) e 'mudanca'"
                    )
                    break
                eras.append({"desde": desde, "mudanca": mudanca})

    # revisado_em
    revisado = _text(d.get("revisado_em"))
    if "revisado_em" in d and (revisado is None or not _ISO_DATE.match(revisado)):
        problems.append(f"revisado_em: {revisado!r} não é AAAA-MM-DD")
        revisado = None

    if problems or None in (id_, fonte, titulo, resumo, versao) or revisado is None:
        return None, problems
    assert id_ and fonte and titulo and resumo and versao and revisado
    header = DatasetDocHeader(
        id=id_,
        fonte=fonte,
        titulo=titulo,
        resumo=resumo,
        orgao=orgao,
        pagina_oficial=pagina,
        licenca=licenca,
        periodicidade=periodicidade,
        cobertura_inicio=inicio,
        cobertura_fim=fim,
        unidade=unidade,
        granularidade_espacial=espacial,
        granularidade_temporal=temporal,
        formato_bruto=formato,
        lake_estado=estado,
        lake_particoes=particoes,
        lake_chave=chave,
        lake_colunas=colunas,
        unidades_de_medida=unidades,
        dados_pessoais=pessoais,
        eras=eras,
        revisado_em=revisado,
        versao_fetcher=versao,
        raw=d,
    )
    return header, problems


def parse_dataset_page(path: Path | str) -> tuple[DatasetDocHeader, list[str]]:
    """Lê uma página de dataset: devolve o cabeçalho validado e os títulos ``##``.

    Levanta :class:`DocFormatError` (com ``problems``) se a página não puder ser
    lida, não tiver o front matter YAML ou se o cabeçalho violar o esquema da
    seção 3 da norma.
    """
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise DocFormatError([f"não consegui ler a página: {exc}"]) from exc
    front, body = _split_front_matter(text)
    yaml = _import_yaml()
    try:
        data = yaml.safe_load(front)
    except yaml.YAMLError as exc:
        raise DocFormatError([f"YAML inválido no cabeçalho: {exc}"]) from exc
    header, problems = _validate(data)
    if header is None:
        raise DocFormatError(problems or ["cabeçalho inválido"])
    return header, section_titles(body)


# --- seções e dicionário --------------------------------------------------------


def _section_body(text: str, title: str) -> str | None:
    """Texto entre ``## title`` e o próximo ``##`` (blocos de código respeitados)."""
    lines = text.splitlines()
    out: list[str] = []
    inside = False
    in_fence = False
    for line in lines:
        if _FENCE.match(line):
            in_fence = not in_fence
        elif not in_fence:
            match = _H2.match(line)
            if match:
                if inside:
                    break
                inside = match.group(1) == title
                continue
        if inside:
            out.append(line)
    return "\n".join(out) if inside else None


def _split_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|") and not line.endswith("\\|"):
        line = line[:-1]
    return [c.strip() for c in re.split(r"(?<!\\)\|", line)]


def parse_variables_table(text: str) -> list[str] | None:
    """Nomes da primeira coluna da tabela "Coluna no lake" da seção Variáveis.

    Crases em volta do nome são removidas. ``None`` se a seção ou a tabela não
    existir.
    """
    body = _section_body(text, "Variáveis")
    if body is None:
        return None
    names: list[str] | None = None
    for line in body.splitlines():
        if not line.strip().startswith("|"):
            if names is not None:
                break
            continue
        cells = _split_row(line)
        if names is None:
            if cells and cells[0].strip("` ").lower() == VARIABLES_FIRST_COLUMN.lower():
                names = []
            continue
        first = cells[0] if cells else ""
        if re.fullmatch(r":?-{2,}:?", first.replace(" ", "")):
            continue
        name = first.strip().strip("`").strip()
        if name:
            names.append(name)
    return names


# --- ficha técnica --------------------------------------------------------------


def _cell(value: Any) -> str:
    if value is None or value == "":
        return "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_ficha(header: DatasetDocHeader) -> str:
    """Tabela Markdown da ficha técnica, gerada do cabeçalho."""
    cobertura = f"{_cell(header.cobertura_inicio)} a {_cell(header.cobertura_fim)}"
    granularidade = (
        f"espacial: {_cell(header.granularidade_espacial)}; "
        f"temporal: {_cell(header.granularidade_temporal)}"
    )
    if header.unidades_de_medida:
        medidas = "; ".join(f"`{k}`: {v}" for k, v in header.unidades_de_medida.items())
    else:
        medidas = None
    if header.dados_pessoais is None:
        pessoais = "nenhum"
    else:
        pessoais = "; ".join(
            f"`{p['coluna']}` ({p['categoria']})" for p in header.dados_pessoais
        )
    eras = (
        "; ".join(f"{e['desde']}: {e['mudanca']}" for e in header.eras)
        if header.eras
        else "nenhuma"
    )
    rows = [
        ("Dataset", f"`{header.fonte}/{header.id}`"),
        ("Órgão", header.orgao),
        ("Página oficial", header.pagina_oficial),
        ("Licença", header.licenca),
        ("Periodicidade", header.periodicidade),
        ("Cobertura", cobertura),
        ("Unidade (uma linha)", header.unidade),
        ("Granularidade", granularidade),
        ("Formato bruto", header.formato_bruto),
        ("Estado no lake", header.lake_estado),
        ("Partições", ", ".join(f"`{p}`" for p in header.lake_particoes) or None),
        ("Chave", ", ".join(f"`{c}`" for c in header.lake_chave) or None),
        ("Colunas", header.lake_colunas),
        ("Unidades de medida", medidas),
        ("Dados pessoais", pessoais),
        ("Eras", eras),
        ("Revisado em", header.revisado_em),
        ("Versão do fetcher", header.versao_fetcher),
    ]
    lines = ["| Campo | Valor |", "|---|---|"]
    lines += [f"| {k} | {_cell(v)} |" for k, v in rows]
    return "\n".join(lines)


def update_ficha(page_path: Path | str) -> bool:
    """Reescreve a ficha técnica da página; devolve ``True`` se o arquivo mudou.

    Substitui só o que vem depois de ``<!-- gerado:ficha -->`` até o próximo
    ``##`` (ou o fim do arquivo). Idempotente. Levanta :class:`DocFormatError`
    se a página for inválida ou não tiver o marcador.
    """
    path = Path(page_path)
    header, _ = parse_dataset_page(path)
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    start = next(
        (i for i, line in enumerate(lines) if line.strip() == FICHA_MARKER), None
    )
    if start is None:
        raise DocFormatError([f"marcador {FICHA_MARKER} ausente"])
    end = len(lines)
    in_fence = False
    for i in range(start + 1, len(lines)):
        if _FENCE.match(lines[i]):
            in_fence = not in_fence
        elif not in_fence and _H2.match(lines[i]):
            end = i
            break
    new_lines = [*lines[: start + 1], "", *render_ficha(header).splitlines()]
    if end < len(lines):
        new_lines += ["", *lines[end:]]
    new_text = "\n".join(new_lines) + "\n"
    if new_text == text:
        return False
    path.write_text(new_text, encoding="utf-8")
    return True


# --- esquema de referência -----------------------------------------------------


def schema_from_lake(lake_dir: Path | str, source_id: str, dataset_id: str) -> dict:
    """Esquema de referência de ``<lake_dir>/<source_id>/<dataset_id>``.

    Lê apenas os rodapés dos arquivos Parquet (``pyarrow.parquet.read_schema``),
    une colunas e tipos de todos os arquivos e deduz as partições dos diretórios
    ``chave=valor``. Ignora arquivos e diretórios que começam com ``_`` e
    temporários (``.tmp`` no nome). Formato: ver o docstring do módulo.

    A leitura usa ``pyarrow`` (extra ``docs``), a dependência de Parquet que o
    ecossistema já adota; o ``polars`` não é exigido só para ler esquemas.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise ImportError(
            "schema_from_lake requer o pyarrow. "
            'Instale com: uv add "quantilica-core[docs]"'
        ) from exc

    base = Path(lake_dir) / source_id / dataset_id
    if not base.is_dir():
        raise FileNotFoundError(f"dataset não encontrado no lake: {base}")

    def ignored(rel: Path) -> bool:
        return any(p.startswith("_") or ".tmp" in p.lower() for p in rel.parts)

    files = sorted(
        p
        for p in base.rglob("*.parquet")
        if p.is_file() and not ignored(p.relative_to(base))
    )
    if not files:
        raise FileNotFoundError(f"nenhum arquivo Parquet em {base}")

    particoes: list[str] = []
    columns: dict[str, list[str]] = {}
    for file in files:
        rel = file.relative_to(base)
        for part in rel.parts[:-1]:
            key, sep, _ = part.partition("=")
            if sep and key not in particoes:
                particoes.append(key)
        for arrow_field in pq.read_schema(file):
            types = columns.setdefault(arrow_field.name, [])
            type_name = str(arrow_field.type)
            if type_name not in types:
                types.append(type_name)
    return {
        "fonte": source_id,
        "dataset": dataset_id,
        "gerado_em": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "particoes": particoes,
        "colunas": [
            {"nome": name, "tipos": sorted(types)} for name, types in columns.items()
        ],
        "arquivos": len(files),
    }


def write_schema(path: Path | str, schema: dict) -> bool:
    """Grava o esquema; devolve ``True`` se mudou.

    Se o conteúdo for igual ao existente (ignorando ``gerado_em``), o arquivo
    não é tocado, para não gerar diffs a cada execução.
    """
    path = Path(path)
    if path.exists():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            old = None
        if isinstance(old, dict):
            same = {k: v for k, v in old.items() if k != "gerado_em"} == {
                k: v for k, v in schema.items() if k != "gerado_em"
            }
            if same:
                return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(schema, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return True


# --- verificação ---------------------------------------------------------------


def _load_schema(path: Path) -> tuple[dict | None, str | None]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, f"esquema ilegível: {exc}"
    if (
        not isinstance(data, dict)
        or not isinstance(data.get("particoes"), list)
        or not isinstance(data.get("colunas"), list)
        or not all(isinstance(c, dict) and "nome" in c for c in data["colunas"])
    ):
        return None, "esquema fora do formato (particoes/colunas)"
    return data, None


def check_fetcher_docs(
    docs_dir: Path | str, datasets: Iterable[str], source_id: str
) -> list[DocProblem]:
    """Verifica a pasta ``docs/`` de um fetcher (seção 6 da norma).

    ``datasets`` são os ids que o conversor do lake declara. Devolve a lista de
    :class:`DocProblem` (vazia se tudo estiver certo); ``severity`` distingue
    erros de avisos.
    """
    docs_dir = Path(docs_dir)
    problems: list[DocProblem] = []

    def err(path: str, msg: str) -> None:
        problems.append(DocProblem(path, msg))

    def warn(path: str, msg: str) -> None:
        problems.append(DocProblem(path, msg, "warning"))

    if not (docs_dir / "README.md").is_file():
        err("docs/README.md", "página da fonte ausente")

    datasets_dir = docs_dir / "datasets"
    declared = sorted(set(datasets))
    pages = (
        {p.stem: p for p in datasets_dir.glob("*.md")} if datasets_dir.is_dir() else {}
    )
    for dataset in declared:
        if dataset not in pages:
            err(f"docs/datasets/{dataset}.md", "dataset declarado sem página")
    for stem in sorted(set(pages) - set(declared)):
        err(f"docs/datasets/{stem}.md", "página sem dataset correspondente")

    for dataset in declared:
        page = pages.get(dataset)
        if page is None:
            continue
        rel = f"docs/datasets/{page.name}"
        try:
            header, titles = parse_dataset_page(page)
        except DocFormatError as exc:
            for message in exc.problems:
                err(rel, message)
            continue

        if header.id != dataset:
            err(rel, f"id {header.id!r} diferente do nome do arquivo {dataset!r}")
        if header.fonte != source_id:
            err(rel, f"fonte {header.fonte!r} diferente da fonte {source_id!r}")

        missing = [s for s in REQUIRED_SECTIONS if s not in titles]
        if missing:
            err(rel, "seções ausentes: " + ", ".join(missing))
        else:
            order = [t for t in titles if t in REQUIRED_SECTIONS]
            if order != list(REQUIRED_SECTIONS):
                err(
                    rel,
                    "seções fora de ordem: esperado "
                    + " > ".join(REQUIRED_SECTIONS)
                    + "; encontrado "
                    + " > ".join(order),
                )
        text = page.read_text(encoding="utf-8")
        if FICHA_MARKER not in text:
            err(rel, f"marcador {FICHA_MARKER} ausente")

        if not header.convertido:
            continue

        schema_path = datasets_dir / f"{dataset}.schema.json"
        rel_schema = f"docs/datasets/{schema_path.name}"
        if not schema_path.is_file():
            err(rel_schema, "dataset convertido sem esquema de referência")
            continue
        schema, bad = _load_schema(schema_path)
        if schema is None:
            err(rel_schema, bad or "esquema inválido")
            continue

        if list(header.lake_particoes) != list(schema["particoes"]):
            err(
                rel,
                f"partições do cabeçalho {header.lake_particoes} diferentes das do "
                f"esquema {schema['particoes']}",
            )

        schema_cols = [str(c["nome"]) for c in schema["colunas"]]
        if header.lake_colunas is not None and header.lake_colunas != len(schema_cols):
            warn(
                rel,
                f"lake.colunas={header.lake_colunas}, mas o esquema tem "
                f"{len(schema_cols)} colunas",
            )

        documented = parse_variables_table(text)
        if documented is None:
            if "Variáveis" in titles:
                err(
                    rel,
                    f"seção Variáveis sem tabela cuja primeira coluna é "
                    f"'{VARIABLES_FIRST_COLUMN}'",
                )
        else:
            parts = set(schema["particoes"]) | set(header.lake_particoes)
            expected = [c for c in schema_cols if c not in parts]
            absent = [c for c in expected if c not in documented]
            extra = [c for c in documented if c not in schema_cols and c not in parts]
            if absent:
                err(rel, "colunas do esquema fora do dicionário: " + ", ".join(absent))
            if extra:
                err(
                    rel,
                    "variáveis do dicionário que não estão no esquema: "
                    + ", ".join(extra),
                )

        keys = set(header.lake_chave) | set(schema["particoes"])
        numeric = [
            str(c["nome"])
            for c in schema["colunas"]
            if str(c["nome"]) not in keys
            and any(_NUMERIC_TYPE.match(str(t)) for t in c.get("tipos", []))
        ]
        measured = header.unidades_de_medida or {}
        unmeasured = [c for c in numeric if c not in measured]
        if unmeasured:
            warn(
                rel,
                "unidades_de_medida não cobre colunas numéricas: "
                + ", ".join(unmeasured),
            )
    return problems


def assert_fetcher_docs(
    docs_dir: Path | str, datasets: Iterable[str], source_id: str
) -> None:
    """Para o pytest de cada fetcher: levanta ``AssertionError`` se houver erros.

    Avisos não derrubam o teste. A mensagem lista cada erro em uma linha.
    """
    problems = check_fetcher_docs(docs_dir, datasets, source_id)
    errors = [p for p in problems if p.severity == "error"]
    if errors:
        raise AssertionError(
            f"{len(errors)} problema(s) na documentação dos dados de {source_id}:\n"
            + "\n".join(f"  - {p}" for p in errors)
        )
