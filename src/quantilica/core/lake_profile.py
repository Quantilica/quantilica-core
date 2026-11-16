"""Perfil de colunas de um dataset do lake, calculado dos rodapés Parquet.

O lake guarda um dataset em ``<lake_dir>/<fonte>/<dataset>/`` com partições Hive
``ano=AAAA/mes=MM/``. :func:`profile_from_lake` lê **apenas os rodapés** dos
arquivos (``pyarrow.parquet.ParquetFile(...).metadata``; sem varrer dados) e
devolve, por período, o total de linhas e a contagem de nulos de cada coluna.
:func:`derivar_eras` resume esse perfil em eras de esquema.

Dependência: ``pyarrow`` (extra ``docs``: ``pip install "quantilica-core[docs]"``),
importado só quando usado.

Regras de cálculo
-----------------

- **Período** vem do caminho Hive, nunca do rodapé: ``ano=2005/mes=08`` vira
  ``"2005-08"`` (granularidade ``"mensal"``); só ``ano=2005`` vira ``"2005"``
  (``"anual"``); ``competencia=200508`` (sem ``ano=``) também vira
  ``"2005-08"``. ``mes=8`` e ``mes=08`` são o mesmo mês. Misturar arquivos com
  e sem ``mes`` no mesmo dataset levanta ``ValueError``. Dataset cujos arquivos
  não têm ``ano=`` (nem ``competencia=``) levanta uma vez só
  :class:`DatasetSemPeriodoError` (um ``ValueError``), em vez de acusar arquivo
  por arquivo.
  Subpartições além de ``ano``/``mes`` (por exemplo ``uf=SP``) não definem
  período: os arquivos do mesmo período são somados.
- ``linhas[i]`` é a soma de ``num_rows`` de todos os arquivos do período ``i``;
  ``nulos[i]`` de cada coluna é a soma dos ``null_count`` de todos os grupos de
  linhas de todos os arquivos do período.
- **Nulo do Parquet**: só ``null`` conta. Texto vazio (``""``) não é nulo. No
  lake o conversor grava nulo, não texto vazio (conferido nas amostras do
  ``sih-rd`` e do ``cnes-st``); para outras fontes, confira antes de interpretar
  o preenchimento.
- ``nulos[i] is None`` significa que a coluna **não existe** no período ``i``
  (evolução do esquema), diferente de ``0`` (existe e está toda preenchida).
  Se um período tem vários arquivos e a coluna falta em alguns, ela existe no
  período e os arquivos sem ela contam todas as suas linhas como nulas.
- **Colunas excluídas** do documento (:data:`EXCLUDED_COLUMNS`): as de partição
  ``ano``, ``mes``, as colunas físicas ``uf`` e ``_origem``, qualquer chave
  ``chave=valor`` que apareça nos caminhos e as colunas que os rodapés declaram
  em ``quantilica.member_columns``.
- **Fallback**: se o rodapé não traz ``null_count`` (arquivo escrito sem
  estatísticas) ou a coluna é aninhada (struct, lista, mapa), a contagem lê essa
  coluna do arquivo. Nos arquivos do lake isso não ocorre.
- Ignora arquivos e diretórios que começam com ``_`` e temporários (``.tmp`` no
  nome), como :func:`quantilica.core.docs.schema_from_lake`.

Formato do documento ``perfil`` (``LakeProfile.perfil``, versão 1)::

    {
      "versao": 1,
      "granularidade": "mensal",
      "periodos": ["1992-01", "1992-02"],
      "linhas": [3008, 3011],
      "colunas": {
        "n_aih": {"nulos": [0, 0]},
        "dt_inter": {"nulos": [null, 17]}
      }
    }

``periodos`` está em ordem crescente (texto ordenável). O preenchimento de uma
coluna no período ``i`` é ``1 - nulos[i] / linhas[i]``. Mudou o formato: sobe
``versao`` e os perfis são regerados do lake.

Hash: ``perfil_sha256`` é o SHA-256 (hex) do JSON canônico do documento
(:func:`canonical_json`: chaves ordenadas, sem espaços, UTF-8, ``ensure_ascii``
desligado). O servidor pode recalculá-lo com :func:`perfil_sha256`.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

__all__ = [
    "EXCLUDED_COLUMNS",
    "DatasetSemPeriodoError",
    "PROFILE_VERSION",
    "LakeProfile",
    "ProfileColumn",
    "canonical_json",
    "derivar_eras",
    "perfil_sha256",
    "period_from_path",
    "profile_from_lake",
]

PROFILE_VERSION = 1
EXCLUDED_COLUMNS = ("ano", "mes", "uf", "_origem")

_HIVE_PART = re.compile(r"^([^=/]+)=(.*)$")
_COMPETENCIA = re.compile(r"^(\d{4})(\d{2})$")
META_MEMBER_COLUMNS = "quantilica.member_columns"


class DatasetSemPeriodoError(ValueError):
    """Nenhum arquivo do dataset tem a partição ``ano=``: não há período.

    Subclasse de ``ValueError``: quem já capturava o erro antigo ("arquivo sem
    partição ano=") segue funcionando. O erro agora é do dataset, não de um
    arquivo.
    """


class ProfileColumn(TypedDict):
    """Uma coluna da união do dataset (``LakeProfile.colunas``).

    ``dtype_arrow``: tipo Arrow (``str(pyarrow.DataType)``) no último período em
    que a coluna existe. ``periodo_inicio``/``periodo_fim``: primeiro e último
    período em que ela existe (mesmo formato de ``periodos``).
    """

    nome: str
    dtype_arrow: str
    periodo_inicio: str
    periodo_fim: str


@dataclass(frozen=True)
class LakeProfile:
    """Resultado de :func:`profile_from_lake`.

    - ``perfil``: o documento descrito no docstring do módulo.
    - ``colunas``: a união das colunas de todos os períodos, na ordem da primeira
      aparição (períodos crescentes; dentro do arquivo, ordem do esquema).
    - ``perfil_sha256``: hash do JSON canônico de ``perfil``.
    """

    perfil: dict[str, Any]
    colunas: list[ProfileColumn]
    perfil_sha256: str


def canonical_json(perfil: Mapping[str, Any]) -> str:
    """JSON canônico: chaves ordenadas, sem espaços, sem escape de não-ASCII."""
    return json.dumps(perfil, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def perfil_sha256(perfil: Mapping[str, Any]) -> str:
    """SHA-256 (hex) do JSON canônico do documento de perfil."""
    return hashlib.sha256(canonical_json(perfil).encode("utf-8")).hexdigest()


def period_from_path(rel: Path | str) -> tuple[str, str] | None:
    """Período de um caminho relativo ao dataset, ou ``None`` se não há período.

    Devolve ``(periodo, granularidade)``: ``("2005-08", "mensal")`` para
    ``ano=2005/mes=8/...`` e para ``competencia=200508/...`` (sem ``ano=``), e
    ``("2005", "anual")`` para ``ano=2005/...``. Com ``ano=`` e
    ``competencia=`` no mesmo caminho vale o ``ano=``. Valores inválidos
    levantam ``ValueError``.
    """
    parts = Path(rel).parts[:-1]
    keys: dict[str, str] = {}
    for part in parts:
        m = _HIVE_PART.match(part)
        if m and m.group(1) not in keys:
            keys[m.group(1)] = m.group(2)
    if "ano" not in keys:
        if "competencia" not in keys:
            return None
        m = _COMPETENCIA.match(keys["competencia"])
        if m is None or not 1 <= int(m.group(2)) <= 12:
            raise ValueError(f"competencia inválida em {rel}")
        return f"{int(m.group(1)):04d}-{m.group(2)}", "mensal"
    try:
        ano = int(keys["ano"])
        mes = int(keys["mes"]) if "mes" in keys else None
    except ValueError as exc:
        raise ValueError(f"partição ano/mes inválida em {rel}") from exc
    if mes is None:
        return f"{ano:04d}", "anual"
    if not 1 <= mes <= 12:
        raise ValueError(f"mes fora de 1-12 em {rel}")
    return f"{ano:04d}-{mes:02d}", "mensal"


def lake_parquet_files(base: Path) -> list[Path]:
    """Parquets de um dataset, ordenados por caminho, sem ``_*`` nem ``.tmp``."""

    def ignored(rel: Path) -> bool:
        return any(p.startswith("_") or ".tmp" in p.lower() for p in rel.parts)

    return sorted(
        p
        for p in base.rglob("*.parquet")
        if p.is_file() and not ignored(p.relative_to(base))
    )


def import_pyarrow_parquet(who: str) -> Any:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depende do ambiente
        raise ImportError(
            f'{who} requer o pyarrow. Instale com: uv add "quantilica-core[docs]"'
        ) from exc
    return pq


_ColStats = dict[str, tuple[str, int]]


def _leaf_index(meta: Any) -> dict[str, int]:
    if meta.num_row_groups == 0:
        return {}
    return {
        meta.row_group(0).column(j).path_in_schema: j for j in range(meta.num_columns)
    }


def _null_count(pf: Any, leaf: dict[str, int], field: Any, groups: range) -> int:
    """Nulos de uma coluna nos grupos ``groups``: pelo rodapé, ou lendo a coluna."""
    import pyarrow as pa

    name = field.name
    meta = pf.metadata
    if meta.num_row_groups == 0 or not groups:
        return 0
    if not pa.types.is_nested(field.type) and name in leaf:  # aninhada: várias folhas
        total = 0
        for g in groups:
            stats = meta.row_group(g).column(leaf[name]).statistics
            if stats is None or not stats.has_null_count:
                break
            total += stats.null_count
        else:
            return total
    return sum(
        pf.read_row_group(g, columns=[name]).column(0).null_count for g in groups
    )


def _file_stats(pq: Any, file: Path) -> tuple[int, _ColStats, list[str]]:
    """``(linhas, {coluna: (tipo_arrow, nulos)}, colunas de membro)`` de um arquivo.

    Tudo pelo rodapé; só lê dados quando a coluna é aninhada ou o arquivo foi
    escrito sem estatísticas.
    """
    pf = pq.ParquetFile(file)
    try:
        meta = pf.metadata
        leaf = _leaf_index(meta)
        everything = range(meta.num_row_groups)
        out: _ColStats = {
            f.name: (str(f.type), _null_count(pf, leaf, f, everything))
            for f in pf.schema_arrow
        }
        raw = (meta.metadata or {}).get(META_MEMBER_COLUMNS.encode(), b"[]")
        members = _member_columns(raw.decode(errors="replace"))
        return meta.num_rows, out, members
    finally:
        pf.close()


def _member_columns(raw: str) -> list[str]:
    """Colunas de membro de ``quantilica.member_columns`` (``[]`` se ilegível)."""
    try:
        names = json.loads(raw)
    except ValueError:
        return []
    return [n for n in names if isinstance(n, str)] if isinstance(names, list) else []


def profile_from_lake(
    lake_dir: Path | str,
    source_id: str,
    dataset_id: str,
    *,
    exclude: Iterable[str] = EXCLUDED_COLUMNS,
) -> LakeProfile:
    """Perfil de ``<lake_dir>/<source_id>/<dataset_id>`` a partir dos rodapés.

    Ver o docstring do módulo para regras e formato. ``exclude`` substitui a
    lista :data:`EXCLUDED_COLUMNS`; chaves de partição Hive dos caminhos e as
    colunas de membro declaradas nos rodapés são sempre excluídas. Levanta
    ``FileNotFoundError`` (dataset ou Parquet ausente) e ``ValueError``
    (``ano``/``mes`` inválidos, granularidade mista, ou
    :class:`DatasetSemPeriodoError` quando nenhum arquivo tem ``ano=``).
    """
    pq = import_pyarrow_parquet("profile_from_lake")
    base = Path(lake_dir) / source_id / dataset_id
    if not base.is_dir():
        raise FileNotFoundError(f"dataset não encontrado no lake: {base}")
    files = lake_parquet_files(base)
    if not files:
        raise FileNotFoundError(f"nenhum arquivo Parquet em {base}")

    excluded = set(exclude)
    found_by_file: dict[Path, tuple[str, str]] = {}
    no_period: list[Path] = []
    for file in files:
        rel = file.relative_to(base)
        found = period_from_path(rel)
        if found is None:
            no_period.append(rel)
            continue
        found_by_file[file] = found
        for part in rel.parts[:-1]:
            m = _HIVE_PART.match(part)
            if m:
                excluded.add(m.group(1))
    if no_period:
        if len(no_period) == len(files):
            raise DatasetSemPeriodoError(
                f"dataset sem partição ano= ({len(files)} arquivos; "
                f"ex.: {no_period[0]}): não há período para o perfil"
            )
        raise ValueError(
            f"{len(no_period)} de {len(files)} arquivos sem partição ano= "
            f"(ex.: {no_period[0]})"
        )

    stats = {file: _file_stats(pq, file) for file in files}
    for _, _, member_columns in stats.values():
        excluded.update(member_columns)

    grans: set[str] = set()
    by_period: dict[str, list[tuple[int, _ColStats]]] = {}
    for file, (period, gran) in found_by_file.items():
        grans.add(gran)
        by_period.setdefault(period, []).append((stats[file][0], stats[file][1]))
    if len(grans) > 1:
        raise ValueError(f"granularidade mista (anual e mensal) em {base}")

    periodos = sorted(by_period)
    linhas: list[int] = []
    # nome -> {indice_do_periodo: nulos}; ordem de inserção = primeira aparição
    nulos: dict[str, dict[int, int]] = {}
    dtype: dict[str, str] = {}
    for i, period in enumerate(periodos):
        in_period = by_period[period]
        rows_total = sum(rows for rows, _ in in_period)
        names: dict[str, None] = {}  # união das colunas do período, em ordem
        for _, cols in in_period:
            for name, (type_name, _) in cols.items():
                if name not in excluded:
                    names.setdefault(name)
                    dtype[name] = type_name
        per_col = {
            name: sum(
                cols[name][1] if name in cols else rows for rows, cols in in_period
            )
            for name in names
        }
        linhas.append(rows_total)
        for name, n_null in per_col.items():
            nulos.setdefault(name, {})[i] = n_null

    colunas_doc = {
        name: {"nulos": [by_i.get(i) for i in range(len(periodos))]}
        for name, by_i in nulos.items()
    }
    perfil = {
        "versao": PROFILE_VERSION,
        "granularidade": grans.pop(),
        "periodos": periodos,
        "linhas": linhas,
        "colunas": colunas_doc,
    }
    colunas: list[ProfileColumn] = []
    for name, by_i in nulos.items():
        colunas.append(
            {
                "nome": name,
                "dtype_arrow": dtype[name],
                "periodo_inicio": periodos[min(by_i)],
                "periodo_fim": periodos[max(by_i)],
            }
        )
    return LakeProfile(perfil, colunas, perfil_sha256(perfil))


def derivar_eras(perfil: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Eras de esquema de um documento ``perfil``.

    Período consecutivos com o **mesmo conjunto de colunas existentes**
    (``nulos[i] is not None``) formam uma era. Cada era é::

        {"inicio": "1992-01", "fim": "1996-12", "n_colunas": 35,
         "entrou": ["col_a"], "saiu": ["col_b"]}

    ``entrou`` e ``saiu`` (ordenadas) comparam com a era anterior: colunas que
    passam a existir e colunas que deixam de existir. A primeira era tem ambas
    vazias. Uma coluna que some e volta gera uma era a mais, com ``saiu`` e
    depois ``entrou``. Função pura: não lê o lake; aceita o ``perfil`` vindo do
    JSON do banco.
    """
    periodos: list[str] = list(perfil.get("periodos", []))
    colunas: Mapping[str, Any] = perfil.get("colunas", {})
    eras: list[dict[str, Any]] = []
    atual: frozenset[str] | None = None
    anterior: frozenset[str] = frozenset()
    for i, periodo in enumerate(periodos):
        existentes = frozenset(
            nome for nome, c in colunas.items() if c["nulos"][i] is not None
        )
        if eras and existentes == atual:
            eras[-1]["fim"] = periodo
            continue
        if eras:
            anterior = atual  # type: ignore[assignment]
        eras.append(
            {
                "inicio": periodo,
                "fim": periodo,
                "n_colunas": len(existentes),
                "entrou": sorted(existentes - anterior) if eras else [],
                "saiu": sorted(anterior - existentes) if eras else [],
            }
        )
        atual = existentes
    return eras
