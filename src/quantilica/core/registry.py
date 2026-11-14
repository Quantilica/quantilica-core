"""Modelo do registro de fetchers da Quantilica.

Declara os fatos de uma fonte de dados que pertencem ao pacote do fetcher
(:class:`FetcherInfo`), as dataclasses de catálogo que antes cada pacote
copiava (:class:`DatasetEntry`, :class:`GroupInfo`) e funções puras para
resolver apelidos, checar duplicidade e montar o ``registry.json``.

O registro tem três camadas, cada uma com um dono:

1. fatos da fonte, declarados no pacote (:class:`FetcherInfo`);
2. identidade de empacotamento, derivada dos metadados da distribuição instalada
   e nunca declarada (:class:`IdentidadePacote`);
3. fatos do ecossistema, declarados num arquivo público (``registro.toml``, lido
   por :func:`carrega_registro_ecossistema` como :class:`RegistroEcossistema`).

Este módulo não carrega infraestrutura (hosts, caminhos, horários, pools):
fatos de operação ficam fora dos pacotes.
"""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, fields
from datetime import date
from pathlib import Path
from typing import Any, Literal, get_args
from urllib.parse import urlparse

__all__ = [
    "CADENCIAS",
    "DISTRIBUICAO_SUFIXO",
    "DOCS_PASTAS",
    "REDISTRIBUICOES",
    "REGISTRY_VERSION",
    "Cadencia",
    "DatasetEntry",
    "FetcherInfo",
    "GroupInfo",
    "IdentidadePacote",
    "RegistroEcossistema",
    "RegistryError",
    "Renome",
    "build_registry",
    "carrega_registro_ecossistema",
    "check_identidade",
    "check_unique",
    "distribuicao_esperada",
    "is_redistributable",
    "registry_sha256",
    "resolve_alias",
]

#: Versão do formato do ``registry.json``. Só sobe em mudança incompatível
#: (campo removido, renomeado ou com outro significado) ou quando o formato ganha
#: um bloco novo que o leitor precisa conhecer. A versão 2 acrescenta a
#: identidade de empacotamento (``module``, ``repo``, ``version``), os fatos do
#: ecossistema (``docs_pasta``, ``vitrine``, ``ci_integracao``, ``json_leve``,
#: ``mascote``, ``health_url``, ``historico``) e o ``sha256`` do conteúdo. Os
#: campos da versão 1 mantêm nome e formato: a versão 2 só acrescenta. Campo
#: novo e opcional dentro de uma versão segue aditivo e NÃO a sobe (caso de
#: ``redistribuicao`` e ``restricao_nota``).
REGISTRY_VERSION = 2

#: Sufixo da regra de nomenclatura: a distribuição (e o repositório) de um
#: fetcher é ``<id>-fetcher``.
DISTRIBUICAO_SUFIXO = "-fetcher"

#: Vocabulário de ``docs_pasta``: as seções de ``docs/docs/fontes/``.
DOCS_PASTAS: tuple[str, ...] = (
    "aviacao",
    "bcb",
    "clima",
    "comex",
    "educacao",
    "empresas",
    "energia",
    "governo",
    "ibge",
    "ipea",
    "loterias",
    "mercado-capitais",
    "petroleo",
    "politica",
    "saude",
    "tesouro",
    "trabalho",
    "transportes",
)

#: Vocabulário de cadência esperada (o mesmo de ``expected_cadence`` nos
#: manifestos e do cálculo de atraso em ``quantilica manifests``), mais
#: ``irregular`` para fonte sem periodicidade fixa (sorteios, cargas avulsas):
#: declara que não há prazo de frescor, diferente de ``None`` (não informado).
Cadencia = Literal["daily", "weekly", "monthly", "quarterly", "yearly", "irregular"]
CADENCIAS: tuple[str, ...] = get_args(Cadencia)

#: Vocabulário de redistribuição dos dados da fonte. ``None`` = não avaliada.
Redistribuicao = Literal["aberta", "restrita"]
REDISTRIBUICOES: tuple[str, ...] = get_args(Redistribuicao)

_ID_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
# Apelidos aceitam também o sublinhado dos nomes históricos (``bcb_microdados``).
_ALIAS_RE = re.compile(r"^[a-z0-9]+([-_][a-z0-9]+)*$")
# Nome de distribuição PyPI, já normalizado em minúsculas.
_DIST_RE = re.compile(r"^[a-z0-9]+([-_.][a-z0-9]+)*$")
# ``Dono/repositório`` do GitHub.
_REPO_SLUG_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class RegistryError(ValueError):
    """Registro inválido: campo mal formado ou id/apelido duplicado."""


def _opt_text(field_name: str, value: object, fid: str) -> None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise RegistryError(
            f"FetcherInfo {fid!r}: {field_name} deve ser texto não vazio ou None, "
            f"recebi {value!r}"
        )


@dataclass(frozen=True)
class FetcherInfo:
    """Fatos de uma fonte, declarados pelo pacote do fetcher.

    Attributes:
        id: Id canônico (nome do repositório sem ``-fetcher``), no formato
            ``[a-z0-9]+(-[a-z0-9]+)*``.
        nome: Nome legível da fonte.
        aliases: Nomes alternativos antigos ou curtos (``td``, ``bcb``).
        orgao: Órgão responsável pelos dados.
        licenca: Licença de uso dos dados.
        homepage: Página oficial da fonte (``http(s)://``).
        cadencia: Cadência esperada de atualização (ver :data:`CADENCIAS`).
        dominio: Domínio temático opcional (ex.: ``"economia"``).
        redistribuicao: ``"aberta"`` ou ``"restrita"`` (ver
            :data:`REDISTRIBUICOES`); ``None`` quando ainda não avaliada.
            Fonte restrita roda para uso interno, mas não é exibida nem
            oferecida para download (ver :func:`is_redistributable`).
        restricao_nota: Motivo curto da marca de redistribuição (texto livre).
    """

    id: str
    nome: str
    aliases: tuple[str, ...] = ()
    orgao: str | None = None
    licenca: str | None = None
    homepage: str | None = None
    cadencia: Cadencia | None = None
    dominio: str | None = None
    redistribuicao: Redistribuicao | None = None
    restricao_nota: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not _ID_RE.match(self.id):
            raise RegistryError(
                f"FetcherInfo: id inválido {self.id!r}; use minúsculas, dígitos e "
                "hífens (ex.: 'tesouro-direto')"
            )
        if not isinstance(self.nome, str) or not self.nome.strip():
            raise RegistryError(f"FetcherInfo {self.id!r}: nome é obrigatório")
        aliases = tuple(self.aliases)
        object.__setattr__(self, "aliases", aliases)
        seen: set[str] = set()
        for alias in aliases:
            if not isinstance(alias, str) or not _ALIAS_RE.match(alias):
                raise RegistryError(
                    f"FetcherInfo {self.id!r}: apelido inválido {alias!r}"
                )
            if alias == self.id:
                raise RegistryError(
                    f"FetcherInfo {self.id!r}: apelido {alias!r} repete o id"
                )
            if alias in seen:
                raise RegistryError(
                    f"FetcherInfo {self.id!r}: apelido {alias!r} repetido"
                )
            seen.add(alias)
        _opt_text("orgao", self.orgao, self.id)
        _opt_text("licenca", self.licenca, self.id)
        _opt_text("dominio", self.dominio, self.id)
        _opt_text("homepage", self.homepage, self.id)
        _opt_text("restricao_nota", self.restricao_nota, self.id)
        if self.homepage is not None and not self.homepage.startswith(
            ("http://", "https://")
        ):
            raise RegistryError(
                f"FetcherInfo {self.id!r}: homepage deve começar com http(s)://, "
                f"recebi {self.homepage!r}"
            )
        if self.cadencia is not None and self.cadencia not in CADENCIAS:
            raise RegistryError(
                f"FetcherInfo {self.id!r}: cadência {self.cadencia!r} desconhecida; "
                f"aceitas: {', '.join(CADENCIAS)}"
            )
        if (
            self.redistribuicao is not None
            and self.redistribuicao not in REDISTRIBUICOES
        ):
            raise RegistryError(
                f"FetcherInfo {self.id!r}: redistribuição {self.redistribuicao!r} "
                f"desconhecida; aceitas: {', '.join(REDISTRIBUICOES)}"
            )

    @property
    def names(self) -> tuple[str, ...]:
        """Id canônico seguido dos apelidos."""
        return (self.id, *self.aliases)

    def missing_fields(self) -> list[str]:
        """Campos de fato ainda sem valor (orgao, licenca, homepage, cadencia)."""
        return [
            name
            for name in ("orgao", "licenca", "homepage", "cadencia")
            if getattr(self, name) is None
        ]

    def to_dict(self) -> dict[str, Any]:
        """Dicionário estável para JSON (aliases como lista, ordem fixa)."""
        data = asdict(self)
        data["aliases"] = list(self.aliases)
        return data

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> FetcherInfo:
        """Reconstrói a partir de :meth:`to_dict`; chaves desconhecidas são erro."""
        known = {f.name for f in fields(cls)}
        extra = set(data) - known
        if extra:
            raise RegistryError(f"FetcherInfo: campos desconhecidos {sorted(extra)}")
        values = dict(data)
        values["aliases"] = tuple(values.get("aliases") or ())
        try:
            return cls(**values)
        except TypeError as exc:
            raise RegistryError(f"FetcherInfo: {exc}") from exc


@dataclass(frozen=True)
class DatasetEntry:
    """Um arquivo/dataset a baixar (união dos campos usados pelos fetchers)."""

    id: str
    base_id: str
    name: str
    url: str
    ext: str
    group: str
    source: str = ""
    year: int | None = None
    quarter: int | None = None
    semester: int | None = None
    month: int | None = None
    slug: str | None = None
    fallback_urls: list[str] | None = None
    # Validadores do recurso remoto (tamanho em bytes e last_modified ISO, UTC).
    remote_size: int | None = None
    remote_modified: str | None = None
    # Identificador estável do recurso, em pacotes com vários recursos por grupo.
    resource_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Converte a entrada em dict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DatasetEntry:
        """Cria a entrada a partir de um dict, ignorando chaves desconhecidas."""
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


@dataclass(frozen=True)
class GroupInfo:
    """Grupo lógico de entradas de dataset."""

    name: str
    entries: list[DatasetEntry] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Converte o grupo em dict."""
        return {
            "name": self.name,
            "entries": [e.to_dict() for e in self.entries],
        }


def is_redistributable(info: FetcherInfo) -> bool:
    """False só quando a fonte está marcada como ``restrita``.

    Fonte não avaliada (``None``) ou ``aberta`` continua redistribuível.
    """
    return info.redistribuicao != "restrita"


def resolve_alias(infos: Iterable[FetcherInfo], nome: str) -> FetcherInfo | None:
    """Acha o fetcher cujo id ou apelido é ``nome`` (``None`` se não houver)."""
    for info in infos:
        if nome in info.names:
            return info
    return None


def check_unique(infos: Iterable[FetcherInfo]) -> None:
    """Garante que nenhum id ou apelido aparece em dois fetchers.

    Raises:
        RegistryError: Com a lista dos nomes duplicados e de quem os declara.
    """
    owners: dict[str, list[str]] = {}
    for info in infos:
        for name in info.names:
            owners.setdefault(name, []).append(info.id)
    dups = {name: ids for name, ids in sorted(owners.items()) if len(ids) > 1}
    if dups:
        detail = "; ".join(f"{n!r} em {', '.join(ids)}" for n, ids in dups.items())
        raise RegistryError(f"ids/apelidos duplicados: {detail}")


def distribuicao_esperada(fetcher_id: str) -> str:
    """Distribuição que a regra prevê para o id: ``<id>-fetcher``."""
    return f"{fetcher_id}{DISTRIBUICAO_SUFIXO}"


@dataclass(frozen=True)
class IdentidadePacote:
    """Identidade de empacotamento de um fetcher, derivada e nunca declarada.

    Sai dos metadados da distribuição instalada (``importlib.metadata``): nome,
    versão, ``[project.urls]`` e os entry points. Ninguém escreve estes valores
    à mão, então eles não divergem do pacote real.

    Attributes:
        distribution: Nome da distribuição (``ibge-sidra-fetcher``).
        module: Pacote Python de topo (``ibge_sidra_fetcher``), do ``value`` do
            entry point.
        repo: URL do repositório (``[project.urls] Repository`` ou ``Source``).
        entry_point: Nome do entry point do grupo ``quantilica.fetchers``.
        lake_entry_point: Nome do entry point do grupo ``quantilica.lake``, se
            o pacote tem conversor para o lake.
        source_id: Id da fonte que o fetcher grava nos manifestos.
        lake_source_id: Id da fonte no lake (o nome do entry point do lake).
        version: Versão instalada da distribuição.
    """

    distribution: str | None = None
    module: str | None = None
    repo: str | None = None
    entry_point: str | None = None
    lake_entry_point: str | None = None
    source_id: str | None = None
    lake_source_id: str | None = None
    version: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Dicionário estável para JSON (ordem fixa dos campos)."""
        return asdict(self)


def _bool_field(name: str, value: object, where: str) -> bool:
    if not isinstance(value, bool):
        raise RegistryError(f"{where}: {name} deve ser true ou false, recebi {value!r}")
    return value


def _http_url(name: str, value: object, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RegistryError(f"{where}: {name} deve ser uma URL, recebi {value!r}")
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise RegistryError(
            f"{where}: {name} deve ser uma URL http(s) válida, recebi {value!r}"
        )
    return value


@dataclass(frozen=True)
class Renome:
    """Um estado anterior do fetcher: como ele se chamava antes de um renome.

    Attributes:
        distribution: Distribuição anterior (``sidra-fetcher``).
        repo: Repositório anterior, como ``Dono/nome`` ou URL.
        id: Id anterior (``sidra``).
        data: Data do renome, ISO 8601 (``2026-11-13``).
    """

    distribution: str
    repo: str
    id: str
    data: str

    def __post_init__(self) -> None:
        where = "Renome"
        if not isinstance(self.distribution, str) or not _DIST_RE.match(
            self.distribution
        ):
            raise RegistryError(f"{where}: distribution inválida {self.distribution!r}")
        if not isinstance(self.repo, str) or not (
            _REPO_SLUG_RE.match(self.repo)
            or (
                urlparse(self.repo).scheme in ("http", "https")
                and urlparse(self.repo).netloc
            )
        ):
            raise RegistryError(
                f"{where} {self.distribution!r}: repo deve ser 'Dono/nome' ou uma "
                f"URL http(s), recebi {self.repo!r}"
            )
        if not isinstance(self.id, str) or not _ALIAS_RE.match(self.id):
            raise RegistryError(
                f"{where} {self.distribution!r}: id inválido {self.id!r}"
            )
        data = self.data
        if isinstance(data, date):
            data = data.isoformat()
        try:
            if not isinstance(data, str):
                raise ValueError
            date.fromisoformat(data)
        except ValueError:
            raise RegistryError(
                f"{where} {self.distribution!r}: data deve ser ISO 8601 "
                f"(AAAA-MM-DD), recebi {self.data!r}"
            ) from None
        object.__setattr__(self, "data", data)

    def to_dict(self) -> dict[str, Any]:
        """Dicionário estável para JSON."""
        return asdict(self)


@dataclass(frozen=True)
class RegistroEcossistema:
    """Fatos do ecossistema sobre um fetcher: onde ele aparece e como.

    Vêm do ``registro.toml`` público (uma seção por id), não do pacote.

    Attributes:
        docs_pasta: Seção do docs em ``fontes/`` (ver :data:`DOCS_PASTAS`).
        vitrine: O fetcher tem cartão no site.
        ci_integracao: Entra no CI de integração do core.
        json_leve: Entra na série JSON leve da auditoria.
        mascote: Mascote do dashboard dos workers, se houver.
        health_url: URL da sonda do ``quantilica health``, se houver.
        historico: Nomes anteriores (renomes), do mais antigo ao mais recente.
    """

    docs_pasta: str
    vitrine: bool = False
    ci_integracao: bool = False
    json_leve: bool = False
    mascote: str | None = None
    health_url: str | None = None
    historico: tuple[Renome, ...] = ()

    def __post_init__(self) -> None:
        if self.docs_pasta not in DOCS_PASTAS:
            raise RegistryError(
                f"docs_pasta {self.docs_pasta!r} fora do vocabulário; aceitas: "
                f"{', '.join(DOCS_PASTAS)}"
            )
        for name in ("vitrine", "ci_integracao", "json_leve"):
            _bool_field(name, getattr(self, name), "RegistroEcossistema")
        if self.mascote is not None and (
            not isinstance(self.mascote, str) or not self.mascote.strip()
        ):
            raise RegistryError(
                f"mascote deve ser texto não vazio ou ausente, recebi {self.mascote!r}"
            )
        if self.health_url is not None:
            _http_url("health_url", self.health_url, "RegistroEcossistema")
        object.__setattr__(self, "historico", tuple(self.historico))

    def to_dict(self) -> dict[str, Any]:
        """Dicionário estável para JSON (``historico`` como lista de dicts)."""
        return {
            "docs_pasta": self.docs_pasta,
            "vitrine": self.vitrine,
            "ci_integracao": self.ci_integracao,
            "json_leve": self.json_leve,
            "mascote": self.mascote,
            "health_url": self.health_url,
            "historico": [r.to_dict() for r in self.historico],
        }


#: O que o registro emite para o fetcher que não tem seção no ``registro.toml``.
_ECOSSISTEMA_VAZIO: dict[str, Any] = {
    "docs_pasta": None,
    "vitrine": False,
    "ci_integracao": False,
    "json_leve": False,
    "mascote": None,
    "health_url": None,
    "historico": [],
}

_CAMPOS_ECOSSISTEMA = {f.name for f in fields(RegistroEcossistema)}
_CAMPOS_RENOME = {f.name for f in fields(Renome)}


def _renome_de(secao: str, i: int, raw: object) -> Renome:
    where = f"registro do ecossistema [{secao}].historico[{i}]"
    if not isinstance(raw, Mapping):
        raise RegistryError(f"{where}: esperava uma tabela, recebi {raw!r}")
    extra = set(raw) - _CAMPOS_RENOME
    if extra:
        raise RegistryError(f"{where}: campos desconhecidos {sorted(extra)}")
    faltam = sorted(_CAMPOS_RENOME - set(raw))
    if faltam:
        raise RegistryError(f"{where}: campos ausentes {faltam}")
    try:
        return Renome(**dict(raw))
    except RegistryError as exc:
        raise RegistryError(f"{where}: {exc}") from exc


def carrega_registro_ecossistema(
    path: str | Path, ids: Iterable[str] | None = None
) -> dict[str, RegistroEcossistema]:
    """Lê e valida o ``registro.toml`` (uma seção por id de fetcher).

    Valida: TOML bem formado; ids no formato do :class:`FetcherInfo`; campos
    desconhecidos ou ausentes; ``docs_pasta`` no vocabulário; ``health_url``
    como URL http(s); tipos dos booleanos; ``historico`` com data ISO e sem
    repetir distribuição.

    Args:
        path: Caminho do arquivo TOML.
        ids: Ids que existem no export. Quando dado, seção de id fora dele é
            erro (:func:`build_registry` repete esta checagem).

    Raises:
        RegistryError: Arquivo ilegível, TOML inválido ou conteúdo inválido.
    """
    path = Path(path)
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise RegistryError(f"registro do ecossistema ilegível: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise RegistryError(
            f"registro do ecossistema {path}: TOML inválido: {exc}"
        ) from exc
    out: dict[str, RegistroEcossistema] = {}
    for fid, secao in raw.items():
        if not _ID_RE.match(fid):
            raise RegistryError(f"registro do ecossistema: id inválido {fid!r}")
        if not isinstance(secao, Mapping):
            raise RegistryError(
                f"registro do ecossistema: [{fid}] deve ser uma seção (tabela)"
            )
        extra = set(secao) - _CAMPOS_ECOSSISTEMA
        if extra:
            raise RegistryError(
                f"registro do ecossistema [{fid}]: campos desconhecidos {sorted(extra)}"
            )
        if "docs_pasta" not in secao:
            raise RegistryError(
                f"registro do ecossistema [{fid}]: docs_pasta é obrigatório"
            )
        values = dict(secao)
        hist_raw = values.pop("historico", [])
        if not isinstance(hist_raw, list):
            raise RegistryError(
                f"registro do ecossistema [{fid}]: historico deve ser lista de tabelas"
            )
        historico = tuple(_renome_de(fid, i, r) for i, r in enumerate(hist_raw))
        dists = [r.distribution for r in historico]
        if len(set(dists)) != len(dists):
            raise RegistryError(
                f"registro do ecossistema [{fid}]: distribuição repetida no historico"
            )
        try:
            out[fid] = RegistroEcossistema(**values, historico=historico)
        except RegistryError as exc:
            raise RegistryError(f"registro do ecossistema [{fid}]: {exc}") from exc
    if ids is not None:
        _check_ids_do_ecossistema(out, set(ids))
    return out


def _check_ids_do_ecossistema(
    ecossistema: Mapping[str, RegistroEcossistema], ids: set[str]
) -> None:
    desconhecidos = sorted(set(ecossistema) - ids)
    if desconhecidos:
        raise RegistryError(
            "registro do ecossistema com id que não está no export: "
            + ", ".join(desconhecidos)
        )


def check_identidade(
    infos: Iterable[FetcherInfo],
    identidades: Mapping[str, IdentidadePacote],
    ecossistema: Mapping[str, RegistroEcossistema] | None = None,
) -> list[str]:
    """Problemas de identidade: distribuição fora da regra ``<id>-fetcher``.

    Uma distribuição que não segue a regra só passa se o ``historico`` do
    fetcher a explica, isto é, se ela aparece como distribuição anterior (o id
    mudou e a distribuição ficou). Sem ``ecossistema`` nada a explica.

    Returns:
        Uma mensagem por problema, ordenada por id (vazia quando está tudo ok).
    """
    problemas: list[str] = []
    for info in sorted(infos, key=lambda i: i.id):
        ident = identidades.get(info.id)
        dist = ident.distribution if ident else None
        if dist is None or dist == distribuicao_esperada(info.id):
            continue
        reg = ecossistema.get(info.id) if ecossistema else None
        if reg is not None and any(r.distribution == dist for r in reg.historico):
            continue
        problemas.append(
            f"{info.id}: distribuição {dist!r} foge da regra "
            f"{distribuicao_esperada(info.id)!r} e o historico não a explica"
        )
    return problemas


def registry_sha256(doc: Mapping[str, Any]) -> str:
    """SHA-256 do conteúdo do registro (``version`` e ``fetchers``).

    Calculado sobre o JSON canônico (chaves ordenadas, sem espaços, UTF-8),
    sem a própria chave ``sha256``: quem recebe o registro recalcula e compara.
    """
    content = {"version": doc["version"], "fetchers": doc["fetchers"]}
    canon = json.dumps(
        content, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def build_registry(
    infos: Iterable[FetcherInfo],
    identidades: Mapping[str, IdentidadePacote] | None = None,
    ecossistema: Mapping[str, RegistroEcossistema] | None = None,
    extras: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Monta o registro versionado, ordenado por id, juntando as três camadas.

    Os campos do :class:`FetcherInfo` vêm sempre; a identidade de empacotamento
    (``identidades``, por id) e os fatos do ecossistema (``ecossistema``, por
    id) entram quando dados. Com ``ecossistema``, o fetcher sem seção sai com os
    campos vazios (``docs_pasta`` nulo, bandeiras falsas, ``historico`` vazio).
    ``extras`` (por id) acrescenta campos de execução do chamador e não
    sobrescreve os da versão 1. O resultado leva o ``sha256`` do conteúdo.

    Raises:
        RegistryError: Id ou apelido duplicado, ou chave de ``identidades``,
            ``ecossistema`` ou ``extras`` que não é id de um fetcher dado.
    """
    ordered = sorted(infos, key=lambda i: i.id)
    check_unique(ordered)
    ids = {i.id for i in ordered}
    for nome, mapa in (
        ("identidades", identidades),
        ("extras", extras),
    ):
        if mapa:
            desconhecidos = sorted(set(mapa) - ids)
            if desconhecidos:
                raise RegistryError(
                    f"{nome} com id que não está no export: " + ", ".join(desconhecidos)
                )
    if ecossistema:
        _check_ids_do_ecossistema(ecossistema, ids)
    fetchers: list[dict[str, Any]] = []
    for info in ordered:
        item = info.to_dict()
        if identidades and info.id in identidades:
            item.update(identidades[info.id].to_dict())
        if ecossistema is not None:
            reg = ecossistema.get(info.id)
            item.update(reg.to_dict() if reg is not None else _ECOSSISTEMA_VAZIO)
        if extras and info.id in extras:
            for key, value in extras[info.id].items():
                item.setdefault(key, value)
        fetchers.append(item)
    doc: dict[str, Any] = {"version": REGISTRY_VERSION, "fetchers": fetchers}
    doc["sha256"] = registry_sha256(doc)
    return doc
