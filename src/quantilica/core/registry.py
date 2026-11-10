"""Modelo do registro de fetchers da Quantilica.

Declara os fatos de uma fonte de dados que pertencem ao pacote do fetcher
(:class:`FetcherInfo`), as dataclasses de catálogo que antes cada pacote
copiava (:class:`DatasetEntry`, :class:`GroupInfo`) e funções puras para
resolver apelidos, checar duplicidade e montar o ``registry.json``.

Este módulo não carrega infraestrutura (hosts, caminhos, horários, pools):
fatos de operação ficam fora dos pacotes.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Literal, get_args

__all__ = [
    "CADENCIAS",
    "REGISTRY_VERSION",
    "Cadencia",
    "DatasetEntry",
    "FetcherInfo",
    "GroupInfo",
    "RegistryError",
    "build_registry",
    "check_unique",
    "resolve_alias",
]

#: Versão do formato do ``registry.json``.
REGISTRY_VERSION = 1

#: Vocabulário de cadência esperada (o mesmo de ``expected_cadence`` nos
#: manifestos e do cálculo de atraso em ``quantilica manifests``), mais
#: ``irregular`` para fonte sem periodicidade fixa (sorteios, cargas avulsas):
#: declara que não há prazo de frescor, diferente de ``None`` (não informado).
Cadencia = Literal["daily", "weekly", "monthly", "quarterly", "yearly", "irregular"]
CADENCIAS: tuple[str, ...] = get_args(Cadencia)

_ID_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
# Apelidos aceitam também o sublinhado dos nomes históricos (``bcb_microdados``).
_ALIAS_RE = re.compile(r"^[a-z0-9]+([-_][a-z0-9]+)*$")


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
    """

    id: str
    nome: str
    aliases: tuple[str, ...] = ()
    orgao: str | None = None
    licenca: str | None = None
    homepage: str | None = None
    cadencia: Cadencia | None = None
    dominio: str | None = None

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


def build_registry(infos: Iterable[FetcherInfo]) -> dict[str, Any]:
    """Monta o registro versionado, ordenado por id.

    Raises:
        RegistryError: Se houver id ou apelido duplicado.
    """
    ordered = sorted(infos, key=lambda i: i.id)
    check_unique(ordered)
    return {
        "version": REGISTRY_VERSION,
        "fetchers": [i.to_dict() for i in ordered],
    }
