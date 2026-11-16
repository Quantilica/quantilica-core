"""Testes de ``quantilica.core.lake_profile`` com lake sintético em tmp_path."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantilica.core.docs import schema_from_lake
from quantilica.core.lake_profile import (
    DatasetSemPeriodoError,
    LakeProfile,
    canonical_json,
    derivar_eras,
    perfil_sha256,
    period_from_path,
    profile_from_lake,
)


def write(
    base: Path,
    rel: str,
    data: dict[str, list],
    *,
    row_group_size: int | None = None,
    **kwargs,
) -> Path:
    path = base / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(data), path, row_group_size=row_group_size, **kwargs)
    return path


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    """3 eras mensais: a, b | a, b, c (c entra) | a, c (b sai) | volta b."""
    base = tmp_path / "alfa" / "vendas"
    common = {"uf": ["SP", "RJ", "SP"], "_origem": ["x", "x", "x"]}
    write(
        base,
        "ano=2020/mes=01/p.parquet",
        {"a": [1, 2, None], "b": ["x", None, None], **common},
    )
    write(
        base,
        "ano=2020/mes=02/p.parquet",
        {"a": [1, 2, 3], "b": ["x", "y", "z"], **common},
    )
    # coluna c entra em 2020-03, no meio do ano; arquivo com 2 grupos de linhas
    write(
        base,
        "ano=2020/mes=03/p.parquet",
        {
            "a": [1, None, None, 4],
            "b": [None, None, "z", "w"],
            "c": [1.0, None, 3.0, 4.0],
            "uf": ["SP"] * 4,
            "_origem": ["x"] * 4,
        },
        row_group_size=2,
    )
    # b sai em 2020-04
    write(
        base,
        "ano=2020/mes=04/p.parquet",
        {"a": [1, 2], "c": [None, None], **{"uf": ["SP"] * 2, "_origem": ["x"] * 2}},
    )
    # b volta em 2020-06 (maio inexistente: lacuna de período, não de coluna)
    write(
        base,
        "ano=2020/mes=06/p.parquet",
        {"a": [1], "b": ["q"], "c": [2.0], "uf": ["SP"], "_origem": ["x"]},
    )
    # lixo que deve ser ignorado
    (base / "_dataset.json").write_text("{}", encoding="utf-8")
    (base / "ano=2020" / "mes=06" / "x.parquet.tmp").write_bytes(b"lixo")
    write(base, "_staging/a.parquet", {"zzz": [1]})
    return tmp_path


def test_perfil_varias_eras_e_grupos_de_linhas(lake: Path) -> None:
    res = profile_from_lake(lake, "alfa", "vendas")
    assert isinstance(res, LakeProfile)
    p = res.perfil
    assert p["versao"] == 1
    assert p["granularidade"] == "mensal"
    assert p["periodos"] == ["2020-01", "2020-02", "2020-03", "2020-04", "2020-06"]
    assert p["linhas"] == [3, 3, 4, 2, 1]
    # colunas de partição e físicas excluídas
    assert set(p["colunas"]) == {"a", "b", "c"}
    assert p["colunas"]["a"]["nulos"] == [1, 0, 2, 0, 0]
    # b: some em 2020-04 (None) e volta em 2020-06
    assert p["colunas"]["b"]["nulos"] == [2, 0, 2, None, 0]
    # c: entra em 2020-03; soma dos dois grupos de linhas (1 nulo)
    assert p["colunas"]["c"]["nulos"] == [None, None, 1, 2, 0]


def test_colunas_da_uniao(lake: Path) -> None:
    res = profile_from_lake(lake, "alfa", "vendas")
    assert res.colunas == [
        {
            "nome": "a",
            "dtype_arrow": "int64",
            "periodo_inicio": "2020-01",
            "periodo_fim": "2020-06",
        },
        {
            "nome": "b",
            "dtype_arrow": "string",
            "periodo_inicio": "2020-01",
            "periodo_fim": "2020-06",
        },
        {
            "nome": "c",
            "dtype_arrow": "double",
            "periodo_inicio": "2020-03",
            "periodo_fim": "2020-06",
        },
    ]


def test_hash_estavel_e_canonico(lake: Path) -> None:
    r1 = profile_from_lake(lake, "alfa", "vendas")
    r2 = profile_from_lake(lake, "alfa", "vendas")
    assert r1.perfil_sha256 == r2.perfil_sha256 == perfil_sha256(r1.perfil)
    assert len(r1.perfil_sha256) == 64
    text = canonical_json(r1.perfil)
    assert " " not in text
    assert json.loads(text) == r1.perfil
    # a ordem das chaves não muda o hash
    shuffled = dict(reversed(list(r1.perfil.items())))
    assert perfil_sha256(shuffled) == r1.perfil_sha256
    # conteúdo diferente, hash diferente
    other = json.loads(text)
    other["linhas"][0] += 1
    assert perfil_sha256(other) != r1.perfil_sha256


def test_periodo_com_varios_arquivos_soma(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write(base, "ano=2021/mes=1/uf=SP/p.parquet", {"a": [1, None], "b": [None, None]})
    write(base, "ano=2021/mes=1/uf=RJ/p.parquet", {"a": [None], "b": ["x"], "c": [1]})
    res = profile_from_lake(tmp_path, "alfa", "d")
    p = res.perfil
    assert p["periodos"] == ["2021-01"]  # mes=1 e mes=01 são o mesmo mês
    assert p["linhas"] == [3]
    assert p["colunas"]["a"]["nulos"] == [2]
    assert p["colunas"]["b"]["nulos"] == [2]
    # c só existe no arquivo do RJ: o arquivo de SP (2 linhas) conta como nulo
    assert p["colunas"]["c"]["nulos"] == [2]
    assert "uf" not in p["colunas"]  # chave de partição Hive excluída


def test_anual(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write(base, "ano=2019/p.parquet", {"a": [1]})
    write(base, "ano=2020/p.parquet", {"a": [None, 2]})
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert res.perfil["granularidade"] == "anual"
    assert res.perfil["periodos"] == ["2019", "2020"]
    assert res.perfil["colunas"]["a"]["nulos"] == [0, 1]


def test_erros(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        profile_from_lake(tmp_path, "alfa", "nada")
    base = tmp_path / "alfa" / "vazio"
    base.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        profile_from_lake(tmp_path, "alfa", "vazio")
    write(tmp_path / "alfa" / "semano", "p.parquet", {"a": [1]})
    with pytest.raises(ValueError, match="ano"):
        profile_from_lake(tmp_path, "alfa", "semano")
    misto = tmp_path / "alfa" / "misto"
    write(misto, "ano=2020/p.parquet", {"a": [1]})
    write(misto, "ano=2021/mes=01/p.parquet", {"a": [1]})
    with pytest.raises(ValueError, match="mista"):
        profile_from_lake(tmp_path, "alfa", "misto")


def test_fallback_sem_estatisticas_e_aninhada(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write(
        base,
        "ano=2020/mes=01/p.parquet",
        {"a": [1, None, None], "s": [{"x": 1}, None, {"x": 2}]},
        write_statistics=False,
    )
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert res.perfil["colunas"]["a"]["nulos"] == [2]
    assert res.perfil["colunas"]["s"]["nulos"] == [1]


def test_texto_vazio_nao_e_nulo(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write(base, "ano=2020/mes=01/p.parquet", {"t": ["", None, "x"]})
    assert profile_from_lake(tmp_path, "alfa", "d").perfil["colunas"]["t"]["nulos"] == [
        1
    ]


def test_exclude_customizado(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write(base, "ano=2020/p.parquet", {"a": [1], "uf": ["SP"], "lixo": [1]})
    res = profile_from_lake(tmp_path, "alfa", "d", exclude=["lixo"])
    assert set(res.perfil["colunas"]) == {"a", "uf"}


def test_period_from_path() -> None:
    assert period_from_path("ano=2005/mes=8/x.parquet") == ("2005-08", "mensal")
    assert period_from_path("ano=2005/uf=SP/x.parquet") == ("2005", "anual")
    assert period_from_path("x.parquet") is None
    with pytest.raises(ValueError):
        period_from_path("ano=2005/mes=13/x.parquet")
    with pytest.raises(ValueError):
        period_from_path("ano=abc/x.parquet")


# --- mês como coluna de membro -------------------------------------------------


def write_grouped(
    base: Path,
    rel: str,
    members: dict[str, dict],
    columns: dict[str, list],
    *,
    member_columns: list[str] | None = None,
    row_group_size: int | None = None,
) -> Path:
    """Parquet agrupado: ``quantilica.members`` e ``quantilica.member_columns``."""
    path = base / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    meta = {"quantilica.members": json.dumps(members)}
    if member_columns is not None:
        meta["quantilica.member_columns"] = json.dumps(member_columns)
    table = pa.table(columns).replace_schema_metadata(meta)
    pq.write_table(table, path, row_group_size=row_group_size)
    return path


def entry(rows: int, **cols) -> dict:
    return {"origin_file": "x.csv", "origin_sha256": "0" * 64, "rows": rows, **cols}


def test_mes_de_membro_com_grupos_de_um_mes_so_nao_le_dados(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "alfa" / "sigtap"
    # Um grupo de linhas por mês: as estatísticas de ``mes`` bastam.
    write_grouped(
        base,
        "ano=2024/sigtap_2024.parquet",
        {"a-202401": entry(2, mes=1), "a-202402": entry(3, mes=2)},
        {
            "v": [1, None, 3, None, None],
            "mes": [1, 1, 2, 2, 2],
            "_origem": ["a-202401"] * 2 + ["a-202402"] * 3,
        },
        member_columns=["mes"],
        row_group_size=2,
    )
    # row_group_size=2 parte o mês 2 em dois grupos (2 + 1 linhas), ambos de um mês.
    monkeypatch.setattr(
        pq.ParquetFile,
        "read_row_group",
        lambda *a, **k: pytest.fail("não deve ler dados"),
    )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["granularidade"] == "mensal"
    assert res.perfil["periodos"] == ["2024-01", "2024-02"]
    assert res.perfil["linhas"] == [2, 3]
    assert res.perfil["colunas"] == {"v": {"nulos": [1, 2]}}  # sem mes nem _origem


def test_mes_de_membro_com_grupo_que_mistura_meses(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    write_grouped(
        base,
        "ano=2024/sigtap_2024.parquet",
        {"a-1": entry(3, mes=1), "a-2": entry(2, mes="02")},
        {
            "v": [None, 2, None, None, 5],
            "mes": [1, 1, 1, 2, 2],
            "_origem": ["a-1"] * 3 + ["a-2"] * 2,
        },
        member_columns=["mes"],
    )  # um grupo só, com os dois meses
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["periodos"] == ["2024-01", "2024-02"]
    assert res.perfil["linhas"] == [3, 2]
    assert res.perfil["colunas"]["v"]["nulos"] == [2, 1]
    assert [c["nome"] for c in res.colunas] == ["v"]
    assert res.colunas[0]["periodo_inicio"] == "2024-01"


def test_mes_de_membro_soma_arquivos_e_anos(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    for ano in (2023, 2024):
        write_grouped(
            base,
            f"ano={ano}/tabela_a.parquet",
            {"m": entry(2, mes=12)},
            {"v": [1, None], "mes": [12, 12]},
            member_columns=["mes"],
        )
        write_grouped(
            base,
            f"ano={ano}/tabela_b.parquet",
            {"m": entry(1, mes=12)},
            {"v": [None], "mes": [12]},
            member_columns=["mes"],
        )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["periodos"] == ["2023-12", "2024-12"]
    assert res.perfil["linhas"] == [3, 3]
    assert res.perfil["colunas"]["v"]["nulos"] == [2, 2]


def test_mes_de_membro_ausente_agrega_como_anual(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    # Sem ``quantilica.members``: anual, sem falhar.
    write(base, "ano=2024/a.parquet", {"v": [1, None], "mes": [1, 2]})
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert res.perfil["granularidade"] == "anual"
    assert res.perfil["periodos"] == ["2024"]
    assert res.perfil["linhas"] == [2]

    # Um arquivo sem mês por membro faz o dataset inteiro sair anual.
    base = tmp_path / "alfa" / "e"
    write_grouped(
        base,
        "ano=2024/a.parquet",
        {"m": entry(2, mes=1)},
        {"v": [1, None], "mes": [1, 1]},
        member_columns=["mes"],
    )
    write_grouped(
        base,
        "ano=2024/b.parquet",
        {"m": entry(1)},  # membro sem mes
        {"v": [1], "mes": [3]},
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "e")
    assert res.perfil["granularidade"] == "anual"
    assert res.perfil["linhas"] == [3]
    assert res.perfil["colunas"]["v"]["nulos"] == [1]


def test_mes_dos_dados_fora_do_rodape_agrega_como_anual(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write_grouped(
        base,
        "ano=2024/a.parquet",
        {"m": entry(2, mes=1)},
        {"v": [1, 2], "mes": [1, 2]},  # o dado tem o mês 2; o rodapé só o 1
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert res.perfil["granularidade"] == "anual"
    assert res.perfil["linhas"] == [2]


def test_colunas_de_membro_do_rodape_saem_do_perfil(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write_grouped(
        base,
        "ano=2024/a.parquet",
        {"m": entry(2, serie="s1")},
        {"v": [1, None], "serie": ["s1", "s1"], "_origem": ["m", "m"]},
        member_columns=["serie"],
    )
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert set(res.perfil["colunas"]) == {"v"}
    assert res.perfil["granularidade"] == "anual"  # mes não é coluna de membro


def test_dataset_sem_ano_erro_unico_do_dataset(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "ipea"
    for i in range(3):
        write(base, f"base=a/p{i}.parquet", {"v": [i]})
    with pytest.raises(DatasetSemPeriodoError, match="3 arquivos") as err:
        profile_from_lake(tmp_path, "alfa", "ipea")
    assert isinstance(err.value, ValueError)  # quem capturava ValueError segue igual

    misto = tmp_path / "alfa" / "misto2"
    write(misto, "ano=2020/p.parquet", {"v": [1]})
    write(misto, "solto.parquet", {"v": [1]})
    with pytest.raises(ValueError, match="1 de 2 arquivos"):
        profile_from_lake(tmp_path, "alfa", "misto2")


# --- derivar_eras ----------------------------------------------------------------


def test_derivar_eras(lake: Path) -> None:
    perfil = profile_from_lake(lake, "alfa", "vendas").perfil
    eras = derivar_eras(perfil)
    assert eras == [
        {
            "inicio": "2020-01",
            "fim": "2020-02",
            "n_colunas": 2,
            "entrou": [],
            "saiu": [],
        },
        {
            "inicio": "2020-03",
            "fim": "2020-03",
            "n_colunas": 3,
            "entrou": ["c"],
            "saiu": [],
        },
        {
            "inicio": "2020-04",
            "fim": "2020-04",
            "n_colunas": 2,
            "entrou": [],
            "saiu": ["b"],
        },
        {
            "inicio": "2020-06",
            "fim": "2020-06",
            "n_colunas": 3,
            "entrou": ["b"],
            "saiu": [],
        },
    ]


def test_derivar_eras_pura_e_vazia() -> None:
    assert derivar_eras({"periodos": [], "linhas": [], "colunas": {}}) == []
    perfil = {
        "periodos": ["1992", "1993", "1994"],
        "linhas": [1, 1, 1],
        "colunas": {"a": {"nulos": [0, 0, 0]}, "z": {"nulos": [None, 5, 5]}},
    }
    copia = json.loads(json.dumps(perfil))
    eras = derivar_eras(perfil)
    assert perfil == copia
    assert [(e["inicio"], e["fim"]) for e in eras] == [
        ("1992", "1992"),
        ("1993", "1994"),
    ]
    assert eras[1]["entrou"] == ["z"]


# --- schema.json estendido -------------------------------------------------------


def test_schema_from_lake_com_periodos(lake: Path) -> None:
    schema = schema_from_lake(lake, "alfa", "vendas")
    cols = {c["nome"]: c for c in schema["colunas"]}
    assert cols["a"]["periodo_inicio"] == "2020-01"
    assert cols["a"]["periodo_fim"] == "2020-06"
    assert cols["c"]["periodo_inicio"] == "2020-03"
    # colunas físicas uf/_origem seguem no esquema, com período
    assert cols["uf"]["periodo_inicio"] == "2020-01"
    assert schema["particoes"] == ["ano", "mes"]
    assert set(cols["a"]) == {"nome", "tipos", "periodo_inicio", "periodo_fim"}


def test_schema_from_lake_sem_particao_ano_omite_periodos(tmp_path: Path) -> None:
    write(tmp_path / "alfa" / "d", "p.parquet", {"a": [1]})
    schema = schema_from_lake(tmp_path, "alfa", "d")
    assert schema["colunas"] == [{"nome": "a", "tipos": ["int64"]}]
