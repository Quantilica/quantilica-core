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
    assert period_from_path("competencia=202408/x.parquet") == ("2024-08", "mensal")
    assert period_from_path("ano=2005/competencia=200301/x.parquet") == (
        "2005",
        "anual",
    )
    assert period_from_path("x.parquet") is None
    with pytest.raises(ValueError):
        period_from_path("competencia=202413/x.parquet")
    with pytest.raises(ValueError):
        period_from_path("competencia=2024/x.parquet")
    with pytest.raises(ValueError):
        period_from_path("ano=2005/mes=13/x.parquet")
    with pytest.raises(ValueError):
        period_from_path("ano=abc/x.parquet")


@pytest.mark.parametrize(
    ("rel", "esperado"),
    [
        ("ano=2024/semestre=1/x.parquet", ("2024-S1", "semestral")),
        ("ano=2024/semestre=02/x.parquet", ("2024-S2", "semestral")),
        ("ano=2024/trimestre=3/x.parquet", ("2024-T3", "trimestral")),
        ("ano=2024/trimestre=4/uf=SP/x.parquet", ("2024-T4", "trimestral")),
        ("ano=2024/mes=3/dia=5/x.parquet", ("2024-03-05", "diaria")),
        ("ano=2024/mes=02/dia=29/x.parquet", ("2024-02-29", "diaria")),
    ],
)
def test_period_from_path_granularidades(rel: str, esperado: tuple[str, str]) -> None:
    assert period_from_path(rel) == esperado


@pytest.mark.parametrize(
    "rel",
    [
        "ano=2024/semestre=0/x.parquet",
        "ano=2024/semestre=3/x.parquet",
        "ano=2024/semestre=a/x.parquet",
        "ano=2024/trimestre=0/x.parquet",
        "ano=2024/trimestre=5/x.parquet",
        "ano=2024/mes=2/dia=30/x.parquet",
        "ano=2023/mes=2/dia=29/x.parquet",
        "ano=2024/mes=4/dia=0/x.parquet",
        "ano=2024/mes=4/dia=x/x.parquet",
        "ano=2024/dia=5/x.parquet",  # dia sem mes
        "ano=2024/semestre=1/trimestre=1/x.parquet",  # ambíguo
        "ano=2024/semestre=1/mes=1/x.parquet",  # ambíguo
        "ano=2024/trimestre=1/mes=1/dia=1/x.parquet",  # ambíguo
    ],
)
def test_period_from_path_invalido(rel: str) -> None:
    with pytest.raises(ValueError):
        period_from_path(rel)


@pytest.mark.parametrize(
    "rel",
    [
        "ano=+2024/x.parquet",
        "ano=2024/mes=+3/x.parquet",
        "ano=2024/mes=1_1/x.parquet",
        "ano=2024/trimestre= 1/x.parquet",
        "ano=2024/semestre=1 /x.parquet",
        "ano=2024/mes=3/dia=\u0661/x.parquet",  # dígito arábico-índico
        "ano=\uff12\uff10\uff12\uff14/x.parquet",  # largura total
        "ano=-2024/x.parquet",
        "ano=2024/mes=/x.parquet",
        "competencia=2024\uff10\uff11/x.parquet",
        "competencia=+20241/x.parquet",
        "competencia=202401\n/x.parquet",
    ],
)
def test_period_from_path_so_digitos_ascii(rel: str) -> None:
    with pytest.raises(ValueError):
        period_from_path(rel)


@pytest.mark.parametrize(
    "rel",
    [
        "competencia=202401/semestre=1/x.parquet",
        "competencia=202401/trimestre=1/x.parquet",
        "competencia=202401/dia=5/x.parquet",
        "competencia=202401/trimestre=1/dia=5/x.parquet",
    ],
)
def test_competencia_com_outras_particoes_de_periodo_e_erro(rel: str) -> None:
    with pytest.raises(ValueError, match="incompatíveis"):
        period_from_path(rel)


def test_competencia_normaliza_mes_e_ano() -> None:
    assert period_from_path("competencia=202401/x.parquet") == ("2024-01", "mensal")
    assert period_from_path("competencia=000512/x.parquet") == ("0005-12", "mensal")


def test_ordem_lexicografica_e_cronologica() -> None:
    for gran, rels in {
        "semestral": [
            f"ano={a}/semestre={s}/x.parquet" for a in (2023, 2024) for s in (1, 2)
        ],
        "trimestral": [
            f"ano={a}/trimestre={t}/x.parquet"
            for a in (2023, 2024)
            for t in (1, 2, 3, 4)
        ],
        "diaria": [
            "ano=2024/mes=1/dia=31/x.parquet",
            "ano=2024/mes=2/dia=1/x.parquet",
            "ano=2024/mes=2/dia=9/x.parquet",
            "ano=2024/mes=2/dia=10/x.parquet",
            "ano=2025/mes=1/dia=1/x.parquet",
        ],
    }.items():
        periodos = []
        for rel in rels:
            found = period_from_path(rel)
            assert found is not None and found[1] == gran
            periodos.append(found[0])
        assert periodos == sorted(periodos)


def test_perfil_trimestral_e_eras(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "pgfn"
    write(base, "ano=2023/trimestre=4/p.parquet", {"a": [1, None], "trimestre": [4, 4]})
    write(base, "ano=2024/trimestre=1/p.parquet", {"a": [1], "b": ["x"]})
    write(base, "ano=2024/trimestre=2/p.parquet", {"a": [None], "b": ["y"]})
    res = profile_from_lake(tmp_path, "alfa", "pgfn")
    p = res.perfil
    assert p["granularidade"] == "trimestral"
    assert p["periodos"] == ["2023-T4", "2024-T1", "2024-T2"]
    assert p["linhas"] == [2, 1, 1]
    assert p["colunas"]["a"]["nulos"] == [1, 0, 1]
    assert p["colunas"]["b"]["nulos"] == [None, 0, 0]
    assert "trimestre" not in p["colunas"]
    assert {c["nome"]: (c["periodo_inicio"], c["periodo_fim"]) for c in res.colunas}[
        "b"
    ] == ("2024-T1", "2024-T2")
    assert [(e["inicio"], e["fim"]) for e in derivar_eras(p)] == [
        ("2023-T4", "2023-T4"),
        ("2024-T1", "2024-T2"),
    ]


def test_perfil_semestral_e_diario(tmp_path: Path) -> None:
    sem = tmp_path / "alfa" / "sem"
    write(sem, "ano=2023/semestre=2/p.parquet", {"a": [1]})
    write(sem, "ano=2024/semestre=1/p.parquet", {"a": [None]})
    p = profile_from_lake(tmp_path, "alfa", "sem").perfil
    assert (p["granularidade"], p["periodos"]) == ("semestral", ["2023-S2", "2024-S1"])
    dia = tmp_path / "alfa" / "dia"
    write(dia, "ano=2024/mes=01/dia=31/p.parquet", {"a": [1, 2]})
    write(dia, "ano=2024/mes=02/dia=01/p.parquet", {"a": [None]})
    p = profile_from_lake(tmp_path, "alfa", "dia").perfil
    assert p["granularidade"] == "diaria"
    assert p["periodos"] == ["2024-01-31", "2024-02-01"]
    assert p["colunas"]["a"]["nulos"] == [0, 1]


def test_granularidade_mista_entre_novas(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "misto"
    write(base, "ano=2024/trimestre=1/p.parquet", {"a": [1]})
    write(base, "ano=2024/semestre=2/p.parquet", {"a": [1]})
    with pytest.raises(ValueError, match="mista"):
        profile_from_lake(tmp_path, "alfa", "misto")


# --- colunas de membro, competencia= e dataset sem período -----------------------


def write_grouped(
    base: Path,
    rel: str,
    members: dict[str, dict],
    columns: dict[str, list],
    *,
    member_columns: list[str] | None = None,
) -> Path:
    """Parquet agrupado: ``quantilica.members`` e ``quantilica.member_columns``."""
    path = base / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    meta = {"quantilica.members": json.dumps(members)}
    if member_columns is not None:
        meta["quantilica.member_columns"] = json.dumps(member_columns)
    pq.write_table(pa.table(columns).replace_schema_metadata(meta), path)
    return path


def entry(rows: int, null_counts: dict | None = None, **cols) -> dict:
    out = {"origin_file": "x.csv", "origin_sha256": "0" * 64, "rows": rows, **cols}
    if null_counts is not None:
        out["null_counts"] = null_counts
    return out


def test_mes_de_membro_define_periodo_mensal(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    write_grouped(
        base,
        "ano=2024/sigtap_2024.parquet",
        {
            "a": entry(2, {"v": 1}, mes="01"),
            "b": entry(3, {"v": 2}, mes="02"),
        },
        {"v": [1, None, 3, None, None], "mes": ["01", "01", "02", "02", "02"]},
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["granularidade"] == "mensal"
    assert res.perfil["periodos"] == ["2024-01", "2024-02"]
    assert res.perfil["linhas"] == [2, 3]
    assert set(res.perfil["colunas"]) == {"v"}  # mes é coluna de membro
    # exato por membro, sem chave extra no documento
    assert res.perfil["colunas"]["v"] == {"nulos": [1, 2]}
    assert set(res.perfil) == {
        "versao",
        "granularidade",
        "periodos",
        "linhas",
        "colunas",
    }
    assert res.perfil["versao"] == 1
    assert [c["periodo_inicio"] for c in res.colunas] == ["2024-01"]


def test_mensal_por_membro_independe_dos_grupos_de_linhas(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    members = {
        "a": entry(3, {"v": 2, "cheia": 0}, mes="01"),
        "b": entry(2, {"v": 0, "cheia": 0}, mes="02"),
        "c": entry(2, {"v": 1, "cheia": 0}, mes="03"),
    }
    meses = ["01"] * 3 + ["02"] * 2 + ["03"] * 2
    path = base / "ano=2024/sigtap_2024.parquet"
    path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "v": [1, None, None, 4, 5, None, 7],
            "cheia": [1, 2, 3, 4, 5, 6, 7],
            "mes": meses,
        }
    ).replace_schema_metadata(
        {
            "quantilica.members": json.dumps(members),
            "quantilica.member_columns": json.dumps(["mes"]),
        }
    )
    docs = []
    for size in (1, 3, 7):  # um grupo cobrindo o ano todo é o caso do SIGTAP
        pq.write_table(table, path, row_group_size=size)
        res = profile_from_lake(tmp_path, "alfa", "sigtap")
        assert res.perfil["linhas"] == [3, 2, 2]
        assert res.perfil["colunas"]["v"] == {"nulos": [2, 0, 1]}
        assert res.perfil["colunas"]["cheia"] == {"nulos": [0, 0, 0]}
        docs.append(res.perfil)
    assert docs[0] == docs[1] == docs[2]


def test_mensal_por_membro_coluna_que_entra_no_meio_do_ano(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    write_grouped(
        base,
        "ano=2024/s.parquet",
        {
            "a": entry(2, {"v": 0}, mes="01"),
            "b": entry(2, {"v": 1, "nova": 2}, mes="02"),
            "c": entry(1, {"v": 0, "nova": 0}, mes="03"),
            "d": entry(2, {"v": 0, "nova": 1}, mes="03"),
        },
        {
            "v": [1, 2, 3, None, 5, 6, 7],
            "nova": [None, None, None, None, 5, 6, None],
            "mes": ["01", "01", "02", "02", "03", "03", "03"],
        },
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["periodos"] == ["2024-01", "2024-02", "2024-03"]
    assert res.perfil["linhas"] == [2, 2, 3]
    assert res.perfil["colunas"]["v"]["nulos"] == [0, 1, 0]
    # nenhum membro de janeiro traz "nova": None, e não "2 nulos de 2"
    assert res.perfil["colunas"]["nova"]["nulos"] == [None, 2, 1]
    nova = next(c for c in res.colunas if c["nome"] == "nova")
    assert (nova["periodo_inicio"], nova["periodo_fim"]) == ("2024-02", "2024-03")
    eras = derivar_eras(res.perfil)
    assert [(e["inicio"], e["fim"], e["entrou"]) for e in eras] == [
        ("2024-01", "2024-01", []),
        ("2024-02", "2024-03", ["nova"]),
    ]


def test_mensal_por_membro_mes_sem_linhas_e_descartado(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    write_grouped(
        base,
        "ano=2024/s.parquet",
        {
            "a": entry(2, {"v": 1}, mes="02"),
            "vazio": entry(0, {"v": 0}, mes="01"),
            "vazio2": entry(0, {"v": 0}, mes="12"),
        },
        {"v": [1, None], "mes": ["02", "02"]},
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["periodos"] == ["2024-02"]
    assert res.perfil["linhas"] == [2]
    assert res.perfil["colunas"]["v"] == {"nulos": [1]}


def test_mensal_por_membro_soma_membros_do_mesmo_mes(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    write_grouped(
        base,
        "ano=2024/s.parquet",
        {
            "a": entry(2, {"v": 1}, mes="01"),
            "b": entry(2, {"v": 2, "w": 0}, mes="01"),
        },
        {"v": [1, None, None, None], "w": [1, 2, 3, 4], "mes": ["01"] * 4},
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["linhas"] == [4]
    assert res.perfil["colunas"]["v"] == {"nulos": [3]}
    # "w" só em um dos dois membros: o outro não tinha a coluna e conta como nulo
    # inteiro, igual a dois arquivos do mesmo período num lake ano=/mes=
    assert res.perfil["colunas"]["w"] == {"nulos": [2]}


def test_mensal_por_membro_varios_anos_e_hash(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    for ano in (2023, 2024):
        write_grouped(
            base,
            f"ano={ano}/t.parquet",
            {"a": entry(1, {"v": 0}, mes="12"), "b": entry(2, {"v": 0}, mes="1")},
            {"v": [1, 2, 3], "mes": ["12", "01", "01"]},
            member_columns=["mes"],
        )
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["periodos"] == ["2023-01", "2023-12", "2024-01", "2024-12"]
    assert res.perfil["linhas"] == [2, 1, 2, 1]
    assert res.perfil["colunas"]["v"] == {"nulos": [0, 0, 0, 0]}
    assert res.perfil_sha256 == perfil_sha256(res.perfil)
    again = profile_from_lake(tmp_path, "alfa", "sigtap")  # regravar não muda
    assert again.perfil_sha256 == res.perfil_sha256


def test_mensal_por_membro_sem_null_counts_e_erro(tmp_path: Path) -> None:
    write_grouped(
        tmp_path / "alfa" / "d",
        "ano=2024/a.parquet",
        {"a": entry(1, {"v": 0}, mes="01"), "b": entry(1, mes="02")},
        {"v": [1, 2], "mes": ["01", "02"]},
        member_columns=["mes"],
    )
    with pytest.raises(ValueError, match="null_counts"):
        profile_from_lake(tmp_path, "alfa", "d")


def test_ano_sem_membros_segue_anual(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write(base, "ano=2024/a.parquet", {"v": [1, None]})
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert res.perfil["granularidade"] == "anual"
    assert res.perfil["periodos"] == ["2024"]
    assert res.perfil["linhas"] == [2]


def test_membros_sem_mes_seguem_anual(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write_grouped(
        base,
        "ano=2024/a.parquet",
        {"a": entry(2, uf="SP"), "b": entry(1, uf="RJ")},
        {"v": [1, None, 3], "uf": ["SP", "SP", "RJ"]},
        member_columns=["uf"],
    )
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert res.perfil["granularidade"] == "anual"
    assert res.perfil["periodos"] == ["2024"]
    assert res.perfil["linhas"] == [3]
    assert res.perfil["colunas"]["v"] == {"nulos": [1]}


def test_mensal_por_membro_misturado_com_particao_mes_e_erro(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write_grouped(
        base,
        "ano=2024/a.parquet",
        {"a": entry(1, {"v": 0}, mes="01")},
        {"v": [1], "mes": ["01"]},
        member_columns=["mes"],
    )
    write(base, "ano=2025/mes=01/b.parquet", {"v": [1]})
    with pytest.raises(ValueError, match="mista"):
        profile_from_lake(tmp_path, "alfa", "d")


def test_mensal_por_membro_misturado_com_anual_e_erro(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "d"
    write_grouped(
        base,
        "ano=2024/a.parquet",
        {"a": entry(1, {"v": 0}, mes="01")},
        {"v": [1], "mes": ["01"]},
        member_columns=["mes"],
    )
    write(base, "ano=2023/b.parquet", {"v": [1]})
    with pytest.raises(ValueError, match="mista"):
        profile_from_lake(tmp_path, "alfa", "d")


@pytest.mark.parametrize(
    ("members", "mes", "match"),
    [
        (
            {"a": entry(1, {"v": 0}, mes="01"), "b": entry(1, {"v": 0})},
            ["01", "01"],
            "sem mes",
        ),
        ({"a": entry(2, {"v": 0}, mes="13")}, ["13", "13"], "inválido"),
        ({"a": entry(2, {"v": 0}, mes="ab")}, ["ab", "ab"], "inválido"),
        ({"a": entry(5, {"v": 0}, mes="01")}, ["01", "01"], "somam 5"),
        ({"a": entry(2, {"v": -1}, mes="01")}, ["01", "01"], "null_counts inválido"),
    ],
)
def test_mensal_por_membro_rodape_inconsistente(
    tmp_path: Path, members: dict, mes: list[str], match: str
) -> None:
    write_grouped(
        tmp_path / "alfa" / "d",
        "ano=2024/a.parquet",
        members,
        {"v": [1, 2], "mes": mes},
        member_columns=["mes"],
    )
    with pytest.raises(ValueError, match=match):
        profile_from_lake(tmp_path, "alfa", "d")


def test_competencia_e_periodo_mensal(tmp_path: Path) -> None:
    base = tmp_path / "alfa" / "sigtap"
    write(base, "competencia=202401/a.parquet", {"v": [1, None]})
    write(base, "competencia=202402/a.parquet", {"v": [None, None, 3]})
    write(base, "competencia=202402/b.parquet", {"v": [4]})
    res = profile_from_lake(tmp_path, "alfa", "sigtap")
    assert res.perfil["granularidade"] == "mensal"
    assert res.perfil["periodos"] == ["2024-01", "2024-02"]
    assert res.perfil["linhas"] == [2, 4]
    assert res.perfil["colunas"]["v"]["nulos"] == [1, 2]


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


def test_mensal_por_membro_colunas_na_ordem_do_esquema(tmp_path: Path) -> None:
    write_grouped(
        tmp_path / "alfa" / "d",
        "ano=2024/a.parquet",
        {"a": entry(2, {"m": 0, "z": 0, "b": 1}, mes="01")},
        {"z": [1, 2], "b": [None, 1], "m": [1, 2], "mes": ["01", "01"]},
        member_columns=["mes"],
    )
    res = profile_from_lake(tmp_path, "alfa", "d")
    assert [c["nome"] for c in res.colunas] == ["z", "b", "m"]


def test_mensal_por_membro_nulos_acima_das_linhas_e_erro(tmp_path: Path) -> None:
    write_grouped(
        tmp_path / "alfa" / "d",
        "ano=2024/a.parquet",
        {"a": entry(2, {"v": 3}, mes="01")},
        {"v": [None, None], "mes": ["01", "01"]},
        member_columns=["mes"],
    )
    with pytest.raises(ValueError, match="maior que"):
        profile_from_lake(tmp_path, "alfa", "d")


@pytest.mark.parametrize("rows", [2, 0])
def test_mensal_por_membro_coluna_fora_do_esquema_e_erro(
    tmp_path: Path, rows: int
) -> None:
    write_grouped(
        tmp_path / "alfa" / "d",
        "ano=2024/a.parquet",
        {
            "a": entry(2, {"v": 0}, mes="01"),
            "b": entry(0, {"fantasma": 0}, mes="02"),
        }
        if rows == 0
        else {"a": entry(2, {"v": 0, "fantasma": 0}, mes="01")},
        {"v": [1, 2], "mes": ["01", "01"]},
        member_columns=["mes"],
    )
    with pytest.raises(ValueError, match="fantasma"):
        profile_from_lake(tmp_path, "alfa", "d")
