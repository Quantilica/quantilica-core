"""Guarda da suíte: nenhum teste pode deixar ``quantilica_extract_*`` no temp.

``decompress_archive()`` sem ``output_dir`` cria a pasta com ``mkdtemp`` e deixa a
remoção para o chamador; quem esquece vaza disco (em produção, RAM do ``/tmp``).
A fixture abaixo isola o ``TMPDIR`` da sessão numa pasta própria (assim o lixo de
outros processos no ``/tmp`` real não conta) e, ao fim, falha a suíte se sobrou
alguma raiz ``quantilica_extract_*``. A falha é um erro de teardown, que
``pytest-xdist`` repete em cada worker (cada um tem a sua pasta) e que deixa o
código de saída diferente de zero.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

PREFIXO_EXTRACAO = "quantilica_extract_"


@pytest.fixture(scope="session", autouse=True)
def guarda_extracao_sem_residuo(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[Path]:
    pasta = tmp_path_factory.mktemp("tmpdir_sessao")
    original = tempfile.tempdir
    mp = pytest.MonkeyPatch()
    mp.setenv("TMPDIR", str(pasta))
    mp.setattr(tempfile, "tempdir", None)
    try:
        yield pasta
    finally:
        mp.undo()
        tempfile.tempdir = original
    sobras = sorted(p.name for p in pasta.glob(f"{PREFIXO_EXTRACAO}*"))
    if sobras:
        pytest.fail(
            f"{len(sobras)} pasta(s) {PREFIXO_EXTRACAO}* ficaram no TMPDIR da "
            f"sessão ({pasta}): {', '.join(sobras[:10])}"
            f"{' ...' if len(sobras) > 10 else ''}. Use extracted_archive() ou "
            "remova a raiz da extração.",
            pytrace=False,
        )
