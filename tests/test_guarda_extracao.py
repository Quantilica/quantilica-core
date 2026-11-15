"""A guarda de sessão (``conftest.py``) isola o TMPDIR e confere as sobras ao fim."""

from __future__ import annotations

import tempfile
import zipfile
from pathlib import Path

from quantilica.core.files import extracted_archive


def test_extracted_archive_zip_com_subpasta_no_temp_da_sessao(
    tmp_path: Path, guarda_extracao_sem_residuo: Path
) -> None:
    """Sem isolar o TMPDIR: quem confere as sobras é a guarda de sessão."""
    assert Path(tempfile.gettempdir()) == guarda_extracao_sem_residuo
    raw = tmp_path / "raw.zip"
    with zipfile.ZipFile(raw, "w") as zf:
        zf.writestr("pasta/interna/dados.csv", "a;b\n1;2\n")
    with extracted_archive(raw) as inner:
        assert inner.name == "dados.csv"
        assert inner.parent.parent.parent.parent == guarda_extracao_sem_residuo
