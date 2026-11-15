import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import pytest

from quantilica.core.exceptions import StorageError
from quantilica.core.files import (
    check_free_space,
    decompress_archive,
    ensure_dir,
    ensure_parent,
    extracted_archive,
    is_complete_file,
    sha256_bytes,
    sha256_file,
    write_bytes_atomic,
    write_text_atomic,
)


def test_ensure_dir_creates_directory(tmp_path):
    path = ensure_dir(tmp_path / "nested" / "dir")

    assert path.exists()
    assert path.is_dir()


def test_is_complete_file_missing(tmp_path):
    assert is_complete_file(tmp_path / "nope.bin") is False


def test_is_complete_file_exists_no_size_check(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")

    assert is_complete_file(target) is True


def test_is_complete_file_size_match(tmp_path):
    target = tmp_path / "data.bin"
    target.write_bytes(b"abc")

    assert is_complete_file(target, expected_size=3) is True
    assert is_complete_file(target, expected_size=99) is False


def test_is_complete_file_rejects_directory(tmp_path):
    assert is_complete_file(tmp_path) is False


def test_ensure_parent_creates_parent_directory(tmp_path):
    path = ensure_parent(tmp_path / "nested" / "file.txt")

    assert path.parent.exists()
    assert path.name == "file.txt"


def test_sha256_bytes_and_file_match(tmp_path):
    content = b"quantilica"
    path = tmp_path / "data.bin"
    path.write_bytes(content)

    assert sha256_file(path) == sha256_bytes(content)


def test_write_bytes_atomic_writes_content(tmp_path):
    path = write_bytes_atomic(tmp_path / "nested" / "data.bin", b"abc")

    assert path.read_bytes() == b"abc"


def test_write_text_atomic_writes_text(tmp_path):
    path = write_text_atomic(tmp_path / "nested" / "data.txt", "olá")

    assert path.read_text(encoding="utf-8") == "olá"


def test_sha256_file_raises_storage_error_for_missing_file(tmp_path):
    with pytest.raises(StorageError):
        sha256_file(tmp_path / "missing.bin")


def test_check_free_space(tmp_path):
    assert check_free_space(tmp_path, required_bytes=100) is True
    assert check_free_space(tmp_path, required_bytes=10**18) is False


def test_check_free_space_never_creates_target_without_suffix(tmp_path):
    target = tmp_path / "2026-05" / "API pública"
    assert check_free_space(target, required_bytes=100) is True
    assert not target.exists()
    assert not target.parent.exists()


# ---------------------------------------------------------------------------
# decompress_archive
# ---------------------------------------------------------------------------


# O caminho legado (``output_dir=None``) emite DeprecationWarning de propósito;
# os testes antigos o exercitam sem tratá-lo como falha.
pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def _make_zip(tmp_path: Path, members: dict[str, bytes]) -> Path:
    archive = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return archive


def test_decompress_archive_requires_existing_file(tmp_path):
    with pytest.raises(StorageError, match="Archive not found"):
        decompress_archive(tmp_path / "missing.zip")


def test_decompress_archive_output_dir_must_be_directory(tmp_path):
    file_path = tmp_path / "occupied.txt"
    file_path.write_text("occupied")
    archive = _make_zip(tmp_path, {"data.csv": b"a,b\n1,2\n"})
    with pytest.raises(StorageError, match="not a directory"):
        decompress_archive(archive, output_dir=file_path)


def test_decompress_archive_unpacks_zip_to_temp_dir(tmp_path):
    archive = _make_zip(tmp_path, {"only.csv": b"a,b\n1,2\n"})
    out = decompress_archive(archive)
    assert out.is_file()
    assert out.name == "only.csv"
    assert out.read_bytes() == b"a,b\n1,2\n"
    assert out.parent.name.startswith("quantilica_extract_")


def test_decompress_archive_unpacks_zip_to_given_dir(tmp_path):
    archive = _make_zip(tmp_path, {"nested/file.txt": b"hello"})
    out_dir = tmp_path / "out"
    out = decompress_archive(archive, output_dir=out_dir)
    assert out == out_dir / "nested" / "file.txt"
    assert (out_dir / "nested" / "file.txt").read_bytes() == b"hello"


def test_decompress_archive_tar_and_tar_gz(tmp_path):
    import io
    import tarfile

    for name, mode in (("archive.tar", "w"), ("archive.tar.gz", "w:gz")):
        ar_path = tmp_path / name
        with tarfile.open(ar_path, mode) as tf:
            info = tarfile.TarInfo("data.csv")
            payload = b"x,y\n1,2\n"
            info.size = len(payload)
            tf.addfile(info, io.BytesIO(payload))

        out = decompress_archive(ar_path)
        assert out.name == "data.csv"
        assert out.read_bytes() == b"x,y\n1,2\n"


def test_decompress_archive_target_extensions_filters_first_match(tmp_path):
    archive = _make_zip(
        tmp_path,
        {"readme.txt": b"notes", "z.csv": b"a\n1\n", "y.json": b"{}"},
    )
    out = decompress_archive(archive, target_extensions=(".json",))
    assert out.name == "y.json"


def test_decompress_archive_target_extensions_accepts_list_without_dot(tmp_path):
    archive = _make_zip(tmp_path, {"a.csv": b"1", "b.json": b"{}"})
    out = decompress_archive(archive, target_extensions=["csv"])
    assert out.name == "a.csv"


def test_decompress_archive_target_extensions_no_match_raises(tmp_path):
    archive = _make_zip(tmp_path, {"docs/readme.txt": b"notes"})
    with pytest.raises(StorageError, match="No extracted file matches"):
        decompress_archive(archive, target_extensions=(".csv",))


def test_decompress_archive_picks_common_data_file_over_dirs(tmp_path):
    archive = _make_zip(
        tmp_path,
        {"docs/table.csv": b"a\n1\n", "docs/table.csv.idx": b"idx"},
    )
    out = decompress_archive(archive)
    assert out.name == "table.csv"


def test_decompress_archive_multiple_unknown_files_returns_dir(tmp_path):
    archive = _make_zip(tmp_path, {"one.bin": b"x", "two.bin": b"y"})
    out_dir = tmp_path / "dest"
    out_dir.mkdir()
    out = decompress_archive(archive, output_dir=out_dir)
    assert out == out_dir


def test_decompress_archive_blocks_zip_slip(tmp_path):
    archive = tmp_path / "evil.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("../escaped.txt", b"boom")
        zf.writestr("ok.txt", b"fine")
    with pytest.raises(StorageError, match="path traversal"):
        decompress_archive(archive)


def test_decompress_archive_blocks_absolute_zip_member(tmp_path):
    archive = tmp_path / "evil_abs.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("/etc/escaped.txt", b"boom")
    with pytest.raises(StorageError, match="path traversal"):
        decompress_archive(archive)


def test_decompress_archive_invalid_zip_raises(tmp_path):
    bogus = tmp_path / "bogus.zip"
    bogus.write_bytes(b"definitely not a zip file")
    with pytest.raises(StorageError, match="Invalid ZIP"):
        decompress_archive(bogus)


def test_decompress_archive_invalid_tar_raises(tmp_path):
    bogus = tmp_path / "bogus.tar"
    bogus.write_bytes(b"definitely not a tar file")
    with pytest.raises(StorageError, match="Invalid TAR"):
        decompress_archive(bogus)


def test_decompress_archive_empty_zip_raises(tmp_path):
    archive = _make_zip(tmp_path, {})
    with pytest.raises(StorageError, match="produced no files"):
        decompress_archive(archive)


def test_decompress_archive_unsupported_suffix_falls_back_to_seven_zip(
    tmp_path, monkeypatch
):
    """Extensão desconhecida: tenta 7z; se ausente, erro claro."""
    import quantilica.core.files as files_mod

    bogus = tmp_path / "bogus.foo"
    bogus.write_bytes(b"data")
    monkeypatch.setattr(
        files_mod.subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError()),
    )
    with pytest.raises(StorageError, match="'7z' binary"):
        decompress_archive(bogus)


def test_decompress_archive_7z_uses_subprocess_fallback(tmp_path, monkeypatch):
    import quantilica.core.files as files_mod

    archive = tmp_path / "data.7z"
    archive.write_bytes(b"7z fake bytes")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    called: dict[str, object] = {}

    def fake_run(cmd, **kwargs):
        called["cmd"] = cmd
        (out_dir / "payload.csv").write_bytes(b"a\n1\n")
        result = type("R", (), {"returncode": 0, "stderr": b""})()
        return result

    monkeypatch.setattr(files_mod.subprocess, "run", fake_run)
    out = decompress_archive(archive, output_dir=out_dir)
    assert called["cmd"][0] == "7z"
    assert str(archive) in called["cmd"]
    assert out == out_dir / "payload.csv"


def test_decompress_archive_7z_failure_raises_storage_error(tmp_path, monkeypatch):
    import quantilica.core.files as files_mod

    archive = tmp_path / "data.7z"
    archive.write_bytes(b"7z fake bytes")

    def fake_run(cmd, **kwargs):
        result = type("R", (), {"returncode": 2, "stderr": b"Cannot open file"})()
        return result

    monkeypatch.setattr(files_mod.subprocess, "run", fake_run)
    with pytest.raises(StorageError, match="7z failed to decompress"):
        decompress_archive(archive)


def test_decompress_archive_missing_7z_binary_raises_clear_error(tmp_path, monkeypatch):
    import quantilica.core.files as files_mod

    archive = tmp_path / "data.7z"
    archive.write_bytes(b"7z bytes")
    monkeypatch.setattr(
        files_mod.subprocess,
        "run",
        lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError()),
    )
    with pytest.raises(StorageError, match="'7z' binary"):
        decompress_archive(archive)


def test_decompress_archive_rar_falls_back_to_seven_zip(tmp_path, monkeypatch):
    import quantilica.core.files as files_mod

    archive = tmp_path / "data.rar"
    archive.write_bytes(b"rar bytes")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    def fake_run(cmd, **kwargs):
        (out_dir / "table.xlsx").write_bytes(b"xlsx")
        return type("R", (), {"returncode": 0, "stderr": b""})()

    monkeypatch.setattr(files_mod.subprocess, "run", fake_run)
    out = decompress_archive(archive, output_dir=out_dir)
    assert out == out_dir / "table.xlsx"


def test_decompress_archive_exported_from_package_root():
    import quantilica.core

    assert quantilica.core.decompress_archive is decompress_archive
    assert "decompress_archive" in quantilica.core.__all__
    assert sys.version_info >= (3, 12)


def test_decompress_archive_ignora_arquivos_de_lixo(tmp_path):
    """Lock do Office, checksum, Thumbs.db, .DS_Store e __MACOSX não são escolhidos."""
    archive = _make_zip(
        tmp_path,
        {
            "__MACOSX/dados.csv": b"lixo",
            "~$dados.xlsx": b"lock",
            "md5_dados.txt": b"abc  dados.csv",
            "MD5_outro.csv": b"abc",
            "Thumbs.db": b"x",
            ".DS_Store": b"x",
            "dados.csv": b"a,b\n1,2\n",
        },
    )
    out = tmp_path / "out"
    result = decompress_archive(archive, out)
    assert result.name == "dados.csv"
    assert "__MACOSX" not in result.parts


def test_decompress_archive_junk_nao_vence_com_target_extensions(tmp_path):
    archive = _make_zip(
        tmp_path,
        {"~$dados.xlsx": b"lock", "pasta/dados.xlsx": b"real"},
    )
    result = decompress_archive(archive, tmp_path / "out", (".xlsx",))
    assert result.name == "dados.xlsx"


def test_decompress_archive_escolha_normal_inalterada(tmp_path):
    archive = _make_zip(tmp_path, {"leia.pdf": b"x", "a.csv": b"1", "b.csv": b"2"})
    assert decompress_archive(archive, tmp_path / "out").name == "a.csv"


def test_decompress_archive_so_lixo_devolve_diretorio(tmp_path):
    archive = _make_zip(tmp_path, {"Thumbs.db": b"x", "md5_a.txt": b"x"})
    out = tmp_path / "out"
    assert decompress_archive(archive, out) == out


# ---------------------------------------------------------------------------
# extracted_archive (extração com dono) e aviso de descontinuação
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def tmpdir_isolado(tmp_path, monkeypatch):
    """Aponta TMPDIR para uma pasta vazia (nada vaza para o /tmp real)."""
    pasta = tmp_path / "tmpdir"
    pasta.mkdir()
    monkeypatch.setenv("TMPDIR", str(pasta))
    monkeypatch.setattr(tempfile, "tempdir", None)
    return pasta


def _sobras(pasta: Path) -> list[Path]:
    return sorted(pasta.glob("quantilica_extract_*"))


def test_extracted_archive_removes_root_on_success(tmp_path, tmpdir_isolado):
    archive = _make_zip(tmp_path, {"a.csv": b"x\n1\n"})
    with extracted_archive(archive) as path:
        assert path.read_bytes() == b"x\n1\n"
        assert _sobras(tmpdir_isolado)
    assert not path.exists()
    assert _sobras(tmpdir_isolado) == []


def test_extracted_archive_removes_root_when_body_raises(tmp_path, tmpdir_isolado):
    archive = _make_zip(tmp_path, {"a.csv": b"x\n1\n"})
    with pytest.raises(ValueError, match="falha do chamador"):
        with extracted_archive(archive):
            raise ValueError("falha do chamador")
    assert _sobras(tmpdir_isolado) == []


def test_extracted_archive_removes_root_when_extraction_fails(tmp_path, tmpdir_isolado):
    bogus = tmp_path / "bogus.zip"
    bogus.write_bytes(b"not a zip")
    with pytest.raises(StorageError):
        with extracted_archive(bogus):
            pass  # pragma: no cover
    assert _sobras(tmpdir_isolado) == []


def test_extracted_archive_missing_archive_creates_nothing(tmp_path, tmpdir_isolado):
    with pytest.raises(StorageError, match="Archive not found"):
        with extracted_archive(tmp_path / "missing.zip"):
            pass  # pragma: no cover
    assert _sobras(tmpdir_isolado) == []


def test_extracted_archive_zip_with_subfolder_leaves_no_root(tmp_path, tmpdir_isolado):
    """Regressão: com subpasta o resultado fica aninhado e a raiz vazava.

    O chamador antigo fazia ``rmtree(resultado.parent)`` e a raiz
    ``quantilica_extract_*`` sobrava.
    """
    archive = _make_zip(tmp_path, {"pasta/interna/dados.csv": b"x\n1\n"})
    with extracted_archive(archive, target_extensions=(".csv",)) as path:
        assert path.name == "dados.csv"
        assert path.parent.name == "interna"
        assert path.parent != _sobras(tmpdir_isolado)[0]
    assert _sobras(tmpdir_isolado) == []
    assert list(tmpdir_isolado.iterdir()) == []


def test_legacy_parent_rmtree_leaks_root_with_subfolder(tmp_path, tmpdir_isolado):
    """Documenta o vazamento do uso antigo (e que o gerenciador o evita)."""
    archive = _make_zip(tmp_path, {"pasta/dados.csv": b"x\n1\n"})
    inner = decompress_archive(archive)
    shutil.rmtree(inner.parent)
    assert len(_sobras(tmpdir_isolado)) == 1


def test_extracted_archive_dir_holds_the_root(tmp_path):
    archive = _make_zip(tmp_path, {"a.csv": b"x\n1\n"})
    base = tmp_path / "novo" / "base"
    with extracted_archive(archive, dir=base) as path:
        assert path.parent.parent == base
        assert path.parent.name.startswith("quantilica_extract_")
    assert base.is_dir()
    assert list(base.iterdir()) == []


def test_extracted_archive_yields_root_when_no_unique_file(tmp_path, tmpdir_isolado):
    archive = _make_zip(tmp_path, {"a.bin": b"1", "b.bin": b"2"})
    with extracted_archive(archive) as path:
        assert path.is_dir()
        assert path.name.startswith("quantilica_extract_")
    assert not path.exists()


def test_extracted_archive_removes_readonly_entries(tmp_path, tmpdir_isolado):
    archive = _make_zip(tmp_path, {"sub/dados.csv": b"x\n1\n"})
    with extracted_archive(archive, target_extensions=(".csv",)) as path:
        path.chmod(0o444)
        path.parent.chmod(0o555)
    assert _sobras(tmpdir_isolado) == []


def test_extracted_archive_removal_failure_is_warning_not_exception(
    tmp_path, monkeypatch
):
    import quantilica.core.files as files_mod

    archive = _make_zip(tmp_path, {"a.csv": b"x\n1\n"})

    def rmtree_quebrado(path, onexc=None, **kwargs):
        raise OSError("disco cheio de rancor")

    monkeypatch.setattr(files_mod.shutil, "rmtree", rmtree_quebrado)
    with pytest.warns(RuntimeWarning, match="Could not remove"):
        with extracted_archive(archive) as path:
            assert path.exists()
    monkeypatch.undo()
    shutil.rmtree(path.parent, ignore_errors=True)


def test_extracted_archive_removal_failure_does_not_mask_original(
    tmp_path, monkeypatch
):
    import quantilica.core.files as files_mod

    archive = _make_zip(tmp_path, {"a.csv": b"x\n1\n"})
    raiz: list[Path] = []

    def rmtree_quebrado(path, onexc=None, **kwargs):
        raiz.append(Path(path))
        raise OSError("sem permissão")

    monkeypatch.setattr(files_mod.shutil, "rmtree", rmtree_quebrado)
    with pytest.warns(RuntimeWarning, match="Could not remove"):
        with pytest.raises(ValueError, match="original"):
            with extracted_archive(archive):
                raise ValueError("original")
    monkeypatch.undo()
    shutil.rmtree(raiz[0], ignore_errors=True)


def test_extracted_archive_explicit_output_dir_is_never_removed(tmp_path):
    """``decompress_archive`` com ``output_dir`` não remove nem avisa."""
    archive = _make_zip(tmp_path, {"sub/a.csv": b"x\n1\n"})
    out_dir = tmp_path / "out"
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = decompress_archive(archive, output_dir=out_dir)
    assert result.read_bytes() == b"x\n1\n"
    assert out_dir.is_dir()
    assert (out_dir / "sub" / "a.csv").exists()


def test_decompress_archive_without_output_dir_warns_deprecation(
    tmp_path, tmpdir_isolado
):
    archive = _make_zip(tmp_path, {"a.csv": b"x\n1\n"})
    with pytest.warns(DeprecationWarning, match="extracted_archive") as record:
        out = decompress_archive(archive)
    assert record[0].filename == __file__
    shutil.rmtree(out.parent)


def test_decompress_archive_failure_without_output_dir_removes_temp(
    tmp_path, tmpdir_isolado
):
    bogus = tmp_path / "bogus.zip"
    bogus.write_bytes(b"not a zip")
    with pytest.raises(StorageError):
        decompress_archive(bogus)
    assert _sobras(tmpdir_isolado) == []


def test_extracted_archive_exported_from_package_root():
    import quantilica.core

    assert quantilica.core.extracted_archive is extracted_archive
    assert "extracted_archive" in quantilica.core.__all__


@pytest.mark.skipif(shutil.which("7z") is None, reason="binário 7z ausente no PATH")
def test_extracted_archive_real_7z_removes_root(tmp_path, tmpdir_isolado):
    fonte = tmp_path / "fonte"
    fonte.mkdir()
    (fonte / "dados.csv").write_bytes(b"x\n1\n")
    archive = tmp_path / "dados.7z"
    subprocess.run(
        ["7z", "a", str(archive), str(fonte / "dados.csv")],
        check=True,
        capture_output=True,
    )
    with extracted_archive(archive, target_extensions=(".csv",)) as path:
        assert path.name == "dados.csv"
        assert path.read_bytes() == b"x\n1\n"
    assert _sobras(tmpdir_isolado) == []
