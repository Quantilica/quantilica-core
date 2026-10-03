import sys
import zipfile
from pathlib import Path

import pytest

from quantilica.core.exceptions import StorageError
from quantilica.core.files import (
    check_free_space,
    decompress_archive,
    ensure_dir,
    ensure_parent,
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


# ---------------------------------------------------------------------------
# decompress_archive
# ---------------------------------------------------------------------------


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
    assert str(archive) in str(called["cmd"])
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
