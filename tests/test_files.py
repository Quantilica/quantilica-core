import pytest

from quantilica_core.exceptions import StorageError
from quantilica_core.files import (
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
