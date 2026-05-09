import pytest

from quantilica_core.exceptions import StorageError
from quantilica_core.files import sha256_bytes
from quantilica_core.storage import LocalStorage


def test_local_storage_write_read_and_stat(tmp_path):
    storage = LocalStorage(tmp_path)

    stat = storage.write_bytes("raw/source/data.bin", b"abc")

    assert storage.exists("raw/source/data.bin")
    assert storage.read_bytes("raw/source/data.bin") == b"abc"
    assert stat.key == "raw/source/data.bin"
    assert stat.size_bytes == 3
    assert stat.sha256 == sha256_bytes(b"abc")
    assert stat.modified_at.endswith("Z")


def test_local_storage_write_and_read_text(tmp_path):
    storage = LocalStorage(tmp_path)

    stat = storage.write_text("docs/readme.txt", "olá")

    assert stat.key == "docs/readme.txt"
    assert storage.read_text("docs/readme.txt") == "olá"


def test_local_storage_list_returns_sorted_keys(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.write_bytes("b/file.txt", b"b")
    storage.write_bytes("a/file.txt", b"a")
    storage.write_bytes("a/nested/file.txt", b"nested")

    assert storage.list() == [
        "a/file.txt",
        "a/nested/file.txt",
        "b/file.txt",
    ]
    assert storage.list("a") == ["a/file.txt", "a/nested/file.txt"]


def test_local_storage_delete(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.write_bytes("data/file.txt", b"abc")

    storage.delete("data/file.txt")

    assert not storage.exists("data/file.txt")


@pytest.mark.parametrize("key", ["", "../outside.txt", "/absolute.txt", "a/../b.txt"])
def test_local_storage_rejects_invalid_keys(tmp_path, key):
    storage = LocalStorage(tmp_path)

    with pytest.raises(StorageError):
        storage.path_for(key)


def test_local_storage_missing_read_raises_storage_error(tmp_path):
    storage = LocalStorage(tmp_path)

    with pytest.raises(StorageError):
        storage.read_bytes("missing.txt")
