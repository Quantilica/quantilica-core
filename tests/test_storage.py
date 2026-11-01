from datetime import date

import pytest

from quantilica.core.exceptions import StorageError
from quantilica.core.files import sha256_bytes
from quantilica.core.storage import (
    LocalStorage,
    StampedDataRepository,
    build_stamped_filename,
    slugify,
)


def test_build_stamped_filename_joins_parts_and_stamps():
    name = build_stamped_filename("exp", 2024, ext="csv", timestamp=date(2024, 3, 15))

    assert name == "exp_2024@20240315.csv"


def test_build_stamped_filename_drops_falsy_parts():
    assert build_stamped_filename("caged", None, "", ext="csv") == "caged.csv"


def test_build_stamped_filename_datetime_precision():
    name = build_stamped_filename(
        "td", ext="csv", timestamp=date(2024, 3, 15), precision="datetime"
    )

    assert name == "td@20240315T000000.csv"


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


# --- slugify ---


@pytest.mark.parametrize(
    "value, expected",
    [
        ("Taxas dos Títulos", "taxas-dos-titulos"),
        ("operações do tesouro direto", "operacoes-do-tesouro-direto"),
        ("file name  with   spaces", "file-name-with-spaces"),
        ("already-slugged", "already-slugged"),
        ("UPPER CASE", "upper-case"),
        ("special!@#chars", "specialchars"),
    ],
)
def test_slugify(value, expected):
    assert slugify(value) == expected


# --- StampedDataRepository ---


def test_stamped_repo_list_dataset_ids(tmp_path):
    repo = StampedDataRepository(tmp_path)
    repo.dataset_path("dataset-a", "file.csv").parent.mkdir(parents=True, exist_ok=True)
    repo.dataset_path("dataset-b", "file.csv").parent.mkdir(parents=True, exist_ok=True)

    assert repo.list_dataset_ids() == ["dataset-a", "dataset-b"]


def test_stamped_repo_list_dataset_ids_empty(tmp_path):
    repo = StampedDataRepository(tmp_path)
    assert repo.list_dataset_ids() == []


def test_stamped_repo_get_latest_stamped_file(tmp_path):
    repo = StampedDataRepository(tmp_path)
    d = repo.dataset_path("ds")
    d.mkdir(parents=True)
    (d / "prices@20250101T000000.csv").write_text("old")
    (d / "prices@20250601T000000.csv").write_text("new")
    (d / "stock@20250101T000000.csv").write_text("other")

    result = repo.get_latest_stamped_file("ds", "prices")

    assert result is not None
    assert result.name == "prices@20250601T000000.csv"


def test_stamped_repo_get_latest_stamped_file_missing(tmp_path):
    repo = StampedDataRepository(tmp_path)
    assert repo.get_latest_stamped_file("nonexistent", "prices") is None


def test_stamped_repo_get_all_latest_stamped_files(tmp_path):
    repo = StampedDataRepository(tmp_path)
    d = repo.dataset_path("ds")
    d.mkdir(parents=True)
    (d / "prices@20250101T000000.csv").write_text("old prices")
    (d / "prices@20250601T000000.csv").write_text("new prices")
    (d / "stock@20250101T000000.csv").write_text("stock")

    results = repo.get_all_latest_stamped_files("ds")
    names = sorted(f.name for f in results)

    assert names == ["prices@20250601T000000.csv", "stock@20250101T000000.csv"]


def test_stamped_repo_get_all_latest_stamped_files_empty(tmp_path):
    repo = StampedDataRepository(tmp_path)
    assert repo.get_all_latest_stamped_files("nonexistent") == []


def test_stamped_repo_ignores_manifest_sidecars(tmp_path):
    repo = StampedDataRepository(tmp_path)
    d = repo.dataset_path("data")
    d.mkdir(parents=True)
    (d / "series_1@20250101T000000.json").write_text("[]")
    (d / "series_1@20250101T000000.json.manifest.json").write_text("{}")
    (d / "series_1@20250601T000000.json").write_text("[]")
    (d / "series_1@20250601T000000.json.manifest.json").write_text("{}")

    latest = repo.get_latest_stamped_file("data", "series_1", ext="json")
    assert latest is not None
    assert latest.name == "series_1@20250601T000000.json"

    all_latest = repo.get_all_latest_stamped_files("data", ext="json")
    assert [f.name for f in all_latest] == ["series_1@20250601T000000.json"]


def test_format_partition_token():
    from quantilica.core.storage import format_partition_token

    assert format_partition_token({}) is None
    assert format_partition_token({"partition": "custom-part"}) == "custom-part"
    assert format_partition_token({"year": 2024}) == "2024"
    assert format_partition_token({"year": "2025"}) == "2025"
    assert format_partition_token({"year": 2024, "semester": 1}) == "2024-S1"
    assert format_partition_token({"year": 2024, "semester": "S2"}) == "2024-S2"
    assert format_partition_token({"year": 2024, "quarter": 3}) == "2024-T3"
    assert format_partition_token({"year": 2024, "quarter": "T4"}) == "2024-T4"
    assert format_partition_token({"year": 2024, "month": 5}) == "2024-05"
    assert format_partition_token({"year": 2024, "month": "08"}) == "2024-08"
    assert format_partition_token({"year": 2024, "month": 5, "day": 9}) == "2024-05-09"
    assert format_partition_token({"year": 2024, "uf": "SP"}) == "2024-sp"
    assert format_partition_token({"year": 2024, "region": "SUL"}) == "2024-sul"
    assert (
        format_partition_token({"year": 2024, "month": 8, "uf": "SP"}) == "2024-08-sp"
    )
    assert format_partition_token({"ano": 2024}) == "2024"
    assert format_partition_token({"ano": 2024, "mes": 5}) == "2024-05"
    assert format_partition_token({"ano": 2024, "semestre": 2}) == "2024-S2"
    assert format_partition_token({"ano": 2024, "trimestre": 1}) == "2024-T1"
    assert format_partition_token({"ano": 2024, "mes": 5, "dia": 10}) == "2024-05-10"
    assert format_partition_token({"uf": "RJ"}) == "rj"
    assert format_partition_token({"region": "Nordeste"}) == "nordeste"


def test_base_data_repository_path_for_entry(tmp_path):
    from datetime import date, datetime

    from quantilica.core.storage import BaseDataRepository

    repo = BaseDataRepository(tmp_path)

    # 1. Standard annual partition
    entry = {"group": "exp", "id": "exp_mun", "year": 2023, "ext": "csv"}
    p = repo.path_for_entry(entry, last_modified=date(2024, 3, 15))
    assert p == tmp_path / "exp" / "exp_mun_2023@20240315.csv"

    # 2. Semester partition
    entry_sem = {"group": "anp", "id": "ca", "year": 2024, "semester": 1, "ext": "csv"}
    p_sem = repo.path_for_entry(entry_sem, last_modified=date(2024, 8, 1))
    assert p_sem == tmp_path / "anp" / "ca_2024-S1@20240801.csv"

    # 3. Subdir by year
    entry_sub = {"group": "pdet", "id": "caged", "year": 2024, "month": 1, "ext": "7z"}
    p_sub = repo.path_for_entry(
        entry_sub, subdir_by_year=True, last_modified=date(2024, 2, 1)
    )
    assert p_sub == tmp_path / "pdet" / "2024" / "caged_2024-01@20240201.7z"

    # 4. Ext from URL
    entry_url = {
        "group": "cvm",
        "id": "cias",
        "url": "https://gov.br/data/file.zip?token=123",
    }
    p_url = repo.path_for_entry(entry_url)
    assert p_url == tmp_path / "cvm" / "cias.zip"

    # 5. Datetime precision
    entry_dt = {"group": "td", "id": "taxas", "ext": "csv"}
    dt_val = datetime(2024, 5, 1, 14, 30, 0)
    p_dt = repo.path_for_entry(entry_dt, last_modified=dt_val, precision="datetime")
    assert p_dt == tmp_path / "td" / "taxas@20240501T143000.csv"
