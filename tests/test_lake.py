from __future__ import annotations

import json
import multiprocessing
import time
from importlib.metadata import EntryPoint
from pathlib import Path, PureWindowsPath

import pytest

from quantilica.core import lake
from quantilica.core.lake import (
    HIVE_DEFAULT_PARTITION,
    DatasetDescriptor,
    dataset_lock,
    encode_partition_value,
    file_key,
    lake_root,
    load_lake_converter,
    parse_partition_path,
    partition_dir,
)

# --- lake_root ---------------------------------------------------------------


def test_lake_root_prefers_lake_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("QUANTILICA_LAKE_DIR", str(tmp_path / "x"))
    monkeypatch.setenv("QUANTILICA_DATA_DIR", str(tmp_path / "data"))
    assert lake_root() == tmp_path / "x"


def test_lake_root_falls_back_to_data_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("QUANTILICA_LAKE_DIR", raising=False)
    monkeypatch.setenv("QUANTILICA_DATA_DIR", str(tmp_path / "data"))
    assert lake_root() == tmp_path / "data" / "lake"


def test_lake_root_default_is_home_data_lake(monkeypatch):
    monkeypatch.delenv("QUANTILICA_LAKE_DIR", raising=False)
    monkeypatch.delenv("QUANTILICA_DATA_DIR", raising=False)
    assert lake_root() == Path.home() / "data" / "lake"


def test_lake_root_treats_empty_variables_as_unset(monkeypatch, tmp_path):
    monkeypatch.setenv("QUANTILICA_LAKE_DIR", "")
    monkeypatch.setenv("QUANTILICA_DATA_DIR", str(tmp_path))
    assert lake_root() == tmp_path / "lake"
    monkeypatch.setenv("QUANTILICA_DATA_DIR", "")
    assert lake_root() == Path.home() / "data" / "lake"


def test_lake_root_expands_user(monkeypatch):
    monkeypatch.setenv("QUANTILICA_LAKE_DIR", "~/somewhere")
    assert lake_root() == Path.home() / "somewhere"


# --- file_key ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("caged_202401.7z", "caged_202401"),
        ("investidores@20261021.csv", "investidores"),
        ("data.csv.gz", "data"),
        ("archive.tar.gz", "archive"),
        ("rais-vinculos_2025-sul@20260513.7z", "rais-vinculos_2025-sul"),
        ("x@20260513T101500.csv.zip", "x"),
        ("DATA.CSV.GZ", "DATA"),
        ("plain", "plain"),
        ("plain@20260101", "plain"),
        ("v1.2_report.csv", "v1.2_report"),  # unknown suffix is kept
        (".csv", ".csv"),  # a bare extension is not stripped to nothing
        ("a@b.csv", "a@b"),  # "@" not followed by a date-like token
    ],
)
def test_file_key(name, expected):
    assert file_key(name) == expected


def test_file_key_uses_only_the_final_component():
    assert file_key(Path("/lake/raw/2024/caged_202401@20260101.7z")) == "caged_202401"
    assert file_key("dir.csv/caged.7z") == "caged"
    assert file_key(PureWindowsPath(r"C:\raw\caged_202401.7z")) == "caged_202401"
    assert file_key(r"C:\raw\caged_202401.7z") == "caged_202401"


# --- encode_partition_value --------------------------------------------------


@pytest.mark.parametrize("value", [None, ""])
def test_encode_missing_values_use_hive_default(value):
    assert encode_partition_value(value) == "__HIVE_DEFAULT_PARTITION__"
    assert HIVE_DEFAULT_PARTITION == "__HIVE_DEFAULT_PARTITION__"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("09", "09"),  # leading zero preserved
        (2025, "2025"),
        (0, "0"),  # falsy but not missing
        (1.5, "1.5"),
        (-1, "-1"),
        ("SP", "SP"),
        ("a.b_c-d", "a.b_c-d"),
        ("2026-09", "2026-09"),
    ],
)
def test_encode_valid_values(value, expected):
    assert encode_partition_value(value) == expected


@pytest.mark.parametrize(
    "value",
    ["a/b", "a=b", "a b", "São Paulo", "a\\b", ".", "..", "a\n", 1e16, "a%2F"],
)
def test_encode_invalid_values_raise(value):
    with pytest.raises(ValueError):
        encode_partition_value(value)


# --- partition_dir / parse_partition_path ------------------------------------


def test_partition_dir_builds_nested_hive_path(tmp_path):
    result = partition_dir(tmp_path, {"ano": 2025, "mes": "09", "uf": None})
    assert result == tmp_path / "ano=2025" / "mes=09" / f"uf={HIVE_DEFAULT_PARTITION}"


def test_partition_dir_without_partitions_is_base(tmp_path):
    assert partition_dir(tmp_path, {}) == tmp_path


@pytest.mark.parametrize("key", ["", "Ano", "a=b", "a/b", "1ano", "a b", "ação"])
def test_partition_dir_rejects_invalid_keys(tmp_path, key):
    with pytest.raises(ValueError):
        partition_dir(tmp_path, {key: 1})


def test_partition_dir_rejects_invalid_value(tmp_path):
    with pytest.raises(ValueError):
        partition_dir(tmp_path, {"uf": "S/P"})


def test_parse_partition_path_directory(tmp_path):
    directory = tmp_path / "ano=2025" / "mes=09"
    assert parse_partition_path(directory, tmp_path) == {"ano": "2025", "mes": "09"}


def test_parse_partition_path_file(tmp_path):
    file = tmp_path / "ano=2025" / "uf=SP" / "caged_202401-001.parquet"
    assert parse_partition_path(file, tmp_path) == {"ano": "2025", "uf": "SP"}


def test_parse_partition_path_ignores_segments_above_base(tmp_path):
    base = tmp_path / "ano=1999" / "src" / "ds"  # "=" above the base is not parsed
    assert parse_partition_path(base / "mes=01", base) == {"mes": "01"}


def test_parse_partition_path_unpartitioned(tmp_path):
    assert parse_partition_path(tmp_path / "f.parquet", tmp_path) == {}
    assert parse_partition_path(tmp_path, tmp_path) == {}


def test_parse_partition_path_keeps_default_marker_and_value_with_equals(tmp_path):
    path = tmp_path / f"uf={HIVE_DEFAULT_PARTITION}" / "k=a=b"
    assert parse_partition_path(path, tmp_path) == {
        "uf": HIVE_DEFAULT_PARTITION,
        "k": "a=b",
    }


def test_parse_partition_path_outside_base_raises(tmp_path):
    with pytest.raises(ValueError):
        parse_partition_path(tmp_path / "a" / "ano=1", tmp_path / "b")


def test_parse_partition_path_windows_paths():
    base = PureWindowsPath(r"C:\lake\src\ds")
    path = PureWindowsPath(r"C:\lake\src\ds\ano=2025\mes=09\k.parquet")
    assert parse_partition_path(path, base) == {"ano": "2025", "mes": "09"}


def test_partition_roundtrip(tmp_path):
    partitions = {"ano": 2025, "mes": "09", "uf": "SP"}
    parsed = parse_partition_path(partition_dir(tmp_path, partitions), tmp_path)
    assert parsed == {"ano": "2025", "mes": "09", "uf": "SP"}


# --- DatasetDescriptor -------------------------------------------------------


def _descriptor(**overrides):
    kwargs = {"source_id": "pdet", "dataset_id": "caged", "alias": "caged"}
    kwargs.update(overrides)
    return DatasetDescriptor(**kwargs)


def test_descriptor_defaults():
    d = _descriptor()
    assert d.partition_keys == []
    assert d.partition_types == {}
    assert d.partition_source == "file"
    assert d.contract_id is None
    assert d.contract_version is None
    assert d.converter is None
    assert d.layout_version == 1


def test_descriptor_mutable_defaults_are_not_shared():
    a, b = _descriptor(), _descriptor()
    a.partition_keys.append("ano")
    a.partition_types["ano"] = "int"
    assert b.partition_keys == []
    assert b.partition_types == {}


def test_descriptor_dict_and_json_roundtrip():
    d = _descriptor(
        partition_keys=["ano", "mes"],
        partition_types={"ano": "int", "mes": "varchar"},
        partition_source="rows",
        contract_id="pdet.caged",
        contract_version=3,
        converter="pdet-fetcher 1.0.0",
    )
    assert DatasetDescriptor.from_dict(d.to_dict()) == d
    assert DatasetDescriptor.from_json(d.to_json()) == d
    assert json.loads(d.to_json()) == d.to_dict()
    assert d.to_dict()["layout_version"] == 1


def test_descriptor_to_json_keeps_non_ascii_and_ends_with_newline():
    text = _descriptor(alias="saúde").to_json()
    assert "saúde" in text
    assert text.endswith("\n")


def test_descriptor_from_dict_ignores_unknown_keys():
    d = DatasetDescriptor.from_dict(
        {"source_id": "s", "dataset_id": "d", "alias": "a", "future_field": 1}
    )
    assert d == DatasetDescriptor("s", "d", "a")


@pytest.mark.parametrize("missing", ["source_id", "dataset_id", "alias"])
def test_descriptor_from_dict_requires_identity_keys(missing):
    data = {"source_id": "s", "dataset_id": "d", "alias": "a"}
    del data[missing]
    with pytest.raises(ValueError, match=missing):
        DatasetDescriptor.from_dict(data)


def test_descriptor_from_json_rejects_non_object():
    with pytest.raises(ValueError):
        DatasetDescriptor.from_json("[1, 2]")


def test_descriptor_from_json_rejects_malformed_json():
    with pytest.raises(json.JSONDecodeError):
        DatasetDescriptor.from_json("{not json")


def test_descriptor_rejects_invalid_partition_source():
    with pytest.raises(ValueError, match="partition_source"):
        _descriptor(partition_source="elsewhere")


def test_descriptor_save_and_load(tmp_path):
    dataset_dir = tmp_path / "pdet" / "caged"  # does not exist yet
    d = _descriptor(partition_keys=["ano"], partition_types={"ano": "int"})
    path = d.save(dataset_dir)
    assert path == dataset_dir / "_dataset.json"
    assert path.is_file()
    assert DatasetDescriptor.load(dataset_dir) == d


def test_descriptor_save_overwrites_and_leaves_no_temp_files(tmp_path):
    _descriptor(alias="old").save(tmp_path)
    _descriptor(alias="new").save(tmp_path)
    assert DatasetDescriptor.load(tmp_path).alias == "new"
    assert [p.name for p in tmp_path.iterdir()] == ["_dataset.json"]


def test_descriptor_load_missing_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        DatasetDescriptor.load(tmp_path)


# --- dataset_lock ------------------------------------------------------------

needs_fcntl = pytest.mark.skipif(lake.fcntl is None, reason="requires fcntl")


@needs_fcntl
def test_dataset_lock_creates_directory_and_lock_file(tmp_path):
    dataset_dir = tmp_path / "src" / "ds"
    with dataset_lock(dataset_dir):
        assert (dataset_dir / ".lock").is_file()
    assert (dataset_dir / ".lock").is_file()  # left in place


@needs_fcntl
def test_dataset_lock_is_exclusive_non_blocking(tmp_path):
    with dataset_lock(tmp_path):
        with pytest.raises(BlockingIOError):
            with dataset_lock(tmp_path, blocking=False):
                pytest.fail("lock should not be acquired")
    with dataset_lock(tmp_path, blocking=False):  # free again after release
        pass


@needs_fcntl
def test_dataset_lock_released_on_exception(tmp_path):
    with pytest.raises(RuntimeError):
        with dataset_lock(tmp_path):
            raise RuntimeError("boom")
    with dataset_lock(tmp_path, blocking=False):
        pass


def _hold_lock(dataset_dir: str, acquired, release) -> None:
    with dataset_lock(Path(dataset_dir)):
        acquired.set()
        release.wait(10)


@needs_fcntl
def test_dataset_lock_excludes_other_processes(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    acquired, release = ctx.Event(), ctx.Event()
    proc = ctx.Process(target=_hold_lock, args=(str(tmp_path), acquired, release))
    proc.start()
    try:
        assert acquired.wait(30), "child did not acquire the lock"
        with pytest.raises(BlockingIOError):
            with dataset_lock(tmp_path, blocking=False):
                pass
    finally:
        release.set()
        proc.join(30)
    assert proc.exitcode == 0
    deadline = time.monotonic() + 5
    while True:
        try:
            with dataset_lock(tmp_path, blocking=False):
                break
        except BlockingIOError:
            assert time.monotonic() < deadline
            time.sleep(0.05)


def test_dataset_lock_is_noop_without_fcntl(monkeypatch, tmp_path):
    monkeypatch.setattr(lake, "fcntl", None)
    dataset_dir = tmp_path / "missing"
    with dataset_lock(dataset_dir):
        with dataset_lock(dataset_dir, blocking=False):  # no exclusion
            pass
    assert not dataset_dir.exists()  # no side effects


# --- load_lake_converter -----------------------------------------------------


def _fake_entry_points(monkeypatch, entries):
    def fake(*, group, name):
        assert group == "quantilica.lake"
        return [e for e in entries if e.name == name]

    monkeypatch.setattr(lake, "entry_points", fake)


def test_load_lake_converter_returns_loaded_object(monkeypatch):
    entry = EntryPoint(
        name="pdet", value="quantilica.core.lake:lake_root", group="quantilica.lake"
    )
    _fake_entry_points(monkeypatch, [entry])
    assert load_lake_converter("pdet") is lake_root


def test_load_lake_converter_unknown_source_returns_none(monkeypatch):
    entry = EntryPoint(
        name="pdet", value="quantilica.core.lake:lake_root", group="quantilica.lake"
    )
    _fake_entry_points(monkeypatch, [entry])
    assert load_lake_converter("cvm") is None


def test_load_lake_converter_with_no_entry_points(monkeypatch):
    _fake_entry_points(monkeypatch, [])
    assert load_lake_converter("pdet") is None


def test_load_lake_converter_propagates_import_errors(monkeypatch):
    entry = EntryPoint(
        name="broken", value="no_such_module_xyz:converter", group="quantilica.lake"
    )
    _fake_entry_points(monkeypatch, [entry])
    with pytest.raises(ImportError):
        load_lake_converter("broken")


def test_load_lake_converter_real_discovery_without_match():
    assert load_lake_converter("no-such-source-xyz") is None


# --- package exports ---------------------------------------------------------


def test_public_exports():
    import quantilica.core as core

    for name in (
        "lake_root",
        "file_key",
        "DatasetDescriptor",
        "encode_partition_value",
        "partition_dir",
        "parse_partition_path",
        "dataset_lock",
        "load_lake_converter",
    ):
        assert name in core.__all__
        assert getattr(core, name) is getattr(lake, name)
