"""Tests for the current-version choice, the lake view and the converter protocol."""

from __future__ import annotations

import errno
import os
from pathlib import Path

import pytest

from quantilica.core import lake
from quantilica.core.files import sha256_bytes
from quantilica.core.lake import (
    LakeConverter,
    Planned,
    ViewSummary,
    converter_inventory,
    materialize_view,
    select_current,
)
from quantilica.core.manifests import (
    DownloadManifest,
    manifest_sidecar_path,
    write_manifest_sidecar,
)


def put(root: Path, rel: str, content: bytes | None = None, *, manifest: bool = True):
    """Create a raw file (content defaults to its name) and its sidecar."""
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    data = rel.encode() if content is None else content
    target.write_bytes(data)
    if manifest:
        write_manifest_sidecar(
            target,
            DownloadManifest.from_digest(
                source_id="s",
                dataset_id="d",
                url=f"https://example.org/{rel}",
                sha256=sha256_bytes(data),
                size_bytes=len(data),
            ),
        )
    return target


def tree(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}


def symlinks_allowed(tmp_path: Path) -> bool:
    probe = tmp_path / "_probe"
    probe.write_bytes(b"")
    try:
        (tmp_path / "_probe_link").symlink_to(probe)
    except (OSError, NotImplementedError):
        return False
    return True


# --- select_current ----------------------------------------------------------


def test_select_current_newest_version_wins():
    paths = [
        "ds/a@20260101.csv",
        "ds/a@20260301.csv",
        "ds/a@20260201.csv",
    ]
    assert select_current(paths) == ["ds/a@20260301.csv"]


def test_select_current_datetime_beats_date_of_same_day():
    paths = ["ds/a@20260301.csv", "ds/a@20260301T093000.csv"]
    assert select_current(paths) == ["ds/a@20260301T093000.csv"]


def test_select_current_date_and_midnight_are_the_same_version():
    paths = ["ds/a@20260301.csv", "ds/a@20260301T000000.csv", "ds/a@20260101.csv"]
    assert select_current(paths) == ["ds/a@20260301.csv", "ds/a@20260301T000000.csv"]


def test_select_current_unstamped_is_the_oldest_version():
    assert select_current(["ds/a.csv", "ds/a@20200101.csv"]) == ["ds/a@20200101.csv"]
    assert select_current(["ds/a.csv"]) == ["ds/a.csv"]


def test_select_current_keeps_format_twins_of_the_same_version():
    paths = ["ds/a@20260301.csv", "ds/a@20260301.zip", "ds/a@20260101.csv"]
    assert select_current(paths) == ["ds/a@20260301.csv", "ds/a@20260301.zip"]
    assert select_current(["ds/b.csv", "ds/b.zip"]) == ["ds/b.csv", "ds/b.zip"]


def test_select_current_same_key_in_different_directories_does_not_collide():
    paths = ["x/a@20260101.csv", "y/a@20260201.csv", "x/a@20250101.csv"]
    assert select_current(paths) == ["x/a@20260101.csv", "y/a@20260201.csv"]


def test_select_current_distinct_keys_are_independent():
    paths = ["ds/a_2024@20260101.csv", "ds/a_2025@20250101.csv"]
    assert select_current(paths) == paths


def test_select_current_manifest_follows_the_chosen_version():
    paths = [
        "ds/a@20260101.csv",
        "ds/a@20260101.csv.manifest.json",
        "ds/a@20260301.csv",
        "ds/a@20260301.csv.manifest.json",
    ]
    assert select_current(paths) == [
        "ds/a@20260301.csv",
        "ds/a@20260301.csv.manifest.json",
    ]


def test_select_current_manifests_are_not_candidates():
    # A newer manifest without its data file must not win over older data.
    paths = ["ds/a@20260101.csv", "ds/a@20260901.csv.manifest.json"]
    assert select_current(paths) == ["ds/a@20260101.csv"]
    assert select_current(["ds/a.csv.manifest.json"]) == []


def test_select_current_ignores_interrupted_writes():
    assert select_current(["ds/.a.csv.x1.tmp", "ds/a.csv"]) == ["ds/a.csv"]


def test_select_current_accepts_paths_and_returns_the_same_objects():
    paths = [Path("ds") / "a@20260101.csv", Path("ds") / "a@20260201.csv"]
    result = select_current(paths)
    assert result == [paths[1]]
    assert result[0] is paths[1]


def test_select_current_accepts_windows_separators():
    paths = ["ds\\a@20260101.csv", "ds\\a@20260201.csv", "other\\a@20260101.csv"]
    assert select_current(paths) == ["ds\\a@20260201.csv", "other\\a@20260101.csv"]


def test_select_current_excludes_by_path():
    paths = ["ds/a@20260301.csv", "ds/a@20260301.csv.manifest.json", "ds/b.csv"]
    result = select_current(paths, exclude={"ds/a@20260301.csv"})
    assert result == ["ds/b.csv"]


def test_select_current_exclude_path_normalizes_separators():
    paths = ["ds/a.csv", "ds/b.csv"]
    assert select_current(paths, exclude={".\\ds\\a.csv"}) == ["ds/b.csv"]


def test_select_current_exclusion_does_not_bring_back_an_older_version():
    paths = ["ds/a@20260101.csv", "ds/a@20260301.csv"]
    assert select_current(paths, exclude={"ds/a@20260301.csv"}) == []


def test_select_current_excludes_by_sha256_using_the_manifest(tmp_path):
    bad = put(tmp_path, "ds/a@20260301.csv", b"corrupt")
    put(tmp_path, "ds/b@20260301.csv", b"fine")
    digest = sha256_bytes(b"corrupt")
    paths = ["ds/a@20260301.csv", "ds/b@20260301.csv"]
    # The sidecar is trusted: the file itself is not read.
    bad.write_bytes(b"changed after the manifest")
    result = select_current(paths, exclude={digest.upper()}, root=tmp_path)
    assert result == ["ds/b@20260301.csv"]


def test_select_current_excludes_by_sha256_hashing_when_there_is_no_manifest(tmp_path):
    put(tmp_path, "ds/a.csv", b"corrupt", manifest=False)
    put(tmp_path, "ds/b.csv", b"fine", manifest=False)
    result = select_current(
        ["ds/a.csv", "ds/b.csv"], exclude={sha256_bytes(b"corrupt")}, root=tmp_path
    )
    assert result == ["ds/b.csv"]


def test_select_current_hashes_only_when_there_are_sha256_exclusions(tmp_path):
    # None of these files exist: path exclusions must not touch the disk.
    result = select_current(["ds/a.csv", "ds/b.csv"], exclude={"ds/a.csv"})
    assert result == ["ds/b.csv"]
    result = select_current(
        ["ds/a.csv"], exclude={"ds/other.csv"}, root=tmp_path / "missing"
    )
    assert result == ["ds/a.csv"]


def test_select_current_hashes_only_the_chosen_versions(tmp_path):
    put(tmp_path, "ds/a@20260301.csv", b"new", manifest=False)
    # The older version is not on disk: hashing it would raise.
    paths = ["ds/a@20260101.csv", "ds/a@20260301.csv"]
    result = select_current(paths, exclude={sha256_bytes(b"x")}, root=tmp_path)
    assert result == ["ds/a@20260301.csv"]


def test_select_current_sha256_exclusion_needs_root():
    with pytest.raises(ValueError, match="root"):
        select_current(["a.csv"], exclude={"0" * 64})


# --- materialize_view --------------------------------------------------------


def make_raw(tmp_path: Path) -> Path:
    raw = tmp_path / "raw"
    put(raw, "src/ds/a@20260101.csv")
    put(raw, "src/ds/a@20260301.csv")
    put(raw, "src/ds/b.csv")
    put(raw, "src/other/a@20250101.csv")
    return raw


def test_materialize_view_links_only_current_versions_and_manifests(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    summary = materialize_view(raw, out)
    assert tree(out) == {
        "src/ds/a@20260301.csv",
        "src/ds/a@20260301.csv.manifest.json",
        "src/ds/b.csv",
        "src/ds/b.csv.manifest.json",
        "src/other/a@20250101.csv",
        "src/other/a@20250101.csv.manifest.json",
    }
    assert summary == ViewSummary(files=6, hardlink=6)
    assert os.path.samefile(raw / "src/ds/b.csv", out / "src/ds/b.csv")
    assert (out / "src/ds/a@20260301.csv").read_bytes() == b"src/ds/a@20260301.csv"


def test_materialize_view_copy_mode_makes_independent_files(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    summary = materialize_view(raw, out, mode="copy")
    assert summary == ViewSummary(files=6, copy=6)
    assert not os.path.samefile(raw / "src/ds/b.csv", out / "src/ds/b.csv")
    assert not (out / "src/ds/b.csv").is_symlink()


def test_materialize_view_symlink_mode(tmp_path):
    if not symlinks_allowed(tmp_path):
        pytest.skip("symlinks are not allowed here")
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    summary = materialize_view(raw, out, mode="symlink")
    assert summary == ViewSummary(files=6, symlink=6)
    assert (out / "src/ds/b.csv").is_symlink()
    assert (out / "src/ds/b.csv").read_bytes() == b"src/ds/b.csv"


def test_materialize_view_falls_back_from_hardlink_to_symlink(tmp_path, monkeypatch):
    if not symlinks_allowed(tmp_path):
        pytest.skip("symlinks are not allowed here")
    raw = make_raw(tmp_path)

    def cross_device(src, dst, *args, **kwargs):
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    monkeypatch.setattr(os, "link", cross_device)
    summary = materialize_view(raw, tmp_path / "view")
    assert summary == ViewSummary(files=6, symlink=6)
    assert (tmp_path / "view/src/ds/b.csv").is_symlink()


def test_materialize_view_falls_back_to_copy_when_links_fail(tmp_path, monkeypatch):
    raw = make_raw(tmp_path)

    def refuse(*args, **kwargs):
        raise OSError(errno.EPERM, "Operation not permitted")

    monkeypatch.setattr(os, "link", refuse)
    monkeypatch.setattr(os, "symlink", refuse)
    out = tmp_path / "view"
    summary = materialize_view(raw, out)
    assert summary == ViewSummary(files=6, copy=6)
    assert (out / "src/ds/b.csv").read_bytes() == b"src/ds/b.csv"
    assert not (out / "src/ds/b.csv").is_symlink()
    # No temporary name is left behind.
    assert not [p for p in tree(out) if p.endswith(".tmp")]


def test_materialize_view_symlink_request_falls_back_to_copy(tmp_path, monkeypatch):
    raw = make_raw(tmp_path)

    def refuse(*args, **kwargs):
        raise OSError(errno.EPERM, "A required privilege is not held")

    monkeypatch.setattr(os, "symlink", refuse)
    summary = materialize_view(raw, tmp_path / "view", mode="symlink")
    assert summary == ViewSummary(files=6, copy=6)


def test_materialize_view_is_idempotent(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    materialize_view(raw, out)
    before = {p: (out / p).stat().st_ino for p in tree(out)}
    again = materialize_view(raw, out)
    assert again == ViewSummary(files=6, kept=6)
    assert {p: (out / p).stat().st_ino for p in tree(out)} == before


def test_materialize_view_is_idempotent_after_a_fallback_to_copy(tmp_path, monkeypatch):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    with monkeypatch.context() as patch:

        def refuse(*args, **kwargs):
            raise OSError(errno.EPERM, "no")

        patch.setattr(os, "link", refuse)
        patch.setattr(os, "symlink", refuse)
        first = materialize_view(raw, out)
        second = materialize_view(raw, out)
    assert first.copy == 6
    assert second == ViewSummary(files=6, kept=6)


def test_materialize_view_drops_what_stopped_being_current(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    materialize_view(raw, out)
    put(raw, "src/ds/a@20260901.csv")
    (raw / "src/other/a@20250101.csv").unlink()
    manifest_sidecar_path(raw / "src/other/a@20250101.csv").unlink()
    summary = materialize_view(raw, out)
    assert tree(out) == {
        "src/ds/a@20260901.csv",
        "src/ds/a@20260901.csv.manifest.json",
        "src/ds/b.csv",
        "src/ds/b.csv.manifest.json",
    }
    assert summary == ViewSummary(files=4, hardlink=2, kept=2, removed=4)
    # The directory with nothing current left is gone too.
    assert not (out / "src/other").exists()


def test_materialize_view_removes_foreign_files_and_leftovers(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    materialize_view(raw, out)
    (out / "src/ds/stray.txt").write_text("x")
    (out / "src/ds/.b.csv.1.tmp").write_text("x")
    (out / "empty/deeper").mkdir(parents=True)
    summary = materialize_view(raw, out)
    assert summary.removed == 2
    assert summary.kept == 6
    assert not (out / "empty").exists()


def test_materialize_view_replaces_a_stale_file(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    (out / "src/ds").mkdir(parents=True)
    (out / "src/ds/b.csv").write_text("stale and longer than the original")
    summary = materialize_view(raw, out)
    assert summary.hardlink == 6
    assert (out / "src/ds/b.csv").read_bytes() == b"src/ds/b.csv"


def test_materialize_view_excludes_by_path_and_sha256(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    summary = materialize_view(
        raw,
        out,
        exclude={
            "src/ds/b.csv",
            sha256_bytes(b"src/other/a@20250101.csv"),
        },
    )
    assert tree(out) == {
        "src/ds/a@20260301.csv",
        "src/ds/a@20260301.csv.manifest.json",
    }
    assert summary == ViewSummary(files=2, hardlink=2, excluded=2)


def test_materialize_view_removes_a_file_that_became_excluded(tmp_path):
    raw = make_raw(tmp_path)
    out = tmp_path / "view"
    materialize_view(raw, out)
    summary = materialize_view(raw, out, exclude={"src/ds/b.csv"})
    assert "src/ds/b.csv" not in tree(out)
    assert "src/ds/b.csv.manifest.json" not in tree(out)
    assert summary == ViewSummary(files=4, kept=4, excluded=1, removed=2)


def test_materialize_view_handles_files_without_manifest(tmp_path):
    raw = tmp_path / "raw"
    put(raw, "ds/a.csv", manifest=False)
    out = tmp_path / "view"
    assert materialize_view(raw, out) == ViewSummary(files=1, hardlink=1)
    assert tree(out) == {"ds/a.csv"}


def test_materialize_view_rejects_overlapping_directories(tmp_path):
    raw = make_raw(tmp_path)
    for out in (raw, raw / "view", tmp_path):
        with pytest.raises(ValueError, match="overlap"):
            materialize_view(raw, out)
    assert (raw / "src/ds/b.csv").exists()


def test_materialize_view_rejects_bad_mode_and_missing_raw(tmp_path):
    raw = make_raw(tmp_path)
    with pytest.raises(ValueError, match="mode"):
        materialize_view(raw, tmp_path / "v", mode="move")  # type: ignore[arg-type]
    with pytest.raises(NotADirectoryError):
        materialize_view(tmp_path / "nope", tmp_path / "v")


def test_materialize_view_does_not_read_unneeded_content(tmp_path, monkeypatch):
    raw = make_raw(tmp_path)

    def boom(*args, **kwargs):
        raise AssertionError("hashed without a sha256 exclusion")

    monkeypatch.setattr(lake, "sha256_file", boom)
    materialize_view(raw, tmp_path / "view", exclude={"src/ds/b.csv"})


# --- converter protocol ------------------------------------------------------


class _Result:
    converted = 0
    skipped = 0
    errors: list[str] = []


class _Plain:
    source_id = "plain"

    def datasets(self):
        return ["one"]

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        return _Result()


class _WithInventory(_Plain):
    def inventory(self, input_dir):
        return [
            Planned(Path(input_dir) / "a.csv", "one"),
            Planned(Path(input_dir) / "a.zip", None, "gemeo"),
        ]


def test_existing_style_converters_satisfy_the_protocol():
    assert isinstance(_Plain(), LakeConverter)
    assert isinstance(_WithInventory(), LakeConverter)
    assert not isinstance(object(), LakeConverter)


def test_converter_inventory_is_none_without_the_method():
    assert converter_inventory(_Plain(), "raw") is None
    assert converter_inventory(object(), Path("raw")) is None


def test_converter_inventory_ignores_a_non_callable_attribute():
    converter = _Plain()
    converter.inventory = None  # type: ignore[attr-defined]
    assert converter_inventory(converter, "raw") is None


def test_converter_inventory_returns_the_plan(tmp_path):
    plan = converter_inventory(_WithInventory(), str(tmp_path))
    assert plan == [
        Planned(tmp_path / "a.csv", "one", None),
        Planned(tmp_path / "a.zip", None, "gemeo"),
    ]


def test_converter_inventory_empty_plan_is_not_none():
    class Empty(_Plain):
        def inventory(self, input_dir):
            return iter(())

    assert converter_inventory(Empty(), "raw") == []


def test_planned_validates_the_skip_reason():
    for reason in ("gemeo", "nao_tabular", "superado", None):
        assert Planned(Path("x"), None, reason).skip_reason == reason
    with pytest.raises(ValueError, match="skip_reason"):
        Planned(Path("x"), None, "whatever")


def test_planned_is_frozen():
    plan = Planned(Path("x"), "ds")
    with pytest.raises(AttributeError):
        plan.dataset = "other"  # type: ignore[misc]
