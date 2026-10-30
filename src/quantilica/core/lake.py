"""Layout contract for the Hive-partitioned Parquet lake.

The lake is a layer of the ecosystem, not a package: whoever honours this
contract writes to it, and whoever reads it does not need to know which
fetcher produced the data. This module holds only the stdlib-level pieces
of the contract (root, file keys, Hive paths, the ``_dataset.json``
descriptor, the per-dataset lock, converter discovery and protocol, and the
choice of the current version of each raw file); writers and readers live in
``quantilica-analytics``.

Layout::

    $QUANTILICA_LAKE_DIR/
      <source_id>/<dataset_id>/
        _dataset.json
        <k1>=<v1>/[<k2>=<v2>/...]
          <file_key>[-NNN].parquet
"""

from __future__ import annotations

import json
import os
import re
import shutil
import time
from collections.abc import Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from importlib.metadata import entry_points
from pathlib import Path, PurePath, PurePosixPath
from typing import Any, Literal, Protocol, runtime_checkable

from .files import sha256_file, write_text_atomic
from .manifests import MANIFEST_SIDECAR_SUFFIX, read_manifest_sidecar

try:  # POSIX
    import fcntl
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    fcntl = None  # type: ignore[assignment]

try:  # Windows
    import msvcrt
except ImportError:
    msvcrt = None  # type: ignore[assignment]

LAKE_DIR_ENV = "QUANTILICA_LAKE_DIR"
DATA_DIR_ENV = "QUANTILICA_DATA_DIR"
LAKE_ENTRY_POINT_GROUP = "quantilica.lake"
DESCRIPTOR_FILENAME = "_dataset.json"
LOCK_FILENAME = ".lock"
LAYOUT_VERSION = 1

# Hive's marker for a missing partition value.
HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__"

PARTITION_SOURCES = ("file", "rows")

_PARTITION_VALUE = re.compile(r"^[0-9A-Za-z._-]+$")
_PARTITION_KEY = re.compile(r"^[a-z][a-z0-9_]*$")
_PARTITION_SEGMENT = re.compile(r"^([^=/]+)=(.*)$")
# Extensions of raw files and compression wrappers dropped by ``file_key``.
# An allow-list, so dots that are part of the name ("v1.2") survive.
_RAW_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".txt",
        ".dat",
        ".json",
        ".jsonl",
        ".ndjson",
        ".xml",
        ".xls",
        ".xlsx",
        ".dbf",
        ".dbc",
        ".parquet",
        ".tar",
        ".zip",
        ".7z",
        ".rar",
        ".gz",
        ".tgz",
        ".bz2",
        ".xz",
        ".zst",
    }
)
_DATE_SUFFIX = re.compile(r"@[0-9T]+$")
# The ``@AAAAMMDD`` / ``@AAAAMMDDTHHMMSS`` stamp that ``stamp_filename`` writes.
_VERSION_STAMP = re.compile(r"@(\d{8})(?:T(\d{6}))?$")
_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")
# Leftovers of ``write_*_atomic`` interrupted mid-write: never raw data.
_TEMP_SUFFIX = ".tmp"

# Why a raw file is not converted to a dataset (see ``Planned``).
SKIP_REASONS = ("gemeo", "nao_tabular", "superado")

LinkMode = Literal["hardlink", "symlink", "copy"]
_LINK_MODES: tuple[LinkMode, ...] = ("hardlink", "symlink", "copy")


def lake_root() -> Path:
    """Resolve the lake root directory.

    ``QUANTILICA_LAKE_DIR`` wins; otherwise ``$QUANTILICA_DATA_DIR/lake``;
    otherwise ``~/data/lake``. Empty variables count as unset.
    """
    explicit = os.environ.get(LAKE_DIR_ENV)
    if explicit:
        return Path(explicit).expanduser()
    data_dir = os.environ.get(DATA_DIR_ENV)
    if data_dir:
        return Path(data_dir).expanduser() / "lake"
    return Path.home() / "data" / "lake"


def file_key(path_or_name: str | PurePath) -> str:
    """Return the raw file's key: no extension and no ``@date`` suffix.

    ``caged_202401.7z`` -> ``caged_202401``;
    ``investidores@20261021.csv`` -> ``investidores``;
    ``data.csv.gz`` -> ``data``. Only the final path component is used,
    and stacked wrappers (``.tar.gz``, ``.csv.zip``) are all removed.
    """
    name = (
        path_or_name.name if isinstance(path_or_name, PurePath) else str(path_or_name)
    )
    return _DATE_SUFFIX.sub("", _strip_raw_suffixes(name))


def _strip_raw_suffixes(name: str) -> str:
    """Drop the final path component's raw extensions and compression wrappers."""
    # Accept Windows-style separators in plain strings.
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    while True:
        stem, dot, ext = name.rpartition(".")
        if stem and dot and f".{ext.lower()}" in _RAW_SUFFIXES:
            name = stem
        else:
            break
    return name


def encode_partition_value(value: str | int | float | None) -> str:
    """Encode a partition value for use in a ``key=value`` directory name.

    ``None`` and the empty string map to ``__HIVE_DEFAULT_PARTITION__``.
    Anything outside ``[0-9A-Za-z._-]`` raises ``ValueError``, as do ``.``
    and ``..`` (they would escape the partition directory).
    """
    if value is None or value == "":
        return HIVE_DEFAULT_PARTITION
    text = str(value)
    if not _PARTITION_VALUE.fullmatch(text) or text in {".", ".."}:
        raise ValueError(
            f"Invalid partition value {text!r}: only [0-9A-Za-z._-] is allowed."
        )
    return text


def _validate_partition_key(key: str) -> str:
    if not isinstance(key, str) or not _PARTITION_KEY.fullmatch(key):
        raise ValueError(
            f"Invalid partition key {key!r}: use snake_case ASCII "
            "(lowercase letters, digits and '_', starting with a letter)."
        )
    return key


def partition_dir(base_dir: Path, partitions: Mapping[str, Any]) -> Path:
    """Build ``base_dir/k1=v1/k2=v2`` in the mapping's insertion order."""
    path = Path(base_dir)
    for key, value in partitions.items():
        path = path / f"{_validate_partition_key(key)}={encode_partition_value(value)}"
    return path


def parse_partition_path(path: PurePath, base_dir: PurePath) -> dict[str, str]:
    """Extract ``{key: value}`` from the ``key=value`` segments of ``path``.

    ``path`` may be a partition directory or a ``.parquet`` file inside one;
    segments are read relative to ``base_dir``. Values stay as strings, and
    the missing-value marker is returned untouched. Raises ``ValueError``
    when ``path`` is not under ``base_dir``.
    """
    try:
        relative = path.relative_to(base_dir)
    except ValueError:
        raise ValueError(f"{path} is not under {base_dir}") from None
    parts = list(relative.parts)
    if parts and relative.suffix.lower() == ".parquet":
        parts.pop()
    partitions: dict[str, str] = {}
    for part in parts:
        match = _PARTITION_SEGMENT.match(part)
        if match:
            partitions[match.group(1)] = match.group(2)
    return partitions


@dataclass
class DatasetDescriptor:
    """Model of a dataset's ``_dataset.json``."""

    source_id: str
    dataset_id: str
    alias: str
    partition_keys: list[str] = field(default_factory=list)
    partition_types: dict[str, str] = field(default_factory=dict)
    # "file": partition derived from the raw file's identity (default);
    # "rows": partition derived from row contents (replaced by generation).
    partition_source: str = "file"
    contract_id: str | None = None
    contract_version: int | None = None
    converter: str | None = None
    layout_version: int = LAYOUT_VERSION

    def __post_init__(self) -> None:
        if self.partition_source not in PARTITION_SOURCES:
            raise ValueError(
                f"Invalid partition_source {self.partition_source!r}: "
                f"expected one of {PARTITION_SOURCES}."
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> DatasetDescriptor:
        """Build a descriptor, ignoring unknown keys (newer writers)."""
        known = {f.name for f in fields(cls)}
        data = {k: v for k, v in d.items() if k in known}
        missing = [k for k in ("source_id", "dataset_id", "alias") if k not in data]
        if missing:
            raise ValueError(f"Descriptor is missing required keys: {missing}")
        return cls(**data)

    @classmethod
    def from_json(cls, s: str) -> DatasetDescriptor:
        data = json.loads(s)
        if not isinstance(data, dict):
            raise ValueError("Descriptor JSON must be an object.")
        return cls.from_dict(data)

    def save(self, dataset_dir: Path) -> Path:
        """Atomically write ``<dataset_dir>/_dataset.json``; return its path."""
        return write_text_atomic(
            Path(dataset_dir) / DESCRIPTOR_FILENAME, self.to_json()
        )

    @classmethod
    def load(cls, dataset_dir: Path) -> DatasetDescriptor:
        """Read ``<dataset_dir>/_dataset.json``."""
        path = Path(dataset_dir) / DESCRIPTOR_FILENAME
        return cls.from_json(path.read_text(encoding="utf-8"))


@contextmanager
def dataset_lock(dataset_dir: Path, *, blocking: bool = True) -> Iterator[None]:
    """Hold an exclusive lock on ``<dataset_dir>/.lock``.

    One writer per dataset; readers take no lock. With ``blocking=False`` a
    held lock raises ``BlockingIOError`` immediately. POSIX uses ``flock``;
    Windows locks the file's first byte with ``msvcrt.locking``. Elsewhere
    this is a no-op: callers get no mutual exclusion. The lock file is left
    in place on release, since removing it would race with another process
    waiting on it.
    """
    if fcntl is None:
        if msvcrt is None:
            yield
            return
        with _msvcrt_lock(Path(dataset_dir), blocking=blocking):
            yield
        return
    directory = Path(dataset_dir)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / LOCK_FILENAME).open("a+") as handle:
        flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        fcntl.flock(handle.fileno(), flags)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _msvcrt_lock(directory: Path, *, blocking: bool) -> Iterator[None]:
    """``dataset_lock`` on Windows: lock byte 0 of the lock file.

    ``LK_NBLCK`` fails at once on a held lock; blocking mode retries it, since
    ``LK_LOCK`` gives up after ten one-second attempts.
    """
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / LOCK_FILENAME).open("a+") as handle:
        fd = handle.fileno()
        while True:
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except OSError as exc:
                if not blocking:
                    raise BlockingIOError(
                        f"dataset lock held: {directory / LOCK_FILENAME}"
                    ) from exc
                time.sleep(0.1)
        try:
            yield
        finally:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)


class ConvertOutcome(Protocol):
    """What ``LakeConverter.convert`` reports back (a ``ConvertResult``)."""

    converted: int
    skipped: int
    errors: list[str]


@dataclass(frozen=True)
class Planned:
    """What a converter plans to do with one raw file.

    Returned by the optional ``LakeConverter.inventory``. ``dataset`` is the
    id of the dataset the file feeds (``None`` when it feeds none) and
    ``skip_reason`` says why a file is deliberately not converted:

    - ``"gemeo"``: another file of the same content in a different format
      (the converter reads the twin);
    - ``"nao_tabular"``: documentation, layouts, maps and other non-tabular
      files;
    - ``"superado"``: an older version of a file that has a newer one.

    A file with ``skip_reason`` set is expected to have no Parquet; any other
    file in the inventory is expected to produce one.
    """

    path: Path
    dataset: str | None = None
    skip_reason: str | None = None

    def __post_init__(self) -> None:
        if self.skip_reason is not None and self.skip_reason not in SKIP_REASONS:
            raise ValueError(
                f"Invalid skip_reason {self.skip_reason!r}: "
                f"expected None or one of {SKIP_REASONS}."
            )


@runtime_checkable
class LakeConverter(Protocol):
    """Contract of the object registered under the ``quantilica.lake`` entry point.

    A fetcher exposes one converter per source (conventionally the module
    attribute ``converter`` of its ``lake`` module). Required members:

    - ``source_id``: the source id, also the entry point name;
    - ``datasets()``: every dataset id the converter writes. These ids are
      not always the names of the raw folders, so callers must plan from
      this list, never from the directory listing;
    - ``convert(input_dir, lake_dir, *, datasets=None, force=False)``:
      converts the raw files under ``input_dir`` into ``lake_dir`` and
      returns an object with ``converted``, ``skipped`` and ``errors``.

    Optional member, detected with :func:`converter_inventory` (never call it
    without checking, and never require it):

    - ``inventory(input_dir) -> list[Planned]``: for each raw file under
      ``input_dir``, the dataset it feeds or the reason it is ignored.
      Without it, tools that audit coverage fall back to heuristics (twins by
      base name, a list of non-tabular extensions), which can raise false
      positives.

    ``isinstance`` checks (the protocol is runtime-checkable) only verify that
    the members exist, not their signatures.
    """

    source_id: str

    def datasets(self) -> list[str]: ...

    def convert(
        self,
        input_dir: Path,
        lake_dir: Path,
        *,
        datasets: list[str] | None = None,
        force: bool = False,
    ) -> ConvertOutcome: ...


def converter_inventory(converter: Any, input_dir: Path | str) -> list[Planned] | None:
    """Return ``converter.inventory(input_dir)``, or ``None`` if it has none.

    ``inventory`` is an optional member of :class:`LakeConverter`, so a
    converter without it is not an error: ``None`` tells the caller to fall
    back to its heuristics. An empty list, by contrast, means the converter
    has an inventory and found nothing in ``input_dir``.
    """
    inventory = getattr(converter, "inventory", None)
    if not callable(inventory):
        return None
    return list(inventory(Path(input_dir)))


def _version_stamp(name: str) -> str:
    """Return the ``@date`` stamp of a raw file name as ``AAAAMMDDTHHMMSS`` digits.

    A date-only stamp counts as midnight, so ``@20261021`` and
    ``@20261021T000000`` are the same version. No stamp gives ``""``, which
    sorts before every real stamp.
    """
    match = _VERSION_STAMP.search(_strip_raw_suffixes(name))
    if match is None:
        return ""
    return f"{match[1]}{match[2] or '000000'}"


def _parse_exclude(exclude: Collection[str] | None) -> tuple[set[str], set[str]]:
    """Split ``exclude`` into ``(sha256 digests, normalized relative paths)``."""
    digests: set[str] = set()
    rels: set[str] = set()
    for entry in exclude or ():
        text = str(entry).strip()
        if _SHA256.fullmatch(text):
            digests.add(text.lower())
        elif text:
            rels.add(PurePosixPath(text.replace("\\", "/")).as_posix())
    return digests, rels


def _digest_of(root: Path, rel: str) -> str:
    """The file's ``sha256``: from its manifest sidecar, else hashing the file."""
    manifest = read_manifest_sidecar(root / rel)
    if manifest is not None and manifest.sha256:
        return manifest.sha256.lower()
    return sha256_file(root / rel).lower()


def _select[P: (str, PurePath)](
    paths: Iterable[P],
    exclude: Collection[str] | None,
    root: str | os.PathLike[str] | None,
) -> tuple[list[P], list[P]]:
    """Implementation of :func:`select_current`; also returns the excluded files."""
    digests, excluded_rels = _parse_exclude(exclude)
    root_dir = Path(root) if root is not None else None
    if digests and root_dir is None:
        raise ValueError("exclude by sha256 needs `root` to find the files")
    candidates: dict[tuple[str, str], list[tuple[str, str, P]]] = {}
    sidecars: dict[str, P] = {}
    for item in paths:
        rel = PurePosixPath(str(item).replace("\\", "/"))
        if rel.name.endswith(MANIFEST_SIDECAR_SUFFIX):
            sidecars[rel.as_posix()] = item
        elif not rel.name.endswith(_TEMP_SUFFIX):
            key = (rel.parent.as_posix(), file_key(rel.name))
            stamp = _version_stamp(rel.name)
            candidates.setdefault(key, []).append((stamp, rel.as_posix(), item))
    selected: list[P] = []
    excluded: list[P] = []
    for key in sorted(candidates):
        versions = candidates[key]
        newest = max(stamp for stamp, _, _ in versions)
        for stamp, rel, item in sorted(versions, key=lambda v: v[1]):
            if stamp != newest:
                continue
            if rel in excluded_rels or (
                root_dir is not None
                and digests
                and _digest_of(root_dir, rel) in digests
            ):
                excluded.append(item)
                continue
            selected.append(item)
            sidecar = sidecars.get(rel + MANIFEST_SIDECAR_SUFFIX)
            if sidecar is not None:
                selected.append(sidecar)
    return selected, excluded


def select_current[P: (str, PurePath)](
    paths: Iterable[P],
    *,
    exclude: Collection[str] | None = None,
    root: str | os.PathLike[str] | None = None,
) -> list[P]:
    """Pick the current version of each raw file among ``paths``.

    ``paths`` are raw file paths (relative to a root; the same strings come
    back, in sorted order). Files are grouped by ``(directory,
    file_key(name))``, so the same name in two directories is two files. The
    version is the ``@AAAAMMDD`` or ``@AAAAMMDDTHHMMSS`` stamp of the name
    (a date-only stamp counts as midnight); the newest wins and the others
    are dropped. Rules:

    - A file without a stamp is the oldest possible version: any stamped
      version of the same key beats it, and it wins only when it is alone.
    - Ties are not broken: files of the same key with the same version are
      format twins of one publication (``x@20261021.csv`` and
      ``x@20261021.zip``, or two unstamped formats) and all stay, since
      telling twins apart is the converter's job (``Planned``), and dropping
      one here would hide it from the converter.
    - Manifest sidecars (``<file>.manifest.json``) are never candidates; the
      sidecar of a selected file, when it is in ``paths``, follows it in the
      result. Interrupted atomic writes (``*.tmp``) are ignored.
    - ``exclude`` removes files from the result: entries are ``sha256``
      digests (64 hex digits) and/or relative paths. It is applied *after*
      choosing the version, so excluding the newest (say, a download that is
      corrupt at the source) does not bring an older version back. The
      digest is read from the manifest sidecar under ``root`` when there is
      one, and the file is hashed only when it has none; with no digest in
      ``exclude`` nothing under ``root`` is read, and ``root`` is optional.
    """
    return _select(paths, exclude, root)[0]


@dataclass(frozen=True)
class ViewSummary:
    """Result of :func:`materialize_view`.

    ``files`` counts every file in the view, manifests included, and equals
    ``hardlink + symlink + copy + kept``. The first three count the files
    created in this run by the mode that worked, which may differ from the
    requested one; ``kept`` counts those already in place. ``excluded`` counts
    data files dropped by ``exclude`` and ``removed`` the files deleted from
    ``out`` because they are no longer current.
    """

    files: int = 0
    hardlink: int = 0
    symlink: int = 0
    copy: int = 0
    kept: int = 0
    excluded: int = 0
    removed: int = 0


def _make_link(mode: LinkMode, src: Path, dst: Path) -> None:
    if mode == "hardlink":
        os.link(src, dst)
    elif mode == "symlink":
        os.symlink(os.path.abspath(src), dst)
    else:
        shutil.copy2(src, dst)


def _in_place(src: Path, dst: Path) -> bool:
    """True when ``dst`` already is ``src``: same file, or a copy of it."""
    if not os.path.lexists(dst):
        return False
    try:
        if os.path.samefile(src, dst):
            return True
        if os.path.islink(dst) or not os.path.isfile(dst):
            return False
        a, b = src.stat(), dst.stat()
    except OSError:
        return False
    # copy2 keeps the mtime; allow 2 s for file systems that round it (FAT).
    return a.st_size == b.st_size and abs(a.st_mtime_ns - b.st_mtime_ns) < 2_000_000_000


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def _place(src: Path, dst: Path, mode: LinkMode) -> LinkMode | None:
    """Put ``src`` at ``dst`` by ``mode``, falling back down the chain.

    Returns the mode that worked, or ``None`` when ``dst`` was already in
    place. The link is made under a temporary name and renamed over ``dst``,
    so a reader never sees a missing or half-written file.
    """
    if _in_place(src, dst):
        return None
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.is_dir() and not dst.is_symlink():
        shutil.rmtree(dst)
    tmp = dst.with_name(f".{dst.name}.{os.getpid()}{_TEMP_SUFFIX}")
    for attempt in _LINK_MODES[_LINK_MODES.index(mode) :]:
        _unlink_quiet(tmp)
        try:
            _make_link(attempt, src, tmp)
        except (OSError, NotImplementedError):
            _unlink_quiet(tmp)
            if attempt == "copy":
                raise
            continue
        os.replace(tmp, dst)
        return attempt
    return None  # pragma: no cover - the loop ends in "copy", which returns or raises


def materialize_view(
    raw: str | os.PathLike[str],
    out: str | os.PathLike[str],
    *,
    mode: LinkMode = "hardlink",
    exclude: Collection[str] | None = None,
) -> ViewSummary:
    """Build in ``out`` a tree with only the current version of each raw file.

    ``out`` mirrors the layout of ``raw`` but holds, per file, just the
    version chosen by :func:`select_current` and its manifest sidecar. That
    is what a converter gets as ``input_dir``: what is there is exactly what
    goes to the lake. ``exclude`` has the meaning it has in
    :func:`select_current`.

    ``mode`` is how files are placed: ``"hardlink"`` (no extra space),
    ``"symlink"`` or ``"copy"``. When the requested mode fails (a hardlink
    across volumes, a symlink on Windows without the privilege) the next one
    is tried: hardlink, then symlink, then copy; the summary counts files by
    the mode that worked.

    Idempotent: a file already in place (same file, or a copy of the same
    size and mtime) is kept, and ``out`` is brought to match ``raw``: files
    that stopped being current, or never were, are deleted, and so are empty
    directories. ``out`` belongs to this function, so point it at a
    directory used only for the view; it must not contain or be inside
    ``raw``.
    """
    if mode not in _LINK_MODES:
        raise ValueError(f"Invalid mode {mode!r}: expected one of {_LINK_MODES}.")
    raw_dir, out_dir = Path(raw), Path(out)
    if not raw_dir.is_dir():
        raise NotADirectoryError(f"raw directory not found: {raw_dir}")
    raw_real, out_real = raw_dir.resolve(), out_dir.resolve()
    if (
        raw_real == out_real
        or raw_real in out_real.parents
        or out_real in raw_real.parents
    ):
        raise ValueError(f"raw ({raw_dir}) and out ({out_dir}) must not overlap.")

    rels: list[str] = []
    for current, _, names in os.walk(raw_dir):
        base = Path(current).relative_to(raw_dir)
        rels.extend((base / name).as_posix() for name in names)
    selected, excluded = _select(rels, exclude, raw_dir)

    counts = {"hardlink": 0, "symlink": 0, "copy": 0, "kept": 0}
    for rel in selected:
        placed = _place(raw_dir / rel, out_dir / rel, mode)
        counts[placed or "kept"] += 1

    wanted = {rel for rel in selected}
    removed = 0
    if out_dir.is_dir():
        for current, dirs, names in os.walk(out_dir, topdown=False):
            base = Path(current).relative_to(out_dir)
            for name in names:
                if (base / name).as_posix() not in wanted:
                    (Path(current) / name).unlink()
                    removed += 1
            for name in dirs:
                directory = Path(current) / name
                if directory.is_symlink():
                    directory.unlink()
                    removed += 1
                else:
                    try:
                        directory.rmdir()
                    except OSError:  # not empty: it holds part of the view
                        pass
    return ViewSummary(
        files=len(selected),
        excluded=len(excluded),
        removed=removed,
        **counts,
    )


def load_lake_converter(source_id: str) -> Any | None:
    """Load the ``quantilica.lake`` entry point named ``source_id``.

    Returns ``None`` when no installed package provides one. Errors raised
    while importing a found entry point propagate to the caller.
    """
    for entry_point in entry_points(group=LAKE_ENTRY_POINT_GROUP, name=source_id):
        return entry_point.load()
    return None
