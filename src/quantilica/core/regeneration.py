"""Regeneration detector: same data, new stamp.

Several public sources rewrite every file each night: a generation-timestamp
column (``DatGeracaoConjuntoDados`` on Câmara and ANEEL files), a new ``ETag``
or ``Last-Modified``, sometimes a different line order of the header comment.
The raw ``sha256`` changes, the *data* does not, and the lake gets reconverted
for nothing.

This module compares a **normalized** hash instead:

- :class:`RegenerationRule` says how to normalize a dataset (which stamp
  columns to ignore and how to read the CSV). Its :attr:`~RegenerationRule.name`
  is stored in ``DownloadManifest.normalizer`` next to ``content_sha256``, so a
  hash is only compared with a hash produced by the same rule.
- :func:`normalized_content_sha256` computes that hash, streaming.
- :func:`detect_regeneration` compares a new file with the previous manifest
  and answers ``"skipped"`` (only the stamp or the ``ETag`` changed),
  ``"changed"`` (real change or not comparable) or ``"new"`` (no previous).
- :func:`with_content_hash` records the hash in a manifest.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import BinaryIO, Literal

from .manifests import DownloadManifest

__all__ = [
    "RegenerationResult",
    "RegenerationRule",
    "detect_regeneration",
    "normalized_content_sha256",
    "with_content_hash",
]

RegenerationStatus = Literal["skipped", "changed", "new"]

_FIELD_SEP = b"\x1f"
_ROW_SEP = b"\x1e"
_EXTRA_MARK = "\x1d"
"""Separates the kept columns from fields beyond the header in the hash."""


@dataclass(frozen=True)
class RegenerationRule:
    """How to normalize one dataset's CSV before hashing.

    Attributes:
        stamp_columns: Header names to drop (compared after ``strip``, case
            insensitive), e.g.
            ``("DatGeracaoConjuntoDados",)``. May be empty (then only the
            container differences are ignored: encoding BOM, line endings,
            gzip).
        delimiter: CSV field delimiter.
        encoding: Text encoding of the file (BOM is tolerated).
        sort_rows: Ignore row order. Needs the whole file in memory; use it
            only for sources that shuffle rows between generations.
    """

    stamp_columns: tuple[str, ...] = ()
    delimiter: str = ";"
    encoding: str = "utf-8"
    sort_rows: bool = False
    _lowered: frozenset[str] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_lowered",
            frozenset(c.strip().casefold() for c in self.stamp_columns),
        )

    @property
    def name(self) -> str:
        """Stable identifier stored in ``DownloadManifest.normalizer``."""
        cols = ",".join(sorted(self._lowered))
        flags = "sorted" if self.sort_rows else "ordered"
        return (
            f"csv-v1;delim={self.delimiter!r};enc={self.encoding};{flags};drop={cols}"
        )


@dataclass(frozen=True)
class RegenerationResult:
    """Outcome of :func:`detect_regeneration`.

    Attributes:
        status: ``"skipped"`` when only stamp/ETag changed (do not
            reconvert), ``"changed"`` when the data differs or the previous
            hash is not comparable, ``"new"`` when there is no previous.
        content_sha256: Normalized hash of the new file.
        normalizer: The rule name that produced it.
        raw_changed: The raw bytes differ from the previous download
            (``None`` when unknown).
        etag_changed: The ``ETag`` differs from the previous one (``None``
            when either side lacks it).
        reason: Short human-readable explanation, for logs.
    """

    status: RegenerationStatus
    content_sha256: str
    normalizer: str
    raw_changed: bool | None = None
    etag_changed: bool | None = None
    reason: str = ""

    @property
    def skipped(self) -> bool:
        """True when the file is a regeneration with no real change."""
        return self.status == "skipped"


def _open_binary(
    source: bytes | str | Path | BinaryIO,
) -> tuple[BinaryIO, list[BinaryIO]]:
    """Open ``source`` as a binary stream, un-gzipping when it is gzip.

    Returns:
        tuple: The stream to read and the streams the caller must close (the
        ``GzipFile`` and the file under it; never a stream the caller passed).
    """
    owned: list[BinaryIO] = []
    if isinstance(source, bytes):
        raw: BinaryIO = io.BytesIO(source)
        owned.append(raw)
    elif isinstance(source, str | Path):
        raw = open(source, "rb")  # noqa: SIM115 - closed by the caller
        owned.append(raw)
    else:
        raw = source
    try:
        magic = raw.read(2)
        raw.seek(raw.tell() - len(magic))
        if magic == b"\x1f\x8b":
            gz = gzip.GzipFile(fileobj=raw)
            owned.insert(0, gz)  # close the GzipFile before the file under it
            return gz, owned
    except BaseException:
        for stream in owned:
            stream.close()
        raise
    return raw, owned


def _rows(stream: BinaryIO, rule: RegenerationRule) -> Iterable[tuple[str, ...]]:
    encoding = rule.encoding
    if encoding.lower().replace("_", "-") in {"utf-8", "utf8"}:
        encoding = "utf-8-sig"  # tolerate a BOM
    text = io.TextIOWrapper(stream, encoding=encoding, newline="")
    try:
        keep: list[int] | None = None
        width = 0
        for row in csv.reader(text, delimiter=rule.delimiter):
            if keep is None:
                header = [c.strip() for c in row]
                width = len(header)
                keep = [
                    i for i, c in enumerate(header) if c.casefold() not in rule._lowered
                ]
                yield tuple(header[i] for i in keep)
            elif row:  # skip blank lines
                kept = tuple(row[i] if i < len(row) else "" for i in keep)
                if len(row) > width:
                    # Fields past the header are data too: hash them (marked,
                    # so they never pass for a kept column).
                    kept = (*kept, _EXTRA_MARK, *row[width:])
                yield kept
    finally:
        text.detach()  # never close a stream we do not own


def normalized_content_sha256(
    source: bytes | str | Path | BinaryIO, rule: RegenerationRule
) -> str:
    """Hash a CSV ignoring stamp columns, line endings, BOM and gzip.

    Args:
        source: Bytes, a path or a binary stream (gzip is detected by magic).
        rule: The normalization rule.

    Returns:
        str: Hex SHA-256 of the canonical serialization of the kept columns.
    """
    stream, owned = _open_binary(source)
    try:
        digest = hashlib.sha256()
        rows = _rows(stream, rule)
        if rule.sort_rows:
            iterator = iter(rows)
            header = next(iterator, None)
            body = sorted(iterator)
            ordered: Iterable[tuple[str, ...]] = (
                [header] if header is not None else []
            ) + body
        else:
            ordered = rows
        for row in ordered:
            digest.update(_FIELD_SEP.join(v.encode("utf-8") for v in row))
            digest.update(_ROW_SEP)
        return digest.hexdigest()
    finally:
        for closeable in owned:
            closeable.close()


def with_content_hash(
    manifest: DownloadManifest, content_sha256: str, rule: RegenerationRule
) -> DownloadManifest:
    """Return ``manifest`` with ``content_sha256`` and ``normalizer`` set."""
    return replace(manifest, content_sha256=content_sha256, normalizer=rule.name)


def detect_regeneration(
    new_source: bytes | str | Path | BinaryIO,
    rule: RegenerationRule,
    *,
    previous_manifest: DownloadManifest | None = None,
    previous_source: bytes | str | Path | BinaryIO | None = None,
    new_sha256: str | None = None,
    new_etag: str | None = None,
) -> RegenerationResult:
    """Decide whether a new download is just a regeneration of the previous.

    The previous normalized hash comes from ``previous_manifest.content_sha256``
    (only when its ``normalizer`` equals ``rule.name``); otherwise it is
    computed from ``previous_source`` when given. Without a comparable
    previous hash the result is ``"changed"`` (never ``"skipped"`` by guess),
    or ``"new"`` when there is no previous at all.

    Args:
        new_source: The just-downloaded file (bytes, path or binary stream).
        rule: The dataset's normalization rule.
        previous_manifest: Manifest of the latest local version.
        previous_source: The latest local file, as fallback when the manifest
            has no comparable ``content_sha256``.
        new_sha256: Raw SHA-256 of the new file, to fill ``raw_changed``.
        new_etag: ``ETag`` of the new response, to fill ``etag_changed``.

    Returns:
        RegenerationResult: ``status`` is ``"skipped"`` only when the
        normalized hashes are equal.
    """
    new_hash = normalized_content_sha256(new_source, rule)
    if previous_manifest is None and previous_source is None:
        return RegenerationResult(
            "new", new_hash, rule.name, reason="no previous version"
        )

    raw_changed: bool | None = None
    etag_changed: bool | None = None
    previous_hash: str | None = None
    if previous_manifest is not None:
        if new_sha256 and previous_manifest.sha256:
            raw_changed = new_sha256.lower() != previous_manifest.sha256.lower()
        prev_etag = (
            previous_manifest.source_meta.etag
            if previous_manifest.source_meta
            else None
        )
        if new_etag and prev_etag:
            etag_changed = new_etag != prev_etag
        if (
            previous_manifest.content_sha256
            and previous_manifest.normalizer == rule.name
        ):
            previous_hash = previous_manifest.content_sha256
    if previous_hash is None and previous_source is not None:
        previous_hash = normalized_content_sha256(previous_source, rule)

    if previous_hash is None:
        return RegenerationResult(
            "changed",
            new_hash,
            rule.name,
            raw_changed,
            etag_changed,
            reason="previous version has no comparable content hash",
        )
    if previous_hash == new_hash:
        what = "stamp/ETag only" if raw_changed or etag_changed else "identical"
        return RegenerationResult(
            "skipped", new_hash, rule.name, raw_changed, etag_changed, reason=what
        )
    return RegenerationResult(
        "changed",
        new_hash,
        rule.name,
        raw_changed,
        etag_changed,
        reason="normalized content differs",
    )
