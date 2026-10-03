"""Small file and path helpers used across data projects."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tarfile
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

from .exceptions import StorageError

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_MIN_FREE_MARGIN = 100 * 1024 * 1024  # 100 MB

_ZIP_SUFFIX = ".zip"
_TAR_SUFFIXES = (
    ".tar",
    ".tar.bz2",
    ".tar.gz",
    ".tar.xz",
    ".tbz",
    ".tbz2",
    ".tgz",
    ".txz",
)


def check_free_space(
    path: str | os.PathLike[str],
    required_bytes: int = 0,
    *,
    margin_bytes: int = DEFAULT_MIN_FREE_MARGIN,
) -> bool:
    """Return ``True`` if the filesystem at ``path`` has sufficient free space.

    Args:
        path: Path or directory on the target filesystem.
        required_bytes: Number of bytes expected to be written.
        margin_bytes: Additional free space buffer (defaults to 100 MB).

    Returns:
        bool: True if there is sufficient free space, False otherwise.
    """
    target = Path(path).expanduser()
    dir_path = target if target.is_dir() or not target.suffix else target.parent
    dir_path.mkdir(parents=True, exist_ok=True)
    try:
        usage = shutil.disk_usage(dir_path)
        return usage.free >= (required_bytes + margin_bytes)
    except OSError:
        return True


def ensure_dir(path: str | os.PathLike[str]) -> Path:
    """Create a directory if needed and return it as a resolved Path.

    Args:
        path: The directory path to create.

    Returns:
        Path: The resolved Path to the directory.
    """
    directory = Path(path).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def ensure_parent(path: str | os.PathLike[str]) -> Path:
    """Create the parent directory for a path and return the normalized path.

    Args:
        path: The file path whose parent directory should be created.

    Returns:
        Path: The normalized target path.
    """
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def is_complete_file(
    path: str | os.PathLike[str],
    expected_size: int | None = None,
) -> bool:
    """Return ``True`` if ``path`` is a file already present and complete.

    When ``expected_size`` is given, the file must also match that byte size
    (guards against truncated/partial downloads). Use to skip work that does
    not go through :meth:`HttpClient.download_with_manifest` (which has its
    own freshness check).

    Args:
        path: The file path to check.
        expected_size: Optional expected size in bytes.

    Returns:
        bool: True if the file exists and its size matches expected_size.
    """
    target = Path(path).expanduser()
    if not target.is_file():
        return False
    if expected_size is not None and target.stat().st_size != expected_size:
        return False
    return True


def sha256_bytes(content: bytes) -> str:
    """Return the SHA-256 hex digest for bytes.

    Args:
        content: The byte string to hash.

    Returns:
        str: The SHA-256 hex digest.
    """
    return hashlib.sha256(content).hexdigest()


def sha256_stream(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Return the SHA-256 hex digest for a binary stream.

    The stream is read from its current position.

    Args:
        stream: The binary stream to read from.
        chunk_size: Size of chunks to read.

    Returns:
        str: The SHA-256 hex digest.
    """
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def sha256_file(
    path: str | os.PathLike[str],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:
    """Return the SHA-256 hex digest for a file.

    Args:
        path: Path to the file.
        chunk_size: Size of chunks to read.

    Returns:
        str: The SHA-256 hex digest.

    Raises:
        StorageError: If the file cannot be read.
    """
    target = Path(path).expanduser()
    try:
        with target.open("rb") as stream:
            return sha256_stream(stream, chunk_size=chunk_size)
    except OSError as exc:
        raise StorageError(f"Could not read file for checksum: {target}") from exc


def write_text_atomic(
    path: str | os.PathLike[str],
    content: str,
    encoding: str = "utf-8",
) -> Path:
    """Write text to a file atomically and return the target path.

    Args:
        path: The path to write to.
        content: The text content to write.
        encoding: The string encoding to use.

    Returns:
        Path: The target file path.
    """
    target = ensure_parent(path)
    data = content.encode(encoding)
    return write_bytes_atomic(target, data)


def write_stream_atomic(
    path: str | os.PathLike[str],
    register_callback: Callable[[Callable[[bytes], None]], None],
) -> tuple[str, int]:
    """Stream data into a file atomically; return (sha256_hex, size_bytes).

    ``register_callback`` is called with a write-chunk function as its only
    argument — matching ftplib.retrbinary's callback model::

        ftp.retrbinary("RETR path", register_callback)

    Args:
        path: The destination path.
        register_callback: A function that takes a chunk-writer callback.

    Returns:
        tuple[str, int]: A tuple containing the SHA-256 digest and total bytes written.

    Raises:
        StorageError: If the stream cannot be written atomically.
    """
    target = ensure_parent(path)
    digest = hashlib.sha256()
    size = 0
    fd = -1
    temp_path: Path | None = None
    try:
        fd, raw_temp_path = tempfile.mkstemp(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent
        )
        temp_path = Path(raw_temp_path)
        with os.fdopen(fd, "wb") as stream:
            fd = -1

            def _write_chunk(chunk: bytes) -> None:
                nonlocal size
                stream.write(chunk)
                digest.update(chunk)
                size += len(chunk)

            register_callback(_write_chunk)
            stream.flush()
            os.fsync(stream.fileno())
        temp_path.replace(target)
        return digest.hexdigest(), size
    except OSError as exc:
        raise StorageError(f"Could not write stream atomically: {target}") from exc
    finally:
        if fd != -1:
            os.close(fd)
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def write_bytes_atomic(path: str | os.PathLike[str], content: bytes) -> Path:
    """Write bytes to a file atomically and return the target path.

    Args:
        path: The destination path.
        content: The byte string to write.

    Returns:
        Path: The target file path.

    Raises:
        StorageError: If the file cannot be written atomically.
    """
    target = ensure_parent(path)
    fd = -1
    temp_path: Path | None = None
    try:
        fd, raw_temp_path = tempfile.mkstemp(
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=target.parent,
        )
        temp_path = Path(raw_temp_path)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        temp_path.replace(target)
        return target
    except OSError as exc:
        raise StorageError(f"Could not write file atomically: {target}") from exc
    finally:
        if fd != -1:
            os.close(fd)
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def _is_within(base: Path, candidate: Path) -> bool:
    """Return ``True`` if ``candidate`` resolves strictly inside ``base``.

    Args:
        base: The trusted destination directory.
        candidate: An untrusted candidate path (possibly attacker-built).

    Returns:
        bool: True if candidate stays within base.
    """
    try:
        candidate.relative_to(base)
    except ValueError:
        return False
    return True


def _temp_extraction_dir() -> Path:
    """Create and return a clean temporary directory for extraction.

    Returns:
        Path: The temporary directory path.
    """
    return Path(tempfile.mkdtemp(prefix="quantilica_extract_"))


def _resolve_output_dir(output_dir: Path | str | None) -> Path:
    """Resolve (or create) the extraction destination directory.

    Args:
        output_dir: User-provided destination, or None for a temp dir.

    Returns:
        Path: The resolved destination directory.

    Raises:
        StorageError: If the destination exists as a non-directory.
    """
    if output_dir is None:
        return _temp_extraction_dir()
    dest = Path(output_dir).expanduser()
    if dest.exists() and not dest.is_dir():
        raise StorageError(f"Output path is not a directory: {dest}")
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def _safe_zip_extract(archive: Path, dest: Path) -> list[Path]:
    """Extract a ZIP archive, guarding every member against Zip Slip.

    Args:
        archive: Path to the ``.zip`` archive.
        dest: Destination directory (resolved).

    Returns:
        list[Path]: Paths of regular files extracted (in archive order).

    Raises:
        StorageError: If a member would escape the destination (Zip Slip) or
            the archive is not a valid ZIP.
    """
    resolved_dest = dest.resolve()
    extracted: list[Path] = []
    try:
        with zipfile.ZipFile(archive) as zf:
            for info in zf.infolist():
                target = (resolved_dest / info.filename).resolve()
                if not _is_within(resolved_dest, target):
                    raise StorageError(
                        f"Blocked path traversal in archive {archive.name}: "
                        f"{info.filename}"
                    )
                if info.is_dir():
                    continue
                zf.extract(info, resolved_dest)
                extracted.append(resolved_dest / info.filename)
    except zipfile.BadZipFile as exc:
        raise StorageError(f"Invalid ZIP archive: {archive}") from exc
    return extracted


def _safe_tar_extract(archive: Path, dest: Path) -> list[Path]:
    """Extract a TAR (optionally compressed) archive to ``dest``.

    Uses the stdlib ``filter='data'`` (always available for this package,
    which requires Python >= 3.12) to block path traversal, absolute paths,
    devices, links and setuid bits.

    Args:
        archive: Path to the ``.tar*`` archive.
        dest: Destination directory (resolved).

    Returns:
        list[Path]: Paths of regular files extracted (in archive order).

    Raises:
        StorageError: If the archive is not a valid TAR.
    """
    resolved_dest = dest.resolve()
    extracted: list[Path] = []
    try:
        with tarfile.open(archive, "r:*") as tf:
            tf.extractall(resolved_dest, filter="data")
            for member in tf.getmembers():
                if member.isfile():
                    extracted.append(resolved_dest / member.name)
    except tarfile.TarError as exc:
        raise StorageError(f"Invalid TAR archive: {archive}") from exc
    return extracted


def _seven_zip_extract(archive: Path, dest: Path) -> list[Path]:
    """Extract an archive via the external ``7z`` binary (fallback path).

    Args:
        archive: Path to the archive (``.7z``, ``.rar``, etc.).
        dest: Destination directory.

    Returns:
        list[Path]: Paths of regular files extracted.

    Raises:
        StorageError: If the ``7z`` binary is missing or exits non-zero.
    """
    result = subprocess.run(
        ["7z", "e", str(archive), f"-o{dest}", "-y"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode(errors="replace").strip()
        raise StorageError(
            f"7z failed to decompress {archive}: {stderr or 'unknown error'}"
        )
    return [p for p in dest.iterdir() if p.is_file()]


def _unsupported_suffix(path: Path) -> StorageError:
    """Build the error for a non-extractable archive suffix.

    Args:
        path: The archive path.

    Returns:
        StorageError: Ready-to-raise error for unsupported formats.
    """
    return StorageError(f"Unsupported archive format: {path}")


def _needs_seven_zip(archive: Path, suffix: str) -> bool:
    """Return True when the archive must go through the 7z fallback.

    Args:
        archive: The archive path.
        suffix: The archive suffix (lowercased, e.g. ``.7z``).

    Returns:
        bool: True if 7z fallback is required (False means either stdlib
            handling or an unsupported extension).
    """
    if suffix in (".7z", ".rar"):
        return True
    if suffix == _ZIP_SUFFIX or archive.name.lower().endswith(_TAR_SUFFIXES):
        return False
    return suffix not in ("", ".tar")


def decompress_archive(
    archive_path: Path | str,
    output_dir: Path | str | None = None,
    target_extensions: tuple[str, ...] | list[str] | None = None,
) -> Path:
    """Extract an archive and return the extracted file or destination.

    Supports the formats the stdlib handles natively (``.zip``; ``.tar``,
    ``.tar.gz``/``.tgz``, ``.tar.bz2``, ``.tar.xz``) and falls back to the
    external ``7z`` binary for ``.7z``/``.rar`` and other unknown formats.
    ZIP members are checked against the destination to prevent Zip Slip path
    traversal; TAR uses ``filter='data'`` on Python >= 3.12.

    Resolution rules when ``target_extensions`` is not given:

    - A single extracted file is returned directly.
    - Multiple extracted files: the first matching a common data extension
      (``.csv``, ``.xls``, ``.xlsx``, ``.json``, ...) if any, otherwise the
      destination directory itself.

    Args:
        archive_path: Path to the archive to decompress.
        output_dir: Destination directory. When ``None``, a clean temporary
            directory is created (caller owns removal).
        target_extensions: Optional sequence of extensions (lower-cased, with
            dot, e.g. ``('.csv', '.xls')``) — the first extracted file whose
            suffix matches (case-insensitive) is returned.

    Returns:
        Path: The chosen extracted file, or the destination directory when
        no unique file can be determined.

    Raises:
        StorageError: If the archive does not exist, its format has no
            available handler (including missing ``7z`` binary), extraction
            fails, or produces no files.
    """
    archive = Path(archive_path).expanduser()
    if not archive.is_file():
        raise StorageError(f"Archive not found: {archive}")
    dest = _resolve_output_dir(output_dir)
    suffix = archive.suffix.lower()

    if suffix == _ZIP_SUFFIX:
        extracted = _safe_zip_extract(archive, dest)
    elif archive.name.lower().endswith(_TAR_SUFFIXES) or suffix == ".tar":
        extracted = _safe_tar_extract(archive, dest)
    elif _needs_seven_zip(archive, suffix):
        try:
            extracted = _seven_zip_extract(archive, dest)
        except FileNotFoundError as exc:
            raise StorageError(
                f"Cannot decompress {archive}: the external '7z' binary is "
                "not installed. Install p7zip and retry, or preconvert the "
                "archive to .zip/.tar.gz."
            ) from exc
    else:
        raise _unsupported_suffix(archive)

    if not extracted:
        raise StorageError(f"Archive produced no files: {archive}")
    return _pick_result(extracted, dest, target_extensions)


def _pick_result(
    extracted: list[Path],
    dest: Path,
    target_extensions: tuple[str, ...] | list[str] | None,
) -> Path:
    """Choose the result path among extracted files per resolution rules.

    Args:
        extracted: Extracted regular files.
        dest: Destination directory.
        target_extensions: Optional requested extensions filter.

    Returns:
        Path: The chosen file or the destination directory.
    """
    extensions = tuple(
        ext.lower() if ext.startswith(".") else f".{ext.lower()}"
        for ext in (target_extensions or ())
    )
    if extensions:
        for candidate in extracted:
            if candidate.suffix.lower() in extensions:
                return candidate
        raise StorageError(
            f"No extracted file matches target extensions {extensions} in {dest}"
        )
    if len(extracted) == 1:
        return extracted[0]
    common = (".csv", ".json", ".xls", ".xlsx", ".txt", ".xml")
    for candidate in extracted:
        if candidate.suffix.lower() in common:
            return candidate
    return dest
