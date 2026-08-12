"""Small file and path helpers used across data projects."""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

from .exceptions import StorageError

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_MIN_FREE_MARGIN = 100 * 1024 * 1024  # 100 MB


def check_free_space(
    path: str | os.PathLike[str],
    required_bytes: int = 0,
    *,
    margin_bytes: int = DEFAULT_MIN_FREE_MARGIN,
) -> bool:
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
