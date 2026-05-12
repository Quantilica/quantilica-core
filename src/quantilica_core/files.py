"""Small file and path helpers used across data projects."""

from __future__ import annotations

import hashlib
import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

from .exceptions import StorageError

DEFAULT_CHUNK_SIZE = 1024 * 1024


def ensure_dir(path: str | os.PathLike[str]) -> Path:
    """Create a directory if needed and return it as a resolved Path."""
    directory = Path(path).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def ensure_parent(path: str | os.PathLike[str]) -> Path:
    """Create the parent directory for a path and return the normalized path."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def sha256_bytes(content: bytes) -> str:
    """Return the SHA-256 hex digest for bytes."""
    return hashlib.sha256(content).hexdigest()


def sha256_stream(stream: BinaryIO, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
    """Return the SHA-256 hex digest for a binary stream.

    The stream is read from its current position.
    """
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(chunk_size), b""):
        digest.update(chunk)
    return digest.hexdigest()


def sha256_file(
    path: str | os.PathLike[str],
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> str:
    """Return the SHA-256 hex digest for a file."""
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
    """Write text to a file atomically and return the target path."""
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
    """Write bytes to a file atomically and return the target path."""
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
