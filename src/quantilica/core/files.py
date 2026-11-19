"""Small file and path helpers used across data projects."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import warnings
import zipfile
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from .exceptions import CorruptPayloadError, EmptyPayloadError, StorageError

DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_MIN_FREE_MARGIN = 100 * 1024 * 1024  # 100 MB

STALE_TMP_ENV = "QUANTILICA_TMP_MAX_AGE_HOURS"
"""Variável de ambiente com a idade mínima (em horas) de um ``.tmp`` órfão."""

DEFAULT_STALE_TMP_HOURS = 48.0
"""Idade padrão: acima do maior ``timeout`` dos jobs de sync do ecossistema."""

MIN_STALE_TMP_HOURS = 6.0
"""Piso: abaixo disso um download lento de outro processo correria risco."""

_LOGGER = logging.getLogger(__name__)
# ``tempfile.mkstemp`` sufixa 8 caracteres de [a-z0-9_] entre o nome e ``.tmp``.
_MKSTEMP_RANDOM = r"[A-Za-z0-9_]{8}"

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
    # Mede no ancestral existente mais próximo, sem criar nada: inferir
    # "diretório" pela falta de extensão criava uma pasta no lugar de um
    # arquivo sem extensão (ex.: "API pública") e quebrava o rename final.
    dir_path = Path(path).expanduser()
    while not dir_path.is_dir() and dir_path != dir_path.parent:
        dir_path = dir_path.parent
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


_DATA_SUFFIXES = frozenset(
    {
        ".csv",
        ".tsv",
        ".txt",
        ".zip",
        ".7z",
        ".xls",
        ".xlsx",
        ".json",
        ".dbc",
        ".dbf",
        ".parquet",
    }
)
_HTML_PREFIXES = (b"<!doctype html", b"<html")


def check_payload(
    path: str | os.PathLike[str],
    name: str | None = None,
    *,
    expect_header: Callable[[bytes], bool] | None = None,
    allow_header_only: bool = False,
    require_final_newline: bool = False,
) -> None:
    """Reject a downloaded file whose content contradicts its name.

    ``name`` is the final file name (``path`` may be a temporary file). A data
    file (``.csv``, ``.zip``...) that starts with an HTML document is a portal's
    error page; a ``.zip``/``.xlsx`` must pass ``testzip``; a ``.7z`` must pass
    ``7z t`` (skipped when no 7z binary is installed); a ``.parquet`` must have
    at least 12 bytes and begin/end with ``PAR1``.

    If ``expect_header`` is provided, it receives the first non-empty line of the file.
    If ``allow_header_only`` is False, a text/CSV file containing only a header or empty
    raises ``EmptyPayloadError``.

    If ``require_final_newline`` is True, a non-empty ``.csv``/``.tsv``/``.txt``
    must end in ``\\n`` (so ``\\r\\n`` also passes); a missing final newline
    signals a truncated transfer. It is off by default because legitimate
    files may lack it; ``download_with_manifest`` enables it only when the
    download size could not be verified (no ``Content-Length``, ``Content-Range``
    total or size-bearing ``ETag``).

    Raises:
        CorruptPayloadError: If the content is not valid for the extension.
        EmptyPayloadError: If the file is empty or only contains a header.
    """
    path = Path(path)
    suffix = Path(name or path.name).suffix.lower()
    if suffix not in _DATA_SUFFIXES:
        return
    with open(path, "rb") as fh:
        head = fh.read(512).lstrip(b"\xef\xbb\xbf \t\r\n").lower()
    _waf_markers = (
        b"the requested url was rejected",
        b"access denied",
        b"attention required! | cloudflare",
        b"servi\xc3\xa7o em manuten\xc3\xa7\xc3\xa3o",
        b"servico em manutencao",
        b"sistema temporariamente indispon",
        b"request rejected",
    )
    if (
        head.startswith(_HTML_PREFIXES)
        or head.startswith((b"<head", b"<body", b"<script"))
        or any(m in head for m in _waf_markers)
    ):
        raise CorruptPayloadError(f"{name or path.name}: página HTML no lugar do dado")

    if expect_header is not None:
        with open(path, "rb") as fh:
            first_line = b""
            for line in fh:
                stripped = line.lstrip(b"\xef\xbb\xbf").strip()
                if stripped:
                    first_line = stripped
                    break
        if not expect_header(first_line):
            raise CorruptPayloadError(
                f"{name or path.name}: cabeçalho não confere com o esperado"
            )

    if require_final_newline and suffix in (".csv", ".tsv", ".txt"):
        size = path.stat().st_size
        if size > 0:
            with open(path, "rb") as fh:
                fh.seek(size - 1)
                last = fh.read(1)
            if last != b"\n":
                raise CorruptPayloadError(
                    f"{name or path.name}: não termina em quebra de linha "
                    "(download possivelmente truncado)"
                )

    if suffix in (".csv", ".txt"):
        with open(path, "rb") as fh:
            lines = 0
            for line in fh:
                if line.strip():
                    lines += 1
                    if lines > 1:
                        break
        if lines == 0:
            raise EmptyPayloadError(f"{name or path.name}: arquivo vazio")
        if lines == 1 and not allow_header_only:
            raise EmptyPayloadError(
                f"{name or path.name}: arquivo contendo apenas cabeçalho"
            )

    if suffix in (".zip", ".xlsx"):
        try:
            with zipfile.ZipFile(path) as archive:
                bad = archive.testzip()
        except (zipfile.BadZipFile, OSError, EOFError) as exc:
            raise CorruptPayloadError(
                f"{name or path.name}: ZIP inválido: {exc}"
            ) from exc
        if bad is not None:
            raise CorruptPayloadError(f"{name or path.name}: CRC inválido em {bad}")
    elif suffix == ".7z":
        tool = shutil.which("7z") or shutil.which("7zz") or shutil.which("7za")
        if tool is None:
            return
        proc = subprocess.run(
            [tool, "t", "-bd", str(path)], capture_output=True, text=True
        )
        if proc.returncode != 0:
            raise CorruptPayloadError(
                f"{name or path.name}: 7z inválido: {proc.stdout.strip()[-200:]}"
            )
    elif suffix == ".parquet":
        size = path.stat().st_size
        if size < 12:
            raise CorruptPayloadError(
                f"{name or path.name}: Parquet inválido: tamanho {size} < 12 bytes"
            )
        with open(path, "rb") as fh:
            magic_head = fh.read(4)
            fh.seek(-4, os.SEEK_END)
            magic_tail = fh.read(4)
        if magic_head != b"PAR1" or magic_tail != b"PAR1":
            raise CorruptPayloadError(
                f"{name or path.name}: Parquet inválido: assinatura PAR1 ausente"
            )


def stale_tmp_max_age_hours(value: float | None = None) -> float | None:
    """Resolve a idade mínima (em horas) para considerar um ``.tmp`` órfão.

    Ordem: ``value`` explícito, variável ``QUANTILICA_TMP_MAX_AGE_HOURS`` e o
    padrão de 48 h. Um valor menor ou igual a zero desliga a limpeza (devolve
    ``None``). Valor não finito (``nan``, ``inf``), abaixo do piso de 6 h ou
    inválido na variável cai no padrão de 48 h com aviso: nunca vira "apague
    tudo", porque um ``.tmp`` recente pode ser de outro processo baixando o
    mesmo arquivo.
    """
    if value is None:
        raw = os.environ.get(STALE_TMP_ENV)
        if raw is None or not raw.strip():
            return DEFAULT_STALE_TMP_HOURS
        try:
            value = float(raw)
        except ValueError:
            _LOGGER.warning(
                "%s=%r inválido; usando o padrão de %s h",
                STALE_TMP_ENV,
                raw,
                DEFAULT_STALE_TMP_HOURS,
            )
            return DEFAULT_STALE_TMP_HOURS
    if not math.isfinite(value) or 0 < value < MIN_STALE_TMP_HOURS:
        # nan/inf/abaixo do piso nunca viram "apague tudo": volta ao padrão
        _LOGGER.warning(
            "idade mínima %r h recusada (precisa ser finita e de pelo menos "
            "%s h); usando o padrão de %s h",
            value,
            MIN_STALE_TMP_HOURS,
            DEFAULT_STALE_TMP_HOURS,
        )
        return DEFAULT_STALE_TMP_HOURS
    if value <= 0:
        return None
    return float(value)


def remove_stale_atomic_temps(
    target: str | os.PathLike[str],
    *,
    max_age_hours: float | None = None,
    now: float | None = None,
) -> list[Path]:
    """Remove ``.tmp`` órfãos **do mesmo alvo** deixados por downloads mortos.

    Os downloads atômicos criam ``.<nome>.<8 aleatórios>.tmp`` ao lado do alvo.
    Se o processo morre sem passar pelo ``finally`` (``SIGTERM`` do ``timeout``,
    ``SIGKILL``, queda de energia), o arquivo fica para sempre. Antes de criar o
    novo ``.tmp``, esta função apaga os antigos do mesmo ``<nome>`` na mesma
    pasta cuja ``mtime`` passa de ``max_age_hours`` (padrão 48 h, ver
    :func:`stale_tmp_max_age_hours`).

    Só os downloads em streaming chamam esta função (``write_stream_atomic`` e
    o download do ``http``): os ``.tmp`` de ``write_bytes_atomic`` vivem
    milissegundos e varrer a pasta a cada gravação custaria caro em pastas com
    dezenas de milhares de arquivos.

    Garantias: só toca arquivos regulares (nunca symlink) com o padrão exato do
    ``mkstemp`` deste alvo; um ``.tmp`` recente (talvez de outro processo baixando
    o mesmo arquivo) nunca é removido; qualquer falha vira aviso no log, jamais
    erro do download.

    Returns:
        list[Path]: Os arquivos removidos.
    """
    removed: list[Path] = []
    try:
        age_hours = stale_tmp_max_age_hours(max_age_hours)
        if age_hours is None:
            return removed
        target_path = Path(target).expanduser()
        parent = target_path.parent
        prefix = f".{target_path.name}."
        pattern = re.compile(re.escape(prefix) + _MKSTEMP_RANDOM + re.escape(".tmp"))
        cutoff = (time.time() if now is None else now) - age_hours * 3600
        try:
            entries = list(os.scandir(parent))
        except FileNotFoundError:
            return removed
        for entry in entries:
            if not entry.name.startswith(prefix) or not pattern.fullmatch(entry.name):
                continue
            try:
                if not entry.is_file(follow_symlinks=False):
                    continue
                if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                    continue
                os.unlink(entry.path)
                removed.append(Path(entry.path))
                _LOGGER.info(
                    "Removido .tmp órfão de %s: %s", target_path.name, entry.path
                )
            except FileNotFoundError:
                continue  # outro processo removeu antes
            except OSError as exc:
                _LOGGER.warning(
                    "Não foi possível remover o .tmp órfão %s: %s", entry.path, exc
                )
    except Exception as exc:  # a limpeza jamais derruba o download
        _LOGGER.warning("Falha ao limpar .tmp órfãos de %s: %s", target, exc)
    return removed


def write_stream_atomic(
    path: str | os.PathLike[str],
    register_callback: Callable[[Callable[[bytes], None]], None],
    validate: Callable[[Path], None] | None = None,
) -> tuple[str, int]:
    """Stream data into a file atomically; return (sha256_hex, size_bytes).

    ``register_callback`` is called with a write-chunk function as its only
    argument — matching ftplib.retrbinary's callback model::

        ftp.retrbinary("RETR path", register_callback)

    Args:
        path: The destination path.
        register_callback: A function that takes a chunk-writer callback.
        validate: Called with the complete temporary file before it replaces
            ``path``; an exception leaves ``path`` untouched.

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
    remove_stale_atomic_temps(target)
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
        if validate is not None:
            validate(temp_path)
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


_EXTRACT_PREFIX = "quantilica_extract_"


def _temp_extraction_dir(parent: Path | str | None = None) -> Path:
    """Create and return a clean temporary directory for extraction.

    Args:
        parent: Directory that will hold the new folder. ``None`` uses the
            system temporary directory (``TMPDIR``). Created if missing.

    Returns:
        Path: The temporary directory path.
    """
    if parent is not None:
        parent = Path(parent).expanduser()
        parent.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=_EXTRACT_PREFIX, dir=parent))


def _rmtree_robust(path: Path) -> None:
    """Remove ``path`` recursively without ever raising.

    Read-only entries are made writable and retried. Any failure that remains
    becomes a ``RuntimeWarning`` so cleanup never masks an exception that is
    already propagating.

    Args:
        path: Directory to remove.
    """

    def _on_error(func: Callable[..., object], name: str, exc: BaseException) -> None:
        if isinstance(exc, FileNotFoundError):
            return
        try:
            target = Path(name)
            for node in (target, target.parent):
                if node.exists() and not node.is_symlink():
                    node.chmod(node.stat().st_mode | stat.S_IRWXU)
            func(name)
        except OSError as retry_exc:
            warnings.warn(
                f"Could not remove extraction path {name}: {retry_exc}",
                RuntimeWarning,
                stacklevel=2,
            )

    try:
        shutil.rmtree(path, onexc=_on_error)
    except OSError as exc:  # pragma: no cover - defensive
        warnings.warn(
            f"Could not remove extraction directory {path}: {exc}",
            RuntimeWarning,
            stacklevel=2,
        )


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


def _safe_zip_extract(archive: Path, dest: Path) -> tuple[list[Path], int]:
    """Extract a ZIP archive, guarding every member against Zip Slip.

    Junk members (:func:`_is_junk_name`) are checked like any other but never
    written to ``dest``.

    Args:
        archive: Path to the ``.zip`` archive.
        dest: Destination directory (resolved).

    Returns:
        tuple[list[Path], int]: Paths of regular files extracted (in archive
        order) and how many junk files were skipped.

    Raises:
        StorageError: If a member would escape the destination (Zip Slip) or
            the archive is not a valid ZIP.
    """
    resolved_dest = dest.resolve()
    extracted: list[Path] = []
    skipped = 0
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
                if _is_junk_name(info.filename):
                    skipped += 1
                    continue
                zf.extract(info, resolved_dest)
                extracted.append(resolved_dest / info.filename)
    except zipfile.BadZipFile as exc:
        raise StorageError(f"Invalid ZIP archive: {archive}") from exc
    return extracted, skipped


def _safe_tar_extract(archive: Path, dest: Path) -> tuple[list[Path], int]:
    """Extract a TAR (optionally compressed) archive to ``dest``.

    Uses the stdlib ``filter='data'`` (always available for this package,
    which requires Python >= 3.12) to block path traversal, absolute paths,
    devices, links and setuid bits. Junk members (:func:`_is_junk_name`) are
    never written to ``dest``.

    Args:
        archive: Path to the ``.tar*`` archive.
        dest: Destination directory (resolved).

    Returns:
        tuple[list[Path], int]: Paths of regular files extracted (in archive
        order) and how many junk files were skipped.

    Raises:
        StorageError: If the archive is not a valid TAR.
    """
    resolved_dest = dest.resolve()
    extracted: list[Path] = []
    skipped = 0
    try:
        with tarfile.open(archive, "r:*") as tf:
            members = tf.getmembers()
            wanted = [m for m in members if not _is_junk_name(m.name)]
            tf.extractall(resolved_dest, members=wanted, filter="data")
            for member in members:
                if not member.isfile():
                    continue
                if _is_junk_name(member.name):
                    skipped += 1
                else:
                    extracted.append(resolved_dest / member.name)
    except tarfile.TarError as exc:
        raise StorageError(f"Invalid TAR archive: {archive}") from exc
    return extracted, skipped


def _seven_zip_extract(archive: Path, dest: Path) -> tuple[list[Path], int]:
    """Extract an archive via the external ``7z`` binary (fallback path).

    ``7z e`` flattens the tree, so junk is recognised by file name. Junk files
    that this extraction created are removed afterwards; junk that was already
    in ``dest`` before is left alone (it belongs to the caller) but is not
    returned.

    Args:
        archive: Path to the archive (``.7z``, ``.rar``, etc.).
        dest: Destination directory.

    Returns:
        tuple[list[Path], int]: Paths of regular files extracted and how many
        junk files were skipped.

    Raises:
        StorageError: If the ``7z`` binary is missing or exits non-zero.
    """
    before = {p.name for p in dest.iterdir() if p.is_file()}
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
    kept: list[Path] = []
    skipped = 0
    for p in dest.iterdir():
        if not p.is_file():
            continue
        if _is_junk_name(p.name):
            skipped += 1
            if p.name not in before:
                p.unlink(missing_ok=True)
        else:
            kept.append(p)
    return kept, skipped


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


def _extract_into(
    archive: Path,
    dest: Path,
    target_extensions: tuple[str, ...] | list[str] | None,
) -> Path:
    """Extract ``archive`` into the existing ``dest`` and pick the result.

    Args:
        archive: Existing archive file.
        dest: Existing destination directory.
        target_extensions: Optional extensions filter (see
            :func:`decompress_archive`).

    Returns:
        Path: The chosen extracted file, or ``dest``.

    Raises:
        StorageError: On unsupported format, missing ``7z``, failed extraction
            or an archive without files.
    """
    suffix = archive.suffix.lower()
    if suffix == _ZIP_SUFFIX:
        extracted, skipped = _safe_zip_extract(archive, dest)
    elif archive.name.lower().endswith(_TAR_SUFFIXES) or suffix == ".tar":
        extracted, skipped = _safe_tar_extract(archive, dest)
    elif _needs_seven_zip(archive, suffix):
        try:
            extracted, skipped = _seven_zip_extract(archive, dest)
        except FileNotFoundError as exc:
            raise StorageError(
                f"Cannot decompress {archive}: the external '7z' binary is "
                "not installed. Install p7zip and retry, or preconvert the "
                "archive to .zip/.tar.gz."
            ) from exc
    else:
        raise _unsupported_suffix(archive)

    if not extracted and not skipped:
        raise StorageError(f"Archive produced no files: {archive}")
    # Only junk inside: nothing was written; the picker returns ``dest`` (or
    # raises when ``target_extensions`` asks for something).
    return _pick_result(extracted, dest, target_extensions)


def _require_archive(archive_path: Path | str) -> Path:
    """Return the archive path, or raise if it is not an existing file."""
    archive = Path(archive_path).expanduser()
    if not archive.is_file():
        raise StorageError(f"Archive not found: {archive}")
    return archive


def decompress_archive(
    archive_path: Path | str,
    output_dir: Path | str | None = None,
    target_extensions: tuple[str, ...] | list[str] | None = None,
) -> Path:
    """Extract an archive and return the extracted file or destination.

    .. warning::
       With ``output_dir=None`` this function creates a temporary directory
       (``quantilica_extract_*``) that **the caller must remove**, and the
       directory to remove is the *root* of the extraction, not the parent of
       the returned file (an archive with subfolders returns a nested path).
       Forgetting it leaks the extracted data. That form is deprecated and
       emits :class:`DeprecationWarning`; use :func:`extracted_archive`,
       which always removes the root. An explicit ``output_dir`` is never
       removed.

    Supports the formats the stdlib handles natively (``.zip``; ``.tar``,
    ``.tar.gz``/``.tgz``, ``.tar.bz2``, ``.tar.xz``) and falls back to the
    external ``7z`` binary for ``.7z``/``.rar`` and other unknown formats.
    ZIP members are checked against the destination to prevent Zip Slip path
    traversal; TAR uses ``filter='data'`` on Python >= 3.12.

    Junk members are never written to the destination: Office lock files
    (``~$*``), ``md5_*`` checksums, ``Thumbs.db``, ``.DS_Store`` and
    ``__MACOSX/`` entries (names compared case-insensitively). An archive with
    only junk extracts nothing and returns the destination directory. For
    ``.7z``/``.rar`` the external tool writes everything, so the junk files
    that extraction created are removed afterwards (junk already in
    ``output_dir`` is left alone).

    Resolution rules when ``target_extensions`` is not given:

    - A single extracted file is returned directly.
    - Multiple extracted files: the first matching a common data extension
      (``.csv``, ``.xls``, ``.xlsx``, ``.json``, ...) if any, otherwise the
      destination directory itself.

    Args:
        archive_path: Path to the archive to decompress.
        output_dir: Destination directory (owned and removed by the caller).
            When ``None``, a clean temporary directory is created (deprecated:
            the caller owns removal; if extraction itself fails the directory
            is removed here before the error propagates).
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
    archive = _require_archive(archive_path)
    if output_dir is not None:
        return _extract_into(
            archive, _resolve_output_dir(output_dir), target_extensions
        )

    warnings.warn(
        "decompress_archive() without output_dir creates a temporary "
        "directory that the caller must remove (its root is not "
        "result.parent when the archive has subfolders); use "
        "quantilica.core.files.extracted_archive(), which always removes it.",
        DeprecationWarning,
        stacklevel=2,
    )
    dest = _temp_extraction_dir()
    try:
        return _extract_into(archive, dest, target_extensions)
    except BaseException:
        _rmtree_robust(dest)
        raise


@contextmanager
def extracted_archive(
    archive_path: Path | str,
    *,
    target_extensions: tuple[str, ...] | list[str] | None = None,
    dir: Path | str | None = None,  # noqa: A002 - mirrors tempfile.mkdtemp
) -> Iterator[Path]:
    """Extract an archive into a temporary root that is always removed.

    Context-manager form of :func:`decompress_archive` that owns the
    extraction directory: it creates ``quantilica_extract_*`` (inside ``dir``
    or ``TMPDIR``), extracts, yields the same path ``decompress_archive``
    would return and, on exit (success, exception or ``KeyboardInterrupt``),
    removes the whole root, subfolders included. The yielded path is only
    valid inside the ``with`` block; read or copy what you need before
    leaving it.

    Removal never raises: read-only entries are made writable and retried, and
    what still cannot be removed becomes a ``RuntimeWarning`` (a failure here
    never masks the exception raised by the body).

    Example:
        >>> with extracted_archive(raw_zip, target_extensions=(".csv",)) as csv:
        ...     frame = read_csv(csv)

    Args:
        archive_path: Path to the archive to decompress.
        target_extensions: Optional extensions filter, as in
            :func:`decompress_archive`.
        dir: Directory that will hold the temporary root (created if missing).
            ``None`` uses the system temporary directory.

    Yields:
        Path: The chosen extracted file, or the extraction root when no
        unique file can be determined (same resolution as
        :func:`decompress_archive`).

    Raises:
        StorageError: Same conditions as :func:`decompress_archive`.
    """
    archive = _require_archive(archive_path)
    root = _temp_extraction_dir(dir)
    try:
        yield _extract_into(archive, root, target_extensions)
    finally:
        _rmtree_robust(root)


def _is_junk_parts(parts: Iterable[str]) -> bool:
    """Whether a member path (as its components) is archive/OS noise."""
    parts = list(parts)
    if not parts:
        return False
    name = parts[-1].lower()
    return (
        name.startswith("~$")
        or name.startswith("md5_")
        or name in {"thumbs.db", ".ds_store"}
        or any(part.lower() == "__macosx" for part in parts)
    )


def _is_junk_name(name: str) -> bool:
    """Whether an archive member name (``/`` or ``\\`` separated) is noise."""
    return _is_junk_parts(p for p in name.replace("\\", "/").split("/") if p)


def _is_junk_member(path: Path) -> bool:
    """Whether an extracted file is archive/OS noise rather than data.

    Covers Office lock files (``~$*``), checksum files (``md5_*``),
    ``Thumbs.db``, ``.DS_Store`` and anything under ``__MACOSX/``. Archive
    extraction never writes these (see :func:`decompress_archive`); the check
    stays in the result picker as a second line for files already on disk.
    """
    return _is_junk_parts(path.parts)


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
            if candidate.suffix.lower() in extensions and not _is_junk_member(
                candidate
            ):
                return candidate
        raise StorageError(
            f"No extracted file matches target extensions {extensions} in {dest}"
        )
    if len(extracted) == 1:
        return extracted[0]
    common = (".csv", ".json", ".xls", ".xlsx", ".txt", ".xml")
    for candidate in extracted:
        if candidate.suffix.lower() in common and not _is_junk_member(candidate):
            return candidate
    return dest
