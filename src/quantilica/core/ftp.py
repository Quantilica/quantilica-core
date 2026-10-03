"""FTP helpers for data clients."""

from __future__ import annotations

import contextlib
import datetime as dt
import ftplib
import logging
import os
import re
import socket as _socket
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # evita ciclo pesado (http importa httpx2)
    from .http import ProgressCallback

from .exceptions import FetchError
from .files import ensure_parent, write_stream_atomic
from .logging import get_logger, log_step
from .manifests import DownloadManifest, write_manifest_sidecar
from .retry import exponential_delay, retry_call

_logger = get_logger(__name__)

# Errors that warrant a retry (excludes error_perm which is a permanent 5xx).
FTP_TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ftplib.error_temp,
    ftplib.error_proto,
    EOFError,
    OSError,  # superclass of ConnectionResetError, BrokenPipeError, TimeoutError
)

# IIS/Windows DOS-style LIST line, e.g.:
#   "04-21-26  09:00AM       123456789 arquivo.csv"
#   "10-02-02  03:31PM       <DIR>          docs"
_IIS_LINE_RE = re.compile(
    r"^(?P<date>\d{2}-\d{2}-\d{2})\s+"
    r"(?P<time>\d{1,2}:\d{2})(?P<ampm>AM|PM)\s+"
    r"(?P<size><DIR>|\d+)\s+"
    r"(?P<name>.*)$",
    re.IGNORECASE,
)

# UNIX ls -l style LIST line, e.g.:
#   "-rw-r--r-- 1 ftp ftp 123456789 Apr 21 09:00 arquivo.csv"
_UNIX_LINE_RE = re.compile(
    r"^(?P<perm>[-dlbcps][rwxstT-]{9})\s+\d+\s+\S+\s+\S+\s+"
    r"(?P<size>\d+)\s+(?P<month>[A-Z][a-z]{2})\s+"
    r"(?P<day>\d{1,2})\s+(?P<year_or_time>\d{4}|\d{1,2}:\d{2})\s+"
    r"(?P<name>.*)$",
)

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def parse_ftp_list_line(line: str) -> tuple[str, int, dt.datetime] | None:
    """Parse one raw FTP ``LIST`` line into (filename, size, modified).

    Handles both legacy IIS/Windows DOS-style listings (``MM-DD-YY  HH:MMAM
    SIZE NAME``, where size is ``<DIR>`` for directories) and UNIX ``ls -l``
    style listings (permission string, size, month, day, year-or-time, name).

    Args:
        line: Raw line as returned by ``ftplib`` LIST/retrlines.

    Returns:
        tuple[str, int, dt.datetime] | None: ``(filename, size_bytes,
        modified_dt)`` for regular files, or ``None`` for directories,
        device/link entries and empty or unrecognizable lines.
    """
    stripped = line.strip()
    if not stripped:
        return None

    if match := _IIS_LINE_RE.match(stripped):
        raw_dt = dt.datetime.strptime(
            f"{match['date']} {match['time']}{match['ampm'].upper()}",
            "%m-%d-%y %I:%M%p",
        )
        if match["size"].upper() == "<DIR>":
            return None
        name = match["name"].strip()
        return (name, int(match["size"]), raw_dt) if name else None

    if match := _UNIX_LINE_RE.match(stripped):
        permissions = match["perm"]
        if permissions[0] != "-":
            return None  # d/l/c/b/p/s: directory, link, device, fifo...
        month = _MONTHS.get(match["month"].lower())
        day = int(match["day"])
        year_slot = match["year_or_time"]
        if ":" in year_slot:
            hour, minute = (int(value) for value in year_slot.split(":", 1))
            now = dt.datetime.now()
            raw_dt = now.replace(month=month, day=day, hour=hour, minute=minute)
            # ls shows the year only for files older than ~6 months; recent
            # files carry a time-of-day instead. A time-of-day that lands in
            # the future therefore belongs to last year.
            if raw_dt > now:
                raw_dt = raw_dt.replace(year=now.year - 1)
        else:
            raw_dt = dt.datetime(int(year_slot), month, day)
        name = match["name"].strip()
        return (name, int(match["size"]), raw_dt) if name else None

    return None


def ftp_connect(
    host: str,
    *,
    user: str = "anonymous",
    passwd: str = "",
    encoding: str = "latin-1",
    timeout: float = 60.0,
    attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0,
    jitter: float = 1.0,
) -> ftplib.FTP:
    """Open an FTP connection with exponential backoff retry.

    Args:
        host: The FTP host.
        user: The FTP user.
        passwd: The FTP password.
        encoding: The encoding to use.
        timeout: The timeout in seconds.
        attempts: Number of attempts.
        base_delay: Base delay for retry.
        max_delay: Maximum delay for retry.
        jitter: Jitter for retry.

    Returns:
        ftplib.FTP: The connected FTP object.

    Raises:
        FetchError: If the connection fails after all attempts.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            ftp = ftplib.FTP(host, timeout=timeout, encoding=encoding)
            ftp.login(user, passwd)
            return ftp
        except FTP_TRANSIENT_ERRORS as exc:
            last_exc = exc
            if attempt == attempts:
                break
            time.sleep(
                exponential_delay(
                    attempt, base_delay=base_delay, max_delay=max_delay, jitter=jitter
                )
            )
    raise FetchError(
        f"Could not connect to {host} after {attempts} attempts"
    ) from last_exc


class MonitoredFTP(ftplib.FTP):
    """ftplib.FTP com idle-timeout e interrupção de transferência de dados.

    Rastreia o data socket durante ``retrbinary()`` para permitir:

    - Interrupção imediata via :meth:`interrupt_transfer` (útil em workers com
      mecanismo de kill).
    - Watchdog thread que aborta transferências travadas após ``idle_timeout``
      segundos sem bytes recebidos.
    - TCP keepalive ativado automaticamente no socket de controle.
    """

    def __init__(self, *args, idle_timeout: float = 90.0, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.idle_timeout = idle_timeout
        self._data_conn: _socket.socket | None = None

    def connect(self, host="", port=0, timeout=None, source_address=None):
        result = super().connect(host, port, timeout, source_address)
        with contextlib.suppress(OSError):
            self.sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_KEEPALIVE, 1)
        return result

    def interrupt_transfer(self) -> None:
        """Fecha o data socket para interromper um ``retrbinary()`` em andamento.

        Returns:
            None
        """
        conn = self._data_conn
        if conn is not None:
            with contextlib.suppress(OSError):
                conn.shutdown(_socket.SHUT_RDWR)

    def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None) -> str:
        """Como ``FTP.retrbinary``, mas com watchdog de idle-timeout.

        Args:
            cmd: O comando FTP.
            callback: Função para chamar com os dados recebidos.
            blocksize: Tamanho do bloco.
            rest: Onde reiniciar.

        Returns:
            str: A resposta FTP.
        """
        last_chunk = [time.monotonic()]
        stop_event = threading.Event()

        def _watchdog() -> None:
            while not stop_event.wait(timeout=5.0):
                if time.monotonic() - last_chunk[0] > self.idle_timeout:
                    _logger.warning(
                        "FTP transfer stalled (%.0f s sem dados), interrompendo.",
                        self.idle_timeout,
                    )
                    self.interrupt_transfer()
                    return

        self.voidcmd("TYPE I")
        with self.transfercmd(cmd, rest) as conn:
            self._data_conn = conn
            wdog = threading.Thread(target=_watchdog, daemon=True)
            wdog.start()
            try:
                while True:
                    data = conn.recv(blocksize)
                    if not data:
                        break
                    last_chunk[0] = time.monotonic()
                    callback(data)
            finally:
                self._data_conn = None
                stop_event.set()
                wdog.join(timeout=2.0)
        return self.voidresp()


class FtpClient:
    """Small FTP client wrapper around ``ftplib``."""

    def __init__(
        self,
        host: str,
        *,
        user: str = "anonymous",
        passwd: str = "",
        encoding: str = "latin-1",
        timeout: float = 60.0,
        attempts: int = 3,
        retry_base_delay: float = 2.0,
        retry_max_delay: float = 60.0,
        retry_jitter: float = 1.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self.host = host
        self.user = user
        self.passwd = passwd
        self.encoding = encoding
        self.timeout = timeout
        self.attempts = attempts
        self.retry_base_delay = retry_base_delay
        self.retry_max_delay = retry_max_delay
        self.retry_jitter = retry_jitter
        self.logger = logger or get_logger(__name__)

    def _retry(self, func: Any) -> Any:
        return retry_call(
            func,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            max_delay=self.retry_max_delay,
            jitter=self.retry_jitter,
            retry_exceptions=FTP_TRANSIENT_ERRORS,
        )

    def _open(self) -> ftplib.FTP:
        return ftp_connect(
            self.host,
            user=self.user,
            passwd=self.passwd,
            encoding=self.encoding,
            timeout=self.timeout,
            attempts=self.attempts,
            base_delay=self.retry_base_delay,
            max_delay=self.retry_max_delay,
            jitter=self.retry_jitter,
        )

    @contextlib.contextmanager
    def _connected(self) -> Iterator[ftplib.FTP]:
        """Abre conexão FTP cujo `quit()` de saída nunca mascara o resultado.

        Exceções do corpo do `with` propagam intactas; só falhas do `quit()`
        final são suprimidas (servidores como o IIS derrubam a conexão de
        controle após o RETR com `550 network name no longer available` —
        o download já íntegro não pode virar `FetchError` por isso).
        """
        ftp = self._open()
        try:
            yield ftp
        except BaseException:
            with contextlib.suppress(Exception):
                ftp.close()
            raise
        else:
            try:
                ftp.quit()
            except Exception:
                with contextlib.suppress(Exception):
                    ftp.close()

    def download_with_manifest(
        self,
        url: str,
        target_path: str | Path,
        *,
        source_id: str,
        dataset_id: str,
        producer: str,
        force: bool = False,
        metadata: dict[str, Any] | None = None,
        progress: ProgressCallback | None = None,
    ) -> Path:
        """Download a file from FTP; freshness check, streaming write, and manifest.

        Args:
            url: The remote file URL/path.
            target_path: The local path to download to.
            source_id: The source identifier.
            dataset_id: The dataset identifier.
            producer: The producer name.
        force: Whether to force download even if fresh.
        metadata: Additional metadata.
        progress: Progress callback function `(downloaded, total)` — mesmo
            contrato de `ProgressCallback` do HTTP (`total=0` se desconhecido).

        Returns:
            Path: The path to the downloaded file.
        """
        target = Path(target_path)
        ensure_parent(target)
        with log_step(
            self.logger, "ftp-download-with-manifest", host=self.host, path=url
        ):
            # Freshness probe — any failure falls through to download.
            if not force and target.exists():
                with contextlib.suppress(Exception):
                    with self._connected() as ftp:
                        mtime_str = ftp.sendcmd(f"MDTM {url}").split()[1]
                        remote_mtime = time.mktime(
                            time.strptime(mtime_str, "%Y%m%d%H%M%S")
                        )
                        if target.stat().st_mtime >= (remote_mtime - 1):
                            self.logger.debug("File is up to date: %s", target.name)
                            return target

            outcome: dict[str, Any] = {}

            def _attempt() -> None:
                with self._connected() as ftp:
                    try:
                        # Total best-effort (SIZE pode não existir no servidor).
                        total = 0
                        with contextlib.suppress(Exception):
                            total = int(ftp.size(url) or 0)
                        downloaded = 0

                        def _stream(cb: Callable[[bytes], None]) -> None:
                            def _tracked(data: bytes) -> None:
                                nonlocal downloaded
                                cb(data)
                                if progress is not None:
                                    downloaded += len(data)
                                    progress(downloaded, total)

                            ftp.retrbinary(f"RETR {url}", _tracked)

                        sha256, size_bytes = write_stream_atomic(target, _stream)
                    except ftplib.error_perm as exc:
                        raise FetchError(f"FTP file not found: {url}") from exc
                    outcome.update(sha256=sha256, size_bytes=size_bytes)
                    with contextlib.suppress(Exception):
                        outcome["mtime_str"] = ftp.sendcmd(f"MDTM {url}").split()[1]

            self._retry(_attempt)

            if mtime_str := outcome.get("mtime_str"):
                with contextlib.suppress(Exception):
                    remote_mtime = time.mktime(time.strptime(mtime_str, "%Y%m%d%H%M%S"))
                    os.utime(target, (time.time(), remote_mtime))

            manifest = DownloadManifest.from_digest(
                source_id=source_id,
                dataset_id=dataset_id,
                url=f"ftp://{self.host}/{url}" if not url.startswith("ftp://") else url,
                sha256=outcome["sha256"],
                size_bytes=outcome["size_bytes"],
                path=str(target.absolute()),
                producer=producer,
                metadata=metadata or {},
            )
            write_manifest_sidecar(target, manifest)
            return target

    def list_files(
        self,
        directory: str,
        parse_line: Callable[[str], dict[str, Any] | None] | None = None,
    ) -> list[dict[str, Any]]:
        """List files in a directory with basic metadata.

        ``parse_line`` is called for each raw LIST line and should return a
        dict or None (to skip the line). Defaults to a generic POSIX/Windows
        parser when not provided.

        Args:
            directory: The directory to list.
            parse_line: Custom parser for LIST lines.

        Returns:
            list[dict[str, Any]]: A list of dictionaries with file metadata.
        """
        if parse_line is None:

            def parse_line(line: str) -> dict[str, Any] | None:
                parts = line.split()
                if len(parts) < 4:
                    return None
                is_dir = line.startswith("d") or "<DIR>" in line
                name = parts[-1]
                return {
                    "name": name,
                    "is_dir": is_dir,
                    "full_path": f"{directory}/{name}".replace("//", "/"),
                    "raw": line,
                }

        def _attempt() -> list[dict[str, Any]]:
            with self._open() as ftp:
                ftp.cwd(directory)
                lines: list[str] = []
                ftp.retrlines("LIST", lines.append)
                return [r for line in lines if (r := parse_line(line)) is not None]

        return self._retry(_attempt)
