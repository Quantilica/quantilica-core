"""FTP helpers for data clients."""

from __future__ import annotations

import contextlib
import ftplib
import logging
import os
import socket as _socket
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .exceptions import FetchError
from .files import ensure_parent, write_stream_atomic
from .logging import get_logger, log_step
from .manifests import DownloadManifest
from .retry import exponential_delay, retry_call

_logger = get_logger(__name__)

# Errors that warrant a retry (excludes error_perm which is a permanent 5xx).
FTP_TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ftplib.error_temp,
    ftplib.error_proto,
    EOFError,
    OSError,  # superclass of ConnectionResetError, BrokenPipeError, TimeoutError
)


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
    """Open an FTP connection with exponential backoff retry."""
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
        """Fecha o data socket para interromper um ``retrbinary()`` em andamento."""
        conn = self._data_conn
        if conn is not None:
            with contextlib.suppress(OSError):
                conn.shutdown(_socket.SHUT_RDWR)

    def retrbinary(
        self, cmd: str, callback, blocksize: int = 8192, rest=None
    ) -> str:
        """Como ``FTP.retrbinary``, mas com watchdog de idle-timeout."""
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

    def download_with_manifest(
        self,
        remote_path: str,
        target_path: str | Path,
        *,
        source_id: str,
        dataset_id: str,
        producer: str,
        force: bool = False,
        metadata: dict[str, Any] | None = None,
        progress_callback: Callable[[int], None] | None = None,
    ) -> Path:
        """Download a file from FTP; freshness check, streaming write, and manifest."""
        target = Path(target_path)
        ensure_parent(target)
        with log_step(
            self.logger, "ftp-download-with-manifest", host=self.host, path=remote_path
        ):
            # Freshness probe — any failure falls through to download.
            if not force and target.exists():
                with contextlib.suppress(Exception):
                    with self._open() as ftp:
                        mtime_str = ftp.sendcmd(f"MDTM {remote_path}").split()[1]
                        remote_mtime = time.mktime(
                            time.strptime(mtime_str, "%Y%m%d%H%M%S")
                        )
                        if target.stat().st_mtime >= (remote_mtime - 1):
                            self.logger.debug("File is up to date: %s", target.name)
                            return target

            outcome: dict[str, Any] = {}

            def _attempt() -> None:
                with self._open() as ftp:
                    try:
                        def _stream(cb: Callable[[bytes], None]) -> None:
                            def _tracked(data: bytes) -> None:
                                cb(data)
                                if progress_callback is not None:
                                    progress_callback(len(data))

                            ftp.retrbinary(f"RETR {remote_path}", _tracked)

                        sha256, size_bytes = write_stream_atomic(target, _stream)
                    except ftplib.error_perm as exc:
                        raise FetchError(f"FTP file not found: {remote_path}") from exc
                    outcome.update(sha256=sha256, size_bytes=size_bytes)
                    with contextlib.suppress(Exception):
                        outcome["mtime_str"] = ftp.sendcmd(
                            f"MDTM {remote_path}"
                        ).split()[1]

            self._retry(_attempt)

            if mtime_str := outcome.get("mtime_str"):
                with contextlib.suppress(Exception):
                    remote_mtime = time.mktime(time.strptime(mtime_str, "%Y%m%d%H%M%S"))
                    os.utime(target, (time.time(), remote_mtime))

            manifest = DownloadManifest.from_digest(
                source_id=source_id,
                dataset_id=dataset_id,
                url=f"ftp://{self.host}/{remote_path}",
                sha256=outcome["sha256"],
                size_bytes=outcome["size_bytes"],
                path=str(target.absolute()),
                producer=producer,
                metadata=metadata or {},
            )
            manifest.write_json(target.with_suffix(target.suffix + ".manifest.json"))
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
