"""FTP helpers for data clients."""

from __future__ import annotations

import ftplib
import logging
import os
import time
from pathlib import Path
from typing import Any

from .files import write_bytes_atomic
from .logging import get_logger, log_step
from .manifests import DownloadManifest


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
        logger: logging.Logger | None = None,
    ) -> None:
        self.host = host
        self.user = user
        self.passwd = passwd
        self.encoding = encoding
        self.timeout = timeout
        self.logger = logger or get_logger(__name__)

    def download_with_manifest(
        self,
        remote_path: str,
        target_path: str | Path,
        *,
        source_id: str,
        dataset_id: str,
        producer: str,
        force: bool = False,
    ) -> Path:
        """Download a file from FTP with freshness check and manifest."""
        target = Path(target_path)
        step_msg = "ftp-download-with-manifest"
        with log_step(self.logger, step_msg, host=self.host, path=remote_path):
            with ftplib.FTP(
                self.host, timeout=self.timeout, encoding=self.encoding
            ) as ftp:
                ftp.login(self.user, self.passwd)

                # Check freshness if possible
                if not force and target.exists():
                    try:
                        # MDTM is not supported by all servers, but let's try
                        mtime_str = ftp.sendcmd(f"MDTM {remote_path}").split()[1]
                        fmt = "%Y%m%d%H%M%S"
                        remote_mtime = time.mktime(time.strptime(mtime_str, fmt))
                        if target.stat().st_mtime >= (remote_mtime - 1):
                            self.logger.debug(f"File is up to date: {target.name}")
                            return target
                    except (ftplib.error_perm, ValueError, IndexError, OSError):
                        pass

                # Download content to memory first for atomic write and manifest
                # (For very large files, this might need to be optimized)
                content_chunks = []
                ftp.retrbinary(f"RETR {remote_path}", content_chunks.append)
                content = b"".join(content_chunks)

                write_bytes_atomic(target, content)

                # Try to sync mtime if we got it from MDTM earlier
                try:
                    mtime_str = ftp.sendcmd(f"MDTM {remote_path}").split()[1]
                    fmt = "%Y%m%d%H%M%S"
                    remote_mtime = time.mktime(time.strptime(mtime_str, fmt))
                    os.utime(target, (time.time(), remote_mtime))
                except (ftplib.error_perm, ValueError, IndexError, OSError):
                    pass

                # Generate manifest
                manifest = DownloadManifest.from_content(
                    source_id=source_id,
                    dataset_id=dataset_id,
                    url=f"ftp://{self.host}/{remote_path}",
                    content=content,
                    path=str(target.absolute()),
                    producer=producer,
                )
                manifest_path = target.with_suffix(
                    target.suffix + ".manifest.json"
                )
                manifest.write_json(manifest_path)
                return target

    def list_files(self, directory: str) -> list[dict[str, Any]]:
        """List files in a directory with basic metadata."""
        with ftplib.FTP(
            self.host, timeout=self.timeout, encoding=self.encoding
        ) as ftp:
            ftp.login(self.user, self.passwd)
            ftp.cwd(directory)
            lines = []
            ftp.retrlines("LIST", lines.append)

            # Simple parser for common FTP LIST formats
            # This is a bit fragile and might need more robust parsing
            files = []
            for line in lines:
                parts = line.split()
                if len(parts) >= 4:
                    # Very basic heuristic for directory vs file
                    is_dir = line.startswith("d") or "<DIR>" in line
                    name = parts[-1]
                    files.append({
                        "name": name,
                        "is_dir": is_dir,
                        "full_path": f"{directory}/{name}".replace("//", "/"),
                        "raw": line
                    })
            return files
