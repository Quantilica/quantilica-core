"""Progress bar utilities for download operations."""

from __future__ import annotations

import contextlib
from collections.abc import Generator

from tqdm import tqdm

from .http import ProgressCallback


@contextlib.contextmanager
def file_progress(
    description: str,
    *,
    total: int = 0,
    leave: bool = False,
) -> Generator[ProgressCallback, None, None]:
    """Context manager yielding a ProgressCallback backed by a tqdm progress bar.

    Designed for use with HttpClient.download_with_manifest(progress=...).
    ``total`` is the expected file size in bytes; pass 0 when unknown.
    """
    pbar: tqdm[int] = tqdm(
        total=total or None,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=description[:40],
        leave=leave,
    )
    last_seen = 0

    def _callback(downloaded: int, total_bytes: int) -> None:
        nonlocal last_seen
        if downloaded == 0 and total_bytes == 0:  # retry reset signal
            pbar.reset(total=None)
            last_seen = 0
            return
        if total_bytes and pbar.total != total_bytes:
            pbar.total = total_bytes
            pbar.refresh()
        pbar.update(downloaded - last_seen)
        last_seen = downloaded

    try:
        yield _callback
    finally:
        pbar.close()


@contextlib.contextmanager
def batch_progress(
    description: str,
    *,
    total: int,
) -> Generator[tqdm[int], None, None]:
    """Context manager for tracking overall batch progress (file count)."""
    with tqdm(total=total, desc=description, unit="arquivo", leave=True) as pbar:
        yield pbar
