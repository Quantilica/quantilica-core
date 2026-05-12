"""Concurrent download orchestration for data fetcher packages."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any

from tqdm import tqdm

from .http import AsyncHttpClient
from .logging import get_logger
from .storage import StampedDataRepository

_logger = get_logger(__name__)


@dataclass
class RemoteResource:
    """Description of a downloadable remote resource.

    ``filename`` must be pre-computed by the caller (e.g. via ``slugify`` +
    ``stamp_filename``) so the orchestrator can determine the local destination
    and skip-check slug without knowing the source-specific naming rules.
    """

    name: str
    url: str
    filename: str
    size: int = 0
    format: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


async def _download_one(
    resource: RemoteResource,
    repo: StampedDataRepository,
    dataset_id: str,
    client: AsyncHttpClient,
    semaphore: asyncio.Semaphore,
    *,
    source_id: str,
    producer: str,
    ext: str,
    logger: logging.Logger,
) -> dict | None:
    dest = repo.raw_path(dataset_id, resource.filename)

    if resource.size > 0:
        slug = resource.filename.partition("@")[0]
        latest = repo.get_latest_stamped_file(dataset_id, slug, ext)
        if latest is not None and latest.stat().st_size == resource.size:
            logger.debug(
                f"Skipping {resource.filename}: matching local copy {latest.name}"
            )
            return None

    pbar = tqdm(
        total=resource.size or None,
        unit="B",
        unit_scale=True,
        desc=f"Downloading {resource.filename[:30]}...",
        leave=False,
    )
    last_seen = 0

    def _on_progress(downloaded: int, total: int) -> None:
        nonlocal last_seen
        if total and pbar.total != total:
            pbar.total = total
        pbar.update(downloaded - last_seen)
        last_seen = downloaded

    try:
        async with semaphore:
            await client.download_with_manifest(
                resource.url,
                dest,
                source_id=source_id,
                dataset_id=dataset_id,
                producer=producer,
                params=None,
                progress=_on_progress,
            )
    except Exception as exc:
        logger.error(f"Failed to download {resource.url}: {exc}")
        if dest.exists():
            try:
                dest.unlink()
            except OSError:
                pass
        return None
    finally:
        pbar.close()

    return {
        "url": resource.url,
        "filename": resource.filename,
        "destination": dest,
        "file_size": dest.stat().st_size,
    }


async def download_resources(
    resources: list[RemoteResource],
    repo: StampedDataRepository,
    dataset_id: str,
    client: AsyncHttpClient,
    *,
    source_id: str,
    producer: str = "",
    ext: str = "csv",
    max_concurrency: int = 3,
    logger: logging.Logger | None = None,
) -> list[dict]:
    """Download resources concurrently; skip when local file size matches remote.

    Returns a list of dicts with keys ``url``, ``filename``, ``destination``,
    and ``file_size`` for each successfully downloaded resource.
    """
    _log = logger or _logger
    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = [
        _download_one(
            resource,
            repo,
            dataset_id,
            client,
            semaphore,
            source_id=source_id,
            producer=producer,
            ext=ext,
            logger=_log,
        )
        for resource in resources
    ]
    results = await asyncio.gather(*tasks)
    return [r for r in results if r is not None]
