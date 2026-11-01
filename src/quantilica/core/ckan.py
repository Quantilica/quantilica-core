"""CKAN API client for public open data portals."""

from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from typing import Any

from .exceptions import CkanError
from .http import HttpClient
from .logging import get_logger

logger = get_logger(__name__)


class CkanClient:
    """Client for querying CKAN v3 API instances."""

    def __init__(
        self,
        base_url: str,
        *,
        client: HttpClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client or HttpClient()
        self.logger = logger or get_logger(__name__)

    def _call(self, action: str, params: dict[str, Any] | None = None) -> Any:
        endpoint = f"{self.base_url}/api/3/action/{action}"
        try:
            data = self.client.get_json(endpoint, params=params)
        except Exception as exc:
            raise CkanError(f"CKAN request failed for {action}: {exc}") from exc

        if not isinstance(data, dict):
            raise CkanError(
                f"CKAN response for {action} is not a JSON object: {data!r}"
            )

        if not data.get("success", False):
            error_info = data.get("error", "Unknown CKAN error")
            raise CkanError(f"CKAN API error on {action}: {error_info}")

        return data.get("result")

    def status(self) -> dict[str, Any]:
        """Call ``status_show`` to check CKAN portal health/version."""
        result = self._call("status_show")
        return result if isinstance(result, dict) else {}

    def package_show(self, package_id: str) -> dict[str, Any]:
        """Fetch details of a single dataset/package by ID or slug."""
        result = self._call("package_show", params={"id": package_id})
        if not isinstance(result, dict):
            raise CkanError(
                f"Expected dict result from package_show, got {type(result)}"
            )
        return result

    def iter_packages(
        self,
        *,
        q: str | None = None,
        fq: str | None = None,
        rows: int = 1000,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over packages matching query via paginated ``package_search``."""
        start = 0
        while True:
            params: dict[str, Any] = {"rows": rows, "start": start}
            if q:
                params["q"] = q
            if fq:
                params["fq"] = fq
            result = self._call("package_search", params=params)
            if not isinstance(result, dict):
                break
            results = result.get("results", [])
            if not results:
                break
            yield from results
            count = result.get("count", len(results))
            start += len(results)
            if start >= count:
                break


def pick_resources(
    package: dict[str, Any],
    *,
    prefer: Sequence[str] = ("PARQUET", "CSV", "ZIP", "JSON"),
    dedupe_by_name: bool = True,
) -> list[dict[str, Any]]:
    """Select the best resources from a package.

    Picks the preferred format according to ``prefer`` precedence. If ``dedupe_by_name``
    is True, groups resources by logical name (or stem) and picks the one matching the
    highest-preference format, breaking ties by largest ``size``.
    """
    resources = package.get("resources", [])
    if not resources:
        return []

    prefer_ranks = {fmt.upper(): rank for rank, fmt in enumerate(prefer)}

    def format_rank(r: dict[str, Any]) -> int:
        fmt = str(r.get("format") or "").strip().upper()
        return prefer_ranks.get(fmt, 999)

    def resource_size(r: dict[str, Any]) -> int:
        raw = r.get("size")
        try:
            return int(raw or 0)
        except (ValueError, TypeError):
            return 0

    if not dedupe_by_name:
        return sorted(resources, key=lambda r: (format_rank(r), -resource_size(r)))

    groups: dict[str, list[dict[str, Any]]] = {}
    for r in resources:
        name = str(r.get("name") or r.get("id") or "").strip()
        groups.setdefault(name, []).append(r)

    chosen: list[dict[str, Any]] = []
    for _name, candidates in groups.items():
        best = min(candidates, key=lambda r: (format_rank(r), -resource_size(r)))
        chosen.append(best)

    return chosen


__all__ = ["CkanClient", "CkanError", "pick_resources"]
