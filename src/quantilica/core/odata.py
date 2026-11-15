"""OData pagination by key range (``$filter`` on the key), never by ``$skip``.

``$skip`` is O(offset) on the server and silently drops or repeats rows when
the table changes between pages. Paginating by key range asks, page after
page, for ``key > last_seen`` ordered by the key with ``$top`` rows; it is
stable under inserts and costs the same on every page. Olinda (BCB Focus,
juros) honors ``$filter``; sources that ignore it (Ipeadata ``ValoresSerie``)
are detected on the second page and reported as
:class:`ODataPaginationError` instead of looping forever.

The helper is transport-agnostic: it takes a ``fetch_page(params)`` callable
(see :func:`http_odata_fetcher` for the :class:`HttpClient` one), so it is
tested with a fake transport and no network.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Iterator, Mapping, Sequence
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .exceptions import FetchError

if TYPE_CHECKING:
    from .http import HttpClient

__all__ = [
    "ODataPaginationError",
    "http_odata_fetcher",
    "iter_odata_key_pages",
    "iter_odata_rows",
    "odata_literal",
]

PageFetcher = Callable[[Mapping[str, str]], Sequence[Mapping[str, Any]]]
"""``fetch_page(params) -> rows``: one OData request, rows of ``value``."""


class ODataPaginationError(FetchError):
    """The server did not honor the key filter (or the key is not unique)."""


def odata_literal(value: Any) -> str:
    """Format ``value`` as an OData literal for a ``$filter`` expression.

    Numbers and booleans are bare, strings are single-quoted with ``'``
    doubled, dates and datetimes are single-quoted ISO 8601 (the Olinda
    dialect).
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return str(value)
    if isinstance(value, dt.datetime | dt.date):
        return f"'{value.isoformat()}'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


_UNSET: Any = object()
""""No last key yet" marker; ``None`` is a value a (bad) server can send."""


def _and(base_filter: str | None, parts: Sequence[str]) -> str:
    """AND the user's ``base_filter`` (always parenthesized) with key ranges."""
    wrapped = [f"({base_filter})"] if base_filter else []
    return " and ".join([*wrapped, *(p for p in parts if p)])


def iter_odata_key_pages(
    fetch_page: PageFetcher,
    *,
    key: str,
    page_size: int = 1000,
    start: Any = None,
    stop: Any = None,
    base_filter: str | None = None,
    select: Sequence[str] | None = None,
    extra_params: Mapping[str, str] | None = None,
    unique_key: bool = True,
    tiebreak: Sequence[str] = (),
    to_literal: Callable[[Any], str] = odata_literal,
    max_pages: int | None = None,
    trust_short_page: bool = False,
) -> Iterator[list[Mapping[str, Any]]]:
    """Yield pages of rows ordered by ``key``, paginating by key range.

    Args:
        fetch_page: Performs one request with the given OData params.
        key: Name of the ordering key column.
        page_size: ``$top`` of each request.
        start: First key, **inclusive** (``key ge start``); ``None`` = from
            the beginning.
        stop: Last key, **inclusive** (``key le stop``); ``None`` = to the end.
        base_filter: Extra ``$filter`` expression ANDed with the key range.
        select: Columns for ``$select`` (the key is always requested).
        extra_params: Other query options passed through (e.g. ``$format``).
        unique_key: ``True`` when ``key`` identifies one row (continues with
            ``key gt last``). ``False`` for repeated keys (continues with
            ``key ge last`` and skips the rows already delivered at ``last``);
            then the server order inside a key must be stable, so name
            ``tiebreak`` columns that make it total.
        tiebreak: Extra ``$orderby`` columns after ``key``.
        to_literal: Formats a key as an OData literal.
        max_pages: Stop after this many pages (``None`` = until exhausted).
        trust_short_page: Stop at a page shorter than ``page_size`` without
            the confirming empty request. Off by default: servers may cap
            ``$top`` below the request and a short page is not proof of end.

    Yields:
        list[Mapping[str, Any]]: Non-empty pages, in key order, no row twice.

    Raises:
        ODataPaginationError: The key did not advance (the server ignores
            ``$filter``), the key order is violated, a unique key repeats, or
            more rows share one key than fit in a page when ``unique_key`` is
            ``False``; a row with a null key.

    Note:
        Keys are compared in Python (to detect a server that ignores the
        filter) while the server orders by its own collation. For string keys
        the two orders may differ (case, accents, locale), which can raise a
        spurious :class:`ODataPaginationError` or reorder rows; prefer numeric
        or ISO date keys. This is a known limitation, not handled here.
    """
    if page_size < 1:
        raise ValueError("page_size must be >= 1")
    last: Any = _UNSET
    seen_at_last = 0
    pages = 0
    order_by = ", ".join([key, *tiebreak])
    while max_pages is None or pages < max_pages:
        parts: list[str] = []
        if last is not _UNSET:
            op = "gt" if unique_key else "ge"
            parts.append(f"{key} {op} {to_literal(last)}")
        elif start is not None:
            parts.append(f"{key} ge {to_literal(start)}")
        if stop is not None:
            parts.append(f"{key} le {to_literal(stop)}")
        params: dict[str, str] = dict(extra_params or {})
        filter_expr = _and(base_filter, parts)
        if filter_expr:
            params["$filter"] = filter_expr
        params["$orderby"] = order_by
        params["$top"] = str(page_size)
        if select:
            params["$select"] = ", ".join(dict.fromkeys([key, *select]))

        raw = list(fetch_page(params))
        rows = raw
        if not unique_key and last is not _UNSET:
            skip = 0
            while (
                skip < min(seen_at_last, len(rows))
                and rows[skip].get(key, _UNSET) == last
            ):
                skip += 1
            rows = rows[skip:]
        if not rows:
            if len(raw) >= page_size and not unique_key:
                raise ODataPaginationError(
                    f"{len(raw)} rows share {key}={last!r}: more than one page; "
                    "raise page_size or add tiebreak columns"
                )
            return
        _check_order(rows, key, last, unique_key)
        pages += 1
        yield rows

        new_last = rows[-1][key]
        trailing = 0
        for row in reversed(rows):
            if row[key] != new_last:
                break
            trailing += 1
        seen_at_last = seen_at_last + len(rows) if new_last == last else trailing
        last = new_last
        if trust_short_page and len(raw) < page_size:
            return


def _check_order(
    rows: Sequence[Mapping[str, Any]], key: str, last: Any, unique_key: bool
) -> None:
    previous = last
    for row in rows:
        if key not in row:
            raise ODataPaginationError(f"row without the key column {key!r}")
        current = row[key]
        if current is None:
            raise ODataPaginationError(
                f"null key {key!r} from the server: cannot paginate by key range"
            )
        if previous is not _UNSET:
            advanced = current > previous if unique_key else current >= previous
            if not advanced:
                if previous == current and unique_key:
                    raise ODataPaginationError(
                        f"key {key}={current!r} repeats; use unique_key=False"
                    )
                raise ODataPaginationError(
                    f"{key} did not advance ({current!r} after {previous!r}): "
                    "the server ignored $filter/$orderby"
                )
        previous = current


def iter_odata_rows(
    fetch_page: PageFetcher, **kwargs: Any
) -> Iterator[Mapping[str, Any]]:
    """Flatten :func:`iter_odata_key_pages` into rows (same arguments)."""
    for page in iter_odata_key_pages(fetch_page, **kwargs):
        yield from page


def http_odata_fetcher(
    client: HttpClient,
    url: str,
    *,
    value_key: str = "value",
) -> PageFetcher:
    """Build a ``fetch_page`` that GETs ``url`` with a :class:`HttpClient`.

    Args:
        client: The HTTP client (its host policy paces and backs off).
        url: The OData entity-set (or function) URL.
        value_key: Envelope property holding the rows (``value`` in OData).

    Returns:
        PageFetcher: Callable accepted by :func:`iter_odata_key_pages`. It
        raises :class:`ODataPaginationError` when a ``200`` carries no list
        under ``value_key`` (an empty list is a valid, empty page) instead of
        returning ``[]`` and silently truncating the extraction.
    """

    def fetch(params: Mapping[str, str]) -> Sequence[Mapping[str, Any]]:
        payload = client.get_json(url, params=dict(params))
        if isinstance(payload, Mapping):
            rows = payload.get(value_key)
            if not isinstance(rows, Sequence) or isinstance(rows, str | bytes):
                raise ODataPaginationError(
                    f"response of {url} has no list under {value_key!r} "
                    f"(keys: {sorted(map(str, payload))[:8]}); refusing to treat "
                    "it as an empty page"
                )
            return rows
        if isinstance(payload, Sequence) and not isinstance(payload, str | bytes):
            return payload
        raise ODataPaginationError(
            f"response of {url} is neither an envelope nor a list: "
            f"{type(payload).__name__}"
        )

    return fetch
