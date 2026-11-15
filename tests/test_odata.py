"""OData pagination by key range, against a fake server (no network)."""

import datetime as dt
import re
from decimal import Decimal

import httpx2
import pytest

from quantilica.core.http import HttpClient
from quantilica.core.odata import (
    ODataPaginationError,
    http_odata_fetcher,
    iter_odata_key_pages,
    iter_odata_rows,
    odata_literal,
)

_CMP = re.compile(r"(\w+) (gt|ge|le) ('(?:[^']|'')*'|[-\w.:]+)")


def _value(literal: str):
    if literal.startswith("'"):
        return literal[1:-1].replace("''", "'")
    try:
        return int(literal)
    except ValueError:
        return literal


class FakeOData:
    """Honors ``$filter`` (gt/ge/le on one column), ``$orderby`` and ``$top``."""

    def __init__(self, rows, *, cap: int | None = None) -> None:
        self.rows = rows
        self.cap = cap
        self.calls: list[dict[str, str]] = []

    def __call__(self, params):
        self.calls.append(dict(params))
        rows = list(self.rows)
        for col, op, lit in _CMP.findall(params.get("$filter", "")):
            ref = _value(lit)
            rows = [
                r
                for r in rows
                if {"gt": r[col] > ref, "ge": r[col] >= ref, "le": r[col] <= ref}[op]
            ]
        order = [c.strip() for c in params["$orderby"].split(",")]
        rows.sort(key=lambda r: tuple(r[c] for c in order))
        top = int(params["$top"])
        if self.cap:
            top = min(top, self.cap)
        return rows[:top]


def test_literals():
    assert odata_literal(5) == "5"
    assert odata_literal(1.5) == "1.5"
    assert odata_literal(Decimal("2.50")) == "2.50"
    assert odata_literal(True) == "true"
    assert odata_literal("O'Neil") == "'O''Neil'"
    assert odata_literal(dt.date(2026, 11, 15)) == "'2026-11-15'"
    assert odata_literal(dt.datetime(2026, 11, 15, 3, 0)) == "'2026-11-15T03:00:00'"


def test_unique_key_pages_cover_everything_once():
    fake = FakeOData([{"id": i, "v": i * 2} for i in range(1, 11)])
    pages = list(iter_odata_key_pages(fake, key="id", page_size=4))
    assert [[r["id"] for r in p] for p in pages] == [
        [1, 2, 3, 4],
        [5, 6, 7, 8],
        [9, 10],
    ]
    # 3 data pages + the confirming empty request; never $skip.
    assert len(fake.calls) == 4
    assert all("$skip" not in c for c in fake.calls)
    assert "$filter" not in fake.calls[0]
    assert fake.calls[1]["$filter"] == "id gt 4"
    assert fake.calls[0]["$orderby"] == "id"
    assert fake.calls[0]["$top"] == "4"


def test_start_stop_and_base_filter_and_select():
    fake = FakeOData([{"id": i} for i in range(1, 21)])
    rows = list(
        iter_odata_rows(
            fake,
            key="id",
            page_size=5,
            start=6,
            stop=12,
            base_filter="Indicador eq 'IPCA'",
            select=["valor"],
        )
    )
    assert [r["id"] for r in rows] == [6, 7, 8, 9, 10, 11, 12]
    assert fake.calls[0]["$filter"] == "Indicador eq 'IPCA' and id ge 6 and id le 12"
    assert fake.calls[0]["$select"] == "id, valor"
    assert fake.calls[1]["$filter"].startswith("Indicador eq 'IPCA' and id gt 10")


def test_string_date_keys_olinda_style():
    rows = [{"Data": f"2026-01-{d:02d}"} for d in range(1, 8)]
    fake = FakeOData(rows)
    out = list(iter_odata_rows(fake, key="Data", page_size=3, start="2026-01-02"))
    assert [r["Data"] for r in out][:2] == ["2026-01-02", "2026-01-03"]
    assert len(out) == 6
    assert fake.calls[1]["$filter"] == "Data gt '2026-01-04'"


def test_repeated_keys_do_not_lose_or_duplicate_rows_at_page_boundary():
    # Focus-like: many rows per date; the page boundary falls inside a date.
    rows = [{"Data": d, "n": n} for d in (1, 2, 3) for n in range(3)]
    fake = FakeOData(rows)
    out = list(
        iter_odata_rows(fake, key="Data", page_size=4, unique_key=False, tiebreak=["n"])
    )
    assert [(r["Data"], r["n"]) for r in out] == [(r["Data"], r["n"]) for r in rows]
    assert fake.calls[1]["$filter"] == "Data ge 2"
    assert fake.calls[0]["$orderby"] == "Data, n"


def test_repeated_keys_bigger_than_a_page_is_an_error():
    rows = [{"Data": 1, "n": n} for n in range(6)]
    fake = FakeOData(rows)
    with pytest.raises(ODataPaginationError, match="share Data=1"):
        list(
            iter_odata_rows(
                fake, key="Data", page_size=3, unique_key=False, tiebreak=["n"]
            )
        )


def test_unique_key_that_repeats_is_reported():
    fake = FakeOData([{"id": 1}, {"id": 1}, {"id": 2}])
    with pytest.raises(ODataPaginationError, match="repeats"):
        list(iter_odata_key_pages(fake, key="id", page_size=5))


def test_server_that_ignores_filter_does_not_loop_forever():
    # Ipeadata-like: always the whole series, whatever $filter/$top say.
    series = [{"id": i} for i in range(1, 6)]
    calls = []

    def ignoring(params):
        calls.append(params)
        return list(series)

    with pytest.raises(ODataPaginationError, match="ignored"):
        list(iter_odata_key_pages(ignoring, key="id", page_size=3))
    assert len(calls) == 2


def test_server_cap_below_page_size_is_not_mistaken_for_the_end():
    fake = FakeOData([{"id": i} for i in range(1, 8)], cap=2)
    out = list(iter_odata_rows(fake, key="id", page_size=100))
    assert [r["id"] for r in out] == list(range(1, 8))


def test_trust_short_page_saves_the_confirming_request():
    fake = FakeOData([{"id": i} for i in range(1, 6)])
    list(iter_odata_rows(fake, key="id", page_size=4, trust_short_page=True))
    assert len(fake.calls) == 2


def test_max_pages_and_validation():
    fake = FakeOData([{"id": i} for i in range(1, 100)])
    pages = list(iter_odata_key_pages(fake, key="id", page_size=10, max_pages=2))
    assert len(pages) == 2
    with pytest.raises(ValueError):
        list(iter_odata_key_pages(fake, key="id", page_size=0))


def test_empty_source_yields_nothing():
    assert list(iter_odata_key_pages(FakeOData([]), key="id")) == []


def test_http_fetcher_with_fake_transport():
    data = [{"id": i} for i in range(1, 8)]
    seen = []

    def handler(request: httpx2.Request) -> httpx2.Response:
        seen.append(dict(request.url.params))
        fake = FakeOData(data)
        return httpx2.Response(200, json={"value": fake(dict(request.url.params))})

    client = HttpClient(transport=httpx2.MockTransport(handler))
    fetch = http_odata_fetcher(client, "https://olinda.example/odata/Entidade")
    out = list(iter_odata_rows(fetch, key="id", page_size=3))
    assert [r["id"] for r in out] == list(range(1, 8))
    assert seen[1]["$filter"] == "id gt 3"
