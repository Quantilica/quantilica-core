"""Datetime helpers with explicit UTC behavior."""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(UTC)


def to_utc(value: datetime) -> datetime:
    """Convert a datetime to timezone-aware UTC.

    Naive datetimes are treated as UTC to avoid silently applying a local
    machine timezone during ingestion jobs.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def isoformat_utc(value: datetime | None = None) -> str:
    """Return an ISO 8601 UTC timestamp using a trailing ``Z``."""
    current = utc_now() if value is None else to_utc(value)
    return current.isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime and normalize it to UTC."""
    normalized = value.replace("Z", "+00:00")
    return to_utc(datetime.fromisoformat(normalized))


def expand_year_range(*specs: str | int) -> list[int]:
    """Expand year specs into a list of ints, preserving order.

    Each spec is either a single year (``"2020"``) or an inclusive range
    (``"2020:2025"``). Ranges may be descending (``"2025:2020"``). Raises
    ``ValueError`` on malformed specs.
    """
    years: list[int] = []
    for spec in specs:
        text = str(spec).strip()
        if ":" in text:
            start_str, end_str = text.split(":", 1)
            start, end = int(start_str), int(end_str)
            step = 1 if start <= end else -1
            years.extend(range(start, end + step, step))
        else:
            years.append(int(text))
    return years


def year_month_partition(year: int, month: int | None = None) -> str:
    """Return a partition string: ``"YYYY"`` or ``"YYYYMM"``."""
    if month is None:
        return f"{year:04d}"
    return f"{year:04d}{month:02d}"
