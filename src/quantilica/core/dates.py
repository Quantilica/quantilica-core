"""Datetime helpers with explicit UTC behavior."""

from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime.

    Returns:
        datetime: The current time in UTC.
    """
    return datetime.now(UTC)


def to_utc(value: datetime) -> datetime:
    """Convert a datetime to timezone-aware UTC.

    Naive datetimes are treated as UTC to avoid silently applying a local
    machine timezone during ingestion jobs.

    Args:
        value: The datetime to convert.

    Returns:
        datetime: The timezone-aware UTC datetime.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def isoformat_utc(value: datetime | None = None) -> str:
    """Return an ISO 8601 UTC timestamp using a trailing ``Z``.

    Args:
        value: The datetime to format. If None, uses current UTC time.

    Returns:
        str: The ISO 8601 formatted timestamp string.
    """
    current = utc_now() if value is None else to_utc(value)
    return current.isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime and normalize it to UTC.

    Args:
        value: The ISO 8601 datetime string to parse.

    Returns:
        datetime: The normalized timezone-aware UTC datetime.
    """
    normalized = value.replace("Z", "+00:00")
    return to_utc(datetime.fromisoformat(normalized))


def expand_year_range(*specs: str | int) -> list[int]:
    """Expand year specs into a list of ints, preserving order.

    Each spec is either a single year (``"2020"``) or an inclusive range
    (``"2020:2025"``). Ranges may be descending (``"2025:2020"``). Raises
    ``ValueError`` on malformed specs.

    Args:
        *specs: One or more year specs (e.g., 2020, "2021", "2020:2025").

    Returns:
        list[int]: A list of expanded years as integers.

    Raises:
        ValueError: If a spec is malformed.
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


def year_month_partition(year: int | str, month: int | str | None = None) -> str:
    """Return a partition string: ``"YYYY"`` or ``"YYYYMM"``.

    Args:
        year: The year component.
        month: The optional month component.

    Returns:
        str: The formatted partition string.
    """
    y = int(year)
    if month is None:
        return f"{y:04d}"
    return f"{y:04d}{int(month):02d}"
