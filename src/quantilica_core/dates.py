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
