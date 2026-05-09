from datetime import UTC, datetime, timedelta, timezone

from quantilica_core.dates import isoformat_utc, parse_iso_datetime, to_utc, utc_now


def test_utc_now_is_timezone_aware_utc():
    value = utc_now()

    assert value.tzinfo is UTC


def test_to_utc_treats_naive_datetime_as_utc():
    value = datetime(2026, 5, 9, 12, 30, 0)

    result = to_utc(value)

    assert result.tzinfo is UTC
    assert result.hour == 12


def test_to_utc_converts_aware_datetime():
    value = datetime(2026, 5, 9, 9, 30, 0, tzinfo=timezone(timedelta(hours=-3)))

    result = to_utc(value)

    assert result == datetime(2026, 5, 9, 12, 30, 0, tzinfo=UTC)


def test_isoformat_utc_uses_z_suffix():
    value = datetime(2026, 5, 9, 12, 30, 1, tzinfo=UTC)

    assert isoformat_utc(value) == "2026-05-09T12:30:01Z"


def test_parse_iso_datetime_normalizes_to_utc():
    result = parse_iso_datetime("2026-05-09T09:30:00-03:00")

    assert result == datetime(2026, 5, 9, 12, 30, 0, tzinfo=UTC)
